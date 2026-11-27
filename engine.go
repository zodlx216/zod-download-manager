// Download engine: byte-range segments, resume, speed limit, integrity checks.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	minSegment = 256 * 1024
	checkpoint = 1024 * 1024
	maxConn    = 32
)

var (
	retries     = 5
	backoff     = time.Second
	idleTimeout = 30 * time.Second // a connection silent this long is stalled
	errChanged  = errors.New("the file changed on the server; press Resume to restart it")
)

type Segment struct {
	Start, End, Done int64 // End is inclusive; Done is bytes safely on disk
}

type transient struct {
	msg   string
	delay time.Duration
}

func (t transient) Error() string { return t.msg }

// Limiter is shared by every connection, so it limits the real transfer.
type Limiter struct {
	mu   sync.Mutex
	rate int64
	next time.Time
}

func (l *Limiter) Set(bytesPerSecond int64) {
	l.mu.Lock()
	l.rate, l.next = bytesPerSecond, time.Time{}
	l.mu.Unlock()
}

func (l *Limiter) chunk() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rate > 0 {
		return int(max(1024, min(65536, l.rate/10)))
	}
	return 65536
}

func (l *Limiter) wait(ctx context.Context, n int) {
	l.mu.Lock()
	if l.rate <= 0 {
		l.mu.Unlock()
		return
	}
	if now := time.Now(); l.next.Before(now) {
		l.next = now
	}
	l.next = l.next.Add(time.Duration(float64(n) / float64(l.rate) * float64(time.Second)))
	d := time.Until(l.next)
	l.mu.Unlock()
	sleep(ctx, d)
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

var client = &http.Client{
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DisableCompression:    true,
		ResponseHeaderTimeout: 30 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		// HTTP/1.1 only: HTTP/2 would squeeze every segment into one connection.
		TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{},
	},
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("too many redirects")
		}
		if via[len(via)-1].URL.Scheme == "https" && req.URL.Scheme != "https" {
			return errors.New("refused redirect from HTTPS to plain HTTP")
		}
		return nil
	},
}

func checkURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("only http:// and https:// links are supported")
	}
	return nil
}

var (
	badChars = regexp.MustCompile(`[<>:"|?*\x00-\x1f]`)
	reserved = regexp.MustCompile(`(?i)^(con|prn|aux|nul|com\d|lpt\d)(\..*)?$`)
)

// safeName reduces an untrusted name to one harmless Windows path component.
func safeName(name string) string {
	name = strings.ReplaceAll(name, `\`, "/")
	name = name[strings.LastIndex(name, "/")+1:]
	name = strings.Trim(badChars.ReplaceAllString(name, "_"), " .")
	if reserved.MatchString(name) {
		name = "_" + name
	}
	ext := []rune(path.Ext(name))
	stem := []rune(strings.TrimSuffix(name, string(ext)))
	ext = ext[:min(len(ext), 20)]
	stem = stem[:min(len(stem), 150)]
	if name = string(stem) + string(ext); name == "" {
		return "download"
	}
	return name
}

// get performs one request. The returned cancel must always be called.
func get(ctx context.Context, link string, header map[string]string) (*http.Response, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(ctx, "GET", link, nil)
	if err != nil {
		return nil, cancel, err
	}
	req.Header.Set("User-Agent", "ZDM/0.3")
	for k, v := range header {
		req.Header.Set(k, v)
	}
	res, err := client.Do(req)
	if err != nil {
		var certErr *tls.CertificateVerificationError
		if errors.As(err, &certErr) {
			return nil, cancel, fmt.Errorf("invalid certificate: %v", certErr.Err)
		}
		if strings.Contains(err.Error(), "refused redirect") {
			return nil, cancel, err
		}
		return nil, cancel, transient{msg: "network error: " + err.Error()}
	}
	code := res.StatusCode
	if code == 200 || code == 206 || code == 416 {
		return res, cancel, nil
	}
	res.Body.Close()
	explain := map[int]string{401: "login required", 403: "access denied (the link may have expired)",
		404: "file not found", 408: "server timed out", 429: "server is rate limiting requests",
		500: "server error", 502: "bad gateway", 503: "service unavailable"}[code]
	msg := strings.TrimSpace(fmt.Sprintf("HTTP %d %s", code, explain))
	if code == 408 || code == 429 || code == 500 || code == 502 || code == 503 || code == 504 {
		seconds, _ := strconv.Atoi(res.Header.Get("Retry-After"))
		return nil, cancel, transient{msg, time.Duration(min(max(seconds, 0), 120)) * time.Second}
	}
	return nil, cancel, errors.New(msg)
}

// retry counts only consecutive failures that made no progress.
func retry(ctx context.Context, progressed func() int64, action func() error) error {
	failures, mark := 0, progressed()
	for {
		err := action()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var t transient
		if !errors.As(err, &t) {
			return err
		}
		if progressed() > mark {
			failures, mark = 0, progressed()
		}
		failures++
		if failures > retries {
			return fmt.Errorf("%s (gave up after %d retries)", t.msg, retries)
		}
		d := t.delay
		if d == 0 {
			d = min(backoff<<(failures-1), 30*time.Second)
		}
		sleep(ctx, d)
	}
}

type Info struct {
	URL, Name, ETag, Modified string
	Size                      int64 // -1 when unknown
	Ranged                    bool
}

func probe(ctx context.Context, link string) (info Info, err error) {
	if err = checkURL(link); err != nil {
		return
	}
	res, cancel, err := get(ctx, link, map[string]string{"Range": "bytes=0-0"})
	defer func() { cancel() }()
	if err != nil {
		return
	}
	if res.StatusCode == 416 { // for example an empty file
		res.Body.Close()
		cancel()
		if res, cancel, err = get(ctx, link, nil); err != nil {
			return
		}
	}
	defer res.Body.Close()
	info = Info{URL: res.Request.URL.String(), Size: -1,
		ETag: res.Header.Get("ETag"), Modified: res.Header.Get("Last-Modified")}
	if total, ok := strings.CutPrefix(res.Header.Get("Content-Range"), "bytes 0-0/"); ok && res.StatusCode == 206 {
		if n, e := strconv.ParseInt(total, 10, 64); e == nil {
			info.Size, info.Ranged = n, n > 0
		}
	} else if res.StatusCode == 200 {
		info.Size = res.ContentLength
	} else {
		return info, fmt.Errorf("unexpected HTTP status %d", res.StatusCode)
	}
	name := res.Request.URL.Path
	if _, params, e := mime.ParseMediaType(res.Header.Get("Content-Disposition")); e == nil && params["filename"] != "" {
		name = params["filename"]
	}
	info.Name = safeName(name)
	return
}

func split(size int64, connections int) []Segment {
	n := int64(max(1, min(connections, maxConn)))
	n = max(1, min(n, size/minSegment))
	step := size / n
	segments := make([]Segment, n)
	for i := range segments {
		segments[i] = Segment{Start: int64(i) * step, End: int64(i+1)*step - 1}
	}
	segments[n-1].End = size - 1
	return segments
}

// copyBody streams res into file at offset. It stops after want bytes (-1: until EOF).
func copyBody(ctx context.Context, cancel context.CancelFunc, res *http.Response, file *os.File,
	offset, want int64, limiter *Limiter, count func(int), save func() error) (err error) {
	idle := time.AfterFunc(idleTimeout, cancel)
	defer idle.Stop()
	defer func() {
		if e := save(); e != nil && err == nil {
			err = e
		}
	}()
	buffer := make([]byte, 65536)
	var got, unsaved int64
	for want < 0 || got < want {
		size := limiter.chunk()
		if want >= 0 {
			size = int(min(int64(size), want-got))
		}
		idle.Reset(idleTimeout)
		n, readErr := res.Body.Read(buffer[:size])
		idle.Stop() // time spent in the speed limiter is not a stall
		if n > 0 {
			if _, e := file.WriteAt(buffer[:n], offset+got); e != nil {
				return fmt.Errorf("cannot write to disk: %w", e)
			}
			got += int64(n)
			unsaved += int64(n)
			count(n)
			if unsaved >= checkpoint {
				if e := save(); e != nil {
					return e
				}
				unsaved = 0
			}
			limiter.wait(ctx, n)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if readErr == io.EOF && (want < 0 || got == want) {
			return nil
		}
		if readErr != nil {
			return transient{msg: "connection lost or stalled"}
		}
	}
	return nil
}

type Job struct {
	URL, Folder, Name, SHA256 string
	Connections               int
	Part                      string
	// State from an earlier run.
	Size           int64
	ETag, Modified string
	Segments       []Segment

	Limiter  *Limiter
	Received *atomic.Int64 // bytes done, for the display
	Active   *atomic.Int32 // open connections
	Started  func(Job)     // called once the segments are known
	Save     func(index int, done int64)
}

// Download runs or resumes job and returns the final file name.
func Download(ctx context.Context, job Job) (string, error) {
	if err := os.MkdirAll(job.Folder, 0o755); err != nil {
		return "", err
	}
	var info Info
	err := retry(ctx, func() int64 { return 0 }, func() (e error) {
		info, e = probe(ctx, job.URL)
		return
	})
	if err != nil {
		return "", err
	}
	name := info.Name
	if job.Name != "" {
		name = safeName(job.Name)
	}

	// Resume only when the server gives a validator and nothing has changed.
	stat, statErr := os.Stat(job.Part)
	resume := info.Ranged && len(job.Segments) > 0 && statErr == nil &&
		stat.Size() == info.Size && info.Size == job.Size &&
		(info.ETag != "" || info.Modified != "") &&
		info.ETag == job.ETag && info.Modified == job.Modified
	if !resume {
		job.Segments = nil
		if info.Ranged {
			job.Segments = split(info.Size, job.Connections)
		}
	}
	job.Name, job.Size, job.ETag, job.Modified = name, info.Size, info.ETag, info.Modified

	flags := os.O_RDWR | os.O_CREATE
	if !resume {
		flags |= os.O_TRUNC
	}
	file, err := os.OpenFile(job.Part, flags, 0o644)
	if err != nil {
		return "", err
	}
	defer file.Close()
	if info.Ranged && !resume {
		if err := file.Truncate(info.Size); err != nil {
			return "", fmt.Errorf("cannot reserve disk space: %w", err)
		}
	}
	job.Received.Store(0)
	for _, s := range job.Segments {
		job.Received.Add(s.Done)
	}
	job.Started(job)

	if info.Ranged {
		err = ranged(ctx, job, info, file)
	} else {
		err = single(ctx, job, info, file)
	}
	if err != nil {
		return "", err
	}
	if err := file.Sync(); err != nil {
		return "", err
	}
	if stat, err := file.Stat(); err != nil || (info.Size >= 0 && stat.Size() != info.Size) {
		return "", errors.New("size check failed")
	}
	if job.SHA256 != "" {
		hash := sha256.New()
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return "", err
		}
		if _, err := io.Copy(hash, file); err != nil {
			return "", err
		}
		if !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), job.SHA256) {
			file.Close()
			os.Remove(job.Part)
			return "", errors.New("SHA-256 mismatch; the downloaded data was discarded")
		}
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	return publish(job.Part, job.Folder, name)
}

func ranged(parent context.Context, job Job, info Info, file *os.File) error {
	ctx, abort := context.WithCancel(parent)
	defer abort()
	validator := info.ETag
	if validator == "" || strings.HasPrefix(validator, "W/") {
		validator = info.Modified
	}
	var wg sync.WaitGroup
	var once sync.Once
	var failure error
	for i, s := range job.Segments {
		wg.Add(1)
		go func(i int, s Segment) {
			defer wg.Done()
			done := s.Done
			err := retry(ctx, func() int64 { return done }, func() error {
				position := s.Start + done
				if position > s.End {
					return nil
				}
				header := map[string]string{"Range": fmt.Sprintf("bytes=%d-%d", position, s.End)}
				if validator != "" {
					header["If-Range"] = validator
				}
				res, cancel, err := get(ctx, info.URL, header)
				defer cancel()
				if err != nil {
					return err
				}
				defer res.Body.Close()
				if res.StatusCode != 206 || res.Header.Get("Content-Range") !=
					fmt.Sprintf("bytes %d-%d/%d", position, s.End, info.Size) {
					return errChanged
				}
				job.Active.Add(1)
				defer job.Active.Add(-1)
				return copyBody(ctx, cancel, res, file, position, s.End-position+1, job.Limiter,
					func(n int) { done += int64(n); job.Received.Add(int64(n)) },
					func() error {
						if err := file.Sync(); err != nil { // data first, then the record of it
							return err
						}
						job.Save(i, done)
						return nil
					})
			})
			if err != nil {
				once.Do(func() { failure = err; abort() })
			} else if done != s.End-s.Start+1 {
				once.Do(func() { failure = errors.New("a segment is incomplete"); abort() })
			}
		}(i, s)
	}
	wg.Wait()
	if parent.Err() != nil {
		return parent.Err()
	}
	return failure
}

// single serves servers without ranges: one connection, from zero on every retry.
func single(ctx context.Context, job Job, info Info, file *os.File) error {
	var best int64
	return retry(ctx, func() int64 { return best }, func() error {
		job.Received.Store(0)
		if err := file.Truncate(0); err != nil {
			return err
		}
		res, cancel, err := get(ctx, info.URL, nil)
		defer cancel()
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.StatusCode != 200 {
			return fmt.Errorf("unexpected HTTP status %d", res.StatusCode)
		}
		job.Active.Add(1)
		defer job.Active.Add(-1)
		return copyBody(ctx, cancel, res, file, 0, info.Size, job.Limiter,
			func(n int) { job.Received.Add(int64(n)) }, func() error { return nil })
	})
}

// publish moves the finished file into place without overwriting anything.
func publish(part, folder, name string) (string, error) {
	ext := filepath.Ext(name)
	stem := strings.TrimSuffix(name, ext)
	for i := 0; i < 10000; i++ {
		candidate := name
		if i > 0 {
			candidate = fmt.Sprintf("%s (%d)%s", stem, i, ext)
		}
		target := filepath.Join(folder, candidate)
		// Reserve the name exclusively, then replace our own empty placeholder.
		placeholder, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			return "", err
		}
		placeholder.Close()
		if err := os.Rename(part, target); err != nil {
			os.Remove(target)
			return "", err
		}
		return candidate, nil
	}
	return "", errors.New("too many files with the same name")
}
