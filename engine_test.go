package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fake struct {
	*httptest.Server
	mu    sync.Mutex
	data  []byte
	etag  string
	drops int
}

func newFake(t *testing.T) *fake {
	f := &fake{data: make([]byte, 3<<20+123), etag: `"v1"`}
	rand.Read(f.data)
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		data, etag := f.data, f.etag
		drop := f.drops > 0 && r.Header.Get("Range") != "bytes=0-0"
		if drop {
			f.drops--
		}
		f.mu.Unlock()
		switch r.URL.Path {
		case "/missing":
			http.NotFound(w, r)
			return
		case "/noranges":
			w.Write(data)
			return
		}
		w.Header().Set("ETag", etag)
		if drop {
			w = &cut{w, 1000}
		}
		http.ServeContent(w, r, "file.bin", time.Time{}, bytes.NewReader(data))
	}))
	t.Cleanup(f.Close)
	return f
}

// cut aborts the connection after a few bytes.
type cut struct {
	http.ResponseWriter
	left int
}

func (c *cut) Write(p []byte) (int, error) {
	c.ResponseWriter.Write(p[:min(len(p), c.left)])
	c.ResponseWriter.(http.Flusher).Flush()
	panic(http.ErrAbortHandler)
}

func newManager(t *testing.T, dir string) *Manager {
	m, err := NewManager(filepath.Join(dir, "data"), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	return m
}

func wait(t *testing.T, m *Manager, status string) View {
	t.Helper()
	for end := time.Now().Add(20 * time.Second); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		views, _, _ := m.Snapshot()
		m.mu.Lock()
		running := m.Items[0].done != nil
		m.mu.Unlock()
		if !running && views[0].Status == status {
			return views[0]
		}
	}
	views, _, _ := m.Snapshot()
	t.Fatalf("wanted %s, got %+v", status, views[0])
	return View{}
}

func add(t *testing.T, m *Manager, link, sha string) {
	t.Helper()
	if err := m.Add([]string{link}, "", "", sha, 4); err != nil {
		t.Fatal(err)
	}
}

func same(t *testing.T, v View, want []byte) {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(v.Folder, v.Name))
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("content differs (%d vs %d bytes, %v)", len(got), len(want), err)
	}
}

func TestDownloads(t *testing.T) {
	backoff = 10 * time.Millisecond
	for _, c := range []struct {
		name, path string
		drops      int
	}{{"segments", "/file.bin", 0}, {"no ranges", "/noranges", 0}, {"dropped connections", "/file.bin", 3}} {
		t.Run(c.name, func(t *testing.T) {
			f, m := newFake(t), newManager(t, t.TempDir())
			f.drops = c.drops
			sum := sha256.Sum256(f.data)
			add(t, m, f.URL+c.path, hex.EncodeToString(sum[:]))
			same(t, wait(t, m, "completed"), f.data)
		})
	}
}

func pauseMidway(t *testing.T, m *Manager, link string) {
	m.limiter.Set(2 << 20)
	add(t, m, link, "")
	time.Sleep(900 * time.Millisecond)
	m.Pause(1)
	v := wait(t, m, "paused")
	if v.Done <= 0 || v.Done >= v.Size {
		t.Fatalf("not paused midway: %d of %d", v.Done, v.Size)
	}
	m.limiter.Set(0)
}

func TestResumeAfterRestart(t *testing.T) {
	f, dir := newFake(t), t.TempDir()
	first := newManager(t, dir)
	pauseMidway(t, first, f.URL+"/file.bin")
	first.Close()
	second := newManager(t, dir)
	if v, _, _ := second.Snapshot(); v[0].Done <= 0 {
		t.Fatal("progress was not saved")
	}
	second.Resume(1)
	same(t, wait(t, second, "completed"), f.data)
}

func TestChangedFileRestarts(t *testing.T) {
	f, m := newFake(t), newManager(t, t.TempDir())
	pauseMidway(t, m, f.URL+"/file.bin")
	f.mu.Lock()
	changed := bytes.Clone(f.data)
	for i := range changed {
		changed[i] ^= 0xff
	}
	f.data, f.etag = changed, `"v2"`
	f.mu.Unlock()
	m.Resume(1)
	same(t, wait(t, m, "completed"), changed)
}

func TestNeverOverwritesAndFailures(t *testing.T) {
	f, dir := newFake(t), t.TempDir()
	os.WriteFile(filepath.Join(dir, "file.bin"), []byte("mine"), 0o644)
	m := newManager(t, dir)
	add(t, m, f.URL+"/file.bin", "")
	if v := wait(t, m, "completed"); v.Name != "file (1).bin" {
		t.Fatal(v.Name)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "file.bin")); string(b) != "mine" {
		t.Fatal("existing file was overwritten")
	}
	m.Remove(1, false)
	add(t, m, f.URL+"/file.bin", strings.Repeat("0", 64))
	if v := wait(t, m, "failed"); !strings.Contains(v.Error, "SHA-256") {
		t.Fatal(v.Error)
	}
	m.Remove(2, false)
	add(t, m, f.URL+"/missing", "")
	if v := wait(t, m, "failed"); !strings.Contains(v.Error, "404") {
		t.Fatal(v.Error)
	}
}

func TestSpeedLimit(t *testing.T) {
	f, m := newFake(t), newManager(t, t.TempDir())
	m.limiter.Set(2 << 20)
	start := time.Now()
	add(t, m, f.URL+"/file.bin", "")
	wait(t, m, "completed")
	if time.Since(start) < 1300*time.Millisecond {
		t.Fatal("limit not applied")
	}
}

func TestSafety(t *testing.T) {
	for in, want := range map[string]string{`..\..\Windows\evil.exe`: "evil.exe", "../..": "download", "CON.txt": "_CON.txt"} {
		if got := safeName(in); got != want {
			t.Errorf("%q -> %q", in, got)
		}
	}
	if checkURL("file:///etc/passwd") == nil {
		t.Error("file URL accepted")
	}
	m := newManager(t, t.TempDir())
	h := handler(m, "secret", "127.0.0.1:1", new(atomic.Int64), func() {})
	for _, c := range []struct {
		host, token string
		want        int
	}{{"127.0.0.1:1", "secret", 200}, {"127.0.0.1:1", "wrong", 403}, {"evil.example", "secret", 403}} {
		r := httptest.NewRequest("POST", "/api/state", strings.NewReader("{}"))
		r.Host = c.host
		r.Header.Set("X-Token", c.token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != c.want {
			t.Errorf("%+v got %d", c, w.Code)
		}
	}
}
