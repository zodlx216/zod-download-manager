// Queue, persistence and the list shown by the interface.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Item struct {
	ID                        int
	URL, Folder, Name, SHA256 string
	Connections               int
	Size                      int64
	ETag, Modified            string
	Segments                  []Segment
	Status, Error             string // queued, downloading, paused, completed, failed
	Added                     time.Time
	Bytes                     int64

	received atomic.Int64
	active   atomic.Int32
	cancel   context.CancelFunc
	done     chan struct{}
	speed    float64
	last     int64
}

type Settings struct {
	Folder       string
	Connections  int
	Simultaneous int
	LimitKB      int64
}

type Manager struct {
	mu       sync.Mutex
	path     string
	Items    []*Item
	Settings Settings
	NextID   int
	limiter  Limiter
	closed   bool
}

func NewManager(dir, downloads string) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	m := &Manager{path: filepath.Join(dir, "state.json"), NextID: 1,
		Settings: Settings{Folder: downloads, Connections: 8, Simultaneous: 3}}
	data, err := os.ReadFile(m.path)
	if err == nil {
		if err := json.Unmarshal(data, m); err != nil {
			os.Rename(m.path, m.path+".damaged") // keep it for recovery
			m.Items, m.NextID = nil, 1
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	for _, it := range m.Items {
		if it.Status == "downloading" { // interrupted by a crash or shutdown
			it.Status = "paused"
		}
		it.received.Store(it.Bytes)
		for i, s := range it.Segments {
			if i == 0 {
				it.received.Store(0)
			}
			it.received.Add(s.Done)
		}
	}
	m.limiter.Set(m.Settings.LimitKB * 1024)
	m.schedule()
	return m, nil
}

// save writes the state atomically. Caller holds the lock.
func (m *Manager) save() {
	data, err := json.Marshal(m)
	if err != nil {
		return
	}
	tmp := m.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	if err == nil {
		os.Rename(tmp, m.path)
	}
}

func (it *Item) part() string { return filepath.Join(it.Folder, fmt.Sprintf(".zdm-%d.part", it.ID)) }

// schedule starts queued items while there is a free slot. Caller holds the lock
// (or is the only user, during construction).
func (m *Manager) schedule() {
	running := 0
	for _, it := range m.Items {
		if it.done != nil {
			running++
		}
	}
	for _, it := range m.Items {
		if m.closed || running >= m.Settings.Simultaneous {
			return
		}
		if it.Status == "queued" && it.done == nil {
			ctx, cancel := context.WithCancel(context.Background())
			it.cancel, it.done = cancel, make(chan struct{})
			it.Status, it.Error = "downloading", ""
			running++
			go m.run(ctx, it, Job{URL: it.URL, Folder: it.Folder, Name: it.Name, SHA256: it.SHA256,
				Connections: it.Connections, Part: it.part(), Size: it.Size, ETag: it.ETag,
				Modified: it.Modified, Segments: slices.Clone(it.Segments),
				Limiter: &m.limiter, Received: &it.received, Active: &it.active}, it.done)
		}
	}
}

func (m *Manager) run(ctx context.Context, it *Item, job Job, done chan struct{}) {
	job.Started = func(j Job) {
		m.mu.Lock()
		defer m.mu.Unlock()
		it.Name, it.Size, it.ETag, it.Modified = j.Name, j.Size, j.ETag, j.Modified
		it.Segments = slices.Clone(j.Segments)
		m.save()
	}
	job.Save = func(i int, n int64) {
		m.mu.Lock()
		defer m.mu.Unlock()
		it.Segments[i].Done = n
		m.save()
	}
	name, err := Download(ctx, job)

	m.mu.Lock()
	defer m.mu.Unlock()
	switch {
	case err == nil:
		it.Status, it.Name, it.Segments = "completed", name, nil
		if info, e := os.Stat(filepath.Join(it.Folder, name)); e == nil {
			it.Size = info.Size()
			it.received.Store(it.Size)
		}
	case ctx.Err() != nil:
		// Paused by the user; keep "queued" if Resume was already pressed again.
		if it.Status == "downloading" {
			it.Status = "paused"
		}
	default:
		it.Status, it.Error = "failed", err.Error()
	}
	it.Bytes = it.received.Load()
	it.cancel, it.done = nil, nil
	close(done)
	m.save()
	m.schedule()
}

func (m *Manager) Add(links []string, folder, name, sha string, connections int) error {
	var clean []string
	for _, link := range links {
		if link = strings.TrimSpace(link); link != "" {
			if err := checkURL(link); err != nil {
				return fmt.Errorf("%s: %w", link, err)
			}
			clean = append(clean, link)
		}
	}
	if len(clean) == 0 {
		return errors.New("enter at least one link")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if folder = strings.TrimSpace(folder); folder == "" {
		folder = m.Settings.Folder
	}
	if !filepath.IsAbs(folder) {
		return errors.New("the folder must be a full path, such as C:\\Users\\you\\Downloads")
	}
	if len(clean) > 1 {
		name, sha = "", ""
	}
	if connections < 1 || connections > maxConn {
		connections = m.Settings.Connections
	}
	for _, link := range clean {
		m.Items = append(m.Items, &Item{ID: m.NextID, URL: link, Folder: folder,
			Name: strings.TrimSpace(name), SHA256: strings.TrimSpace(sha),
			Connections: connections, Status: "queued", Added: time.Now()})
		m.NextID++
	}
	m.save()
	m.schedule()
	return nil
}

func (m *Manager) find(id int) *Item {
	for _, it := range m.Items {
		if it.ID == id {
			return it
		}
	}
	return nil
}

func (m *Manager) Pause(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if it := m.find(id); it != nil && (it.Status == "queued" || it.Status == "downloading") {
		it.Status = "paused"
		if it.cancel != nil {
			it.cancel()
		}
		m.save()
	}
}

func (m *Manager) Resume(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if it := m.find(id); it != nil && (it.Status == "paused" || it.Status == "failed") {
		it.Status, it.Error = "queued", ""
		m.save()
		m.schedule()
	}
}

// Remove takes an item off the list. The finished file is deleted only on request.
func (m *Manager) Remove(id int, deleteFile bool) error {
	m.Pause(id)
	m.mu.Lock()
	it := m.find(id)
	var done chan struct{}
	if it != nil {
		done = it.done
	}
	m.mu.Unlock()
	if it == nil {
		return nil
	}
	if done != nil {
		<-done
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if it.done != nil {
		return errors.New("the download was restarted; try again")
	}
	if it.Status != "completed" {
		os.Remove(it.part())
	} else if deleteFile {
		if err := os.Remove(filepath.Join(it.Folder, it.Name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	m.Items = slices.DeleteFunc(m.Items, func(x *Item) bool { return x == it })
	m.save()
	m.schedule()
	return nil
}

func (m *Manager) Configure(s Settings) error {
	if !filepath.IsAbs(s.Folder) {
		return errors.New("the folder must be a full path")
	}
	s.Connections = max(1, min(s.Connections, maxConn))
	s.Simultaneous = max(1, min(s.Simultaneous, 10))
	s.LimitKB = max(0, s.LimitKB)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Settings = s
	m.limiter.Set(s.LimitKB * 1024)
	m.save()
	m.schedule()
	return nil
}

// Tick updates the speed readings once per interval.
func (m *Manager) Tick(interval time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, it := range m.Items {
		now := it.received.Load()
		if it.Status != "downloading" {
			it.speed = 0
		} else {
			instant := float64(max(0, now-it.last)) / interval.Seconds()
			it.speed = 0.6*it.speed + 0.4*instant
		}
		it.last = now
	}
}

type View struct {
	ID                                      int
	URL, Folder, Name, Status, Error, Added string
	Size, Done                              int64
	Speed                                   float64
	Connections                             int32
}

func (m *Manager) Snapshot() (views []View, settings Settings, busy bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	views = []View{}
	for _, it := range m.Items {
		views = append(views, View{it.ID, it.URL, it.Folder, it.Name, it.Status, it.Error,
			it.Added.Format("2006-01-02 15:04"), it.Size, it.received.Load(), it.speed, it.active.Load()})
		busy = busy || it.done != nil || it.Status == "queued"
	}
	return views, m.Settings, busy
}

// Close pauses everything and waits. Unfinished items continue on the next start.
func (m *Manager) Close() {
	m.mu.Lock()
	m.closed = true
	var waiting []chan struct{}
	for _, it := range m.Items {
		if it.done != nil {
			it.Status = "queued"
			it.cancel()
			waiting = append(waiting, it.done)
		}
	}
	m.mu.Unlock()
	for _, done := range waiting {
		<-done
	}
	m.mu.Lock()
	m.save()
	m.mu.Unlock()
}
