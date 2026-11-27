// ZOD Download Manager: a local-only window over the download engine.
package main

import (
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

//go:embed index.html
var page []byte

//go:embed logo.png
var logo []byte

// handler serves the interface. Only requests with the secret token and our own
// Host are accepted, so web sites cannot control the program.
func handler(m *Manager, token, host string, lastSeen *atomic.Int64, quit func()) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Write(page)
	})
	mux.HandleFunc("GET /logo.png", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Write(logo)
	})
	api := func(path string, action func(request) (any, error)) {
		mux.HandleFunc("POST /api/"+path, func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-Token") != token {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
			lastSeen.Store(time.Now().Unix())
			var in request
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			out, err := action(in)
			w.Header().Set("Content-Type", "application/json")
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				out = map[string]string{"error": err.Error()}
			}
			json.NewEncoder(w).Encode(out)
		})
	}
	api("state", func(request) (any, error) {
		views, settings, _ := m.Snapshot()
		return map[string]any{"items": views, "settings": settings}, nil
	})
	api("add", func(in request) (any, error) {
		return nil, m.Add(strings.Split(in.URLs, "\n"), in.Folder, in.Name, in.SHA256, in.Connections)
	})
	api("pause", func(in request) (any, error) {
		for _, id := range in.IDs {
			m.Pause(id)
		}
		return nil, nil
	})
	api("resume", func(in request) (any, error) {
		for _, id := range in.IDs {
			m.Resume(id)
		}
		return nil, nil
	})
	api("remove", func(in request) (any, error) {
		var errs []error
		for _, id := range in.IDs {
			errs = append(errs, m.Remove(id, in.DeleteFile))
		}
		return nil, errors.Join(errs...)
	})
	api("settings", func(in request) (any, error) { return nil, m.Configure(in.Settings) })
	api("open", func(in request) (any, error) {
		views, _, _ := m.Snapshot()
		for _, v := range views {
			if len(in.IDs) == 1 && v.ID == in.IDs[0] {
				if in.Folder == "folder" {
					return nil, openPath(v.Folder)
				}
				if v.Status != "completed" {
					return nil, errors.New("the download is not finished")
				}
				return nil, openPath(filepath.Join(v.Folder, v.Name))
			}
		}
		return nil, errors.New("download not found")
	})
	api("exit", func(request) (any, error) { quit(); return nil, nil })
	api("show", func(request) (any, error) { go openWindow("http://" + host + "/#" + token); return nil, nil })

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != host {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

type request struct {
	IDs          []int
	URLs, Folder string
	Name, SHA256 string
	Connections  int
	DeleteFile   bool
	Settings     Settings
}

func openPath(path string) error {
	if _, err := os.Stat(path); err != nil {
		return errors.New("the file or folder no longer exists")
	}
	switch runtime.GOOS {
	case "windows":
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", path).Start()
	case "darwin":
		return exec.Command("open", path).Start()
	}
	return exec.Command("xdg-open", path).Start()
}

// openWindow shows the interface in an app-style window (Edge or Chrome),
// falling back to the default browser.
func openWindow(link string) {
	if runtime.GOOS == "windows" {
		for _, root := range []string{"ProgramFiles(x86)", "ProgramFiles", "LocalAppData"} {
			for _, rel := range []string{`Microsoft\Edge\Application\msedge.exe`, `Google\Chrome\Application\chrome.exe`} {
				exe := filepath.Join(os.Getenv(root), rel)
				if os.Getenv(root) != "" {
					if _, err := os.Stat(exe); err == nil && exec.Command(exe, "--app="+link).Start() == nil {
						return
					}
				}
			}
		}
		exec.Command("rundll32", "url.dll,FileProtocolHandler", link).Start()
		return
	}
	exec.Command("xdg-open", link).Start()
}

// alreadyRunning asks a running copy to show its window.
func alreadyRunning(lock string) bool {
	data, err := os.ReadFile(lock)
	if err != nil {
		return false
	}
	host, token, ok := strings.Cut(string(data), " ")
	if !ok || !strings.HasPrefix(host, "127.0.0.1:") {
		return false
	}
	req, _ := http.NewRequest("POST", "http://"+host+"/api/show", strings.NewReader("{}"))
	req.Header.Set("X-Token", token)
	res, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	if err != nil {
		return false
	}
	res.Body.Close()
	return res.StatusCode == 200
}

func run() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	config, err := os.UserConfigDir()
	if err != nil {
		return err
	}
	data := flag.String("data", filepath.Join(config, "ZDM"), "where settings and history are kept")
	headless := flag.Bool("no-window", false, "print the address instead of opening a window")
	flag.Parse()

	if err := os.MkdirAll(*data, 0o700); err != nil {
		return err
	}
	lock := filepath.Join(*data, "running")
	if alreadyRunning(lock) {
		return nil
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	manager, err := NewManager(*data, filepath.Join(home, "Downloads"))
	if err != nil {
		return err
	}
	secret := make([]byte, 24)
	if _, err := rand.Read(secret); err != nil {
		return err
	}
	token, host := hex.EncodeToString(secret), listener.Addr().String()

	quit := make(chan struct{}, 1)
	stop := func() {
		select {
		case quit <- struct{}{}:
		default:
		}
	}
	var lastSeen atomic.Int64
	lastSeen.Store(time.Now().Unix())
	server := &http.Server{Handler: handler(manager, token, host, &lastSeen, stop)}
	go server.Serve(listener)
	if err := os.WriteFile(lock, []byte(host+" "+token), 0o600); err != nil {
		return err
	}
	defer os.Remove(lock)

	link := "http://" + host + "/#" + token
	if *headless {
		fmt.Println(link)
	} else {
		openWindow(link)
	}

	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-quit:
			time.Sleep(200 * time.Millisecond) // let the reply reach the window
			manager.Close()
			return server.Close()
		case <-tick.C:
			manager.Tick(500 * time.Millisecond)
			// Window closed and nothing left to do: do not linger in the background.
			if _, _, busy := manager.Snapshot(); !busy && time.Now().Unix()-lastSeen.Load() > 180 {
				stop()
			}
		}
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ZDM:", err)
		os.Exit(1)
	}
}
