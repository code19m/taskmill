package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	if _, err := loadConfig(env(nil)); err == nil {
		t.Fatal("no database URL was accepted")
	}

	c, err := loadConfig(env(map[string]string{"TASKMILL_DATABASE_URL": "postgres://db"}))
	if err != nil || c.addr != "localhost:8080" || c.auth != nil || c.readOnly || c.prefix != "" || len(c.frameAncestors) != 0 {
		t.Fatalf("defaults: got %+v, %v", c, err)
	}

	c, err = loadConfig(env(map[string]string{
		"TASKMILL_DATABASE_URL": "postgres://db", "TASKMILL_UI_ADDR": ":9000",
		"TASKMILL_UI_PASSWORD": "s3cret", "TASKMILL_UI_READ_ONLY": "true",
		"TASKMILL_UI_PREFIX":          "/admin/jobs",
		"TASKMILL_UI_FRAME_ANCESTORS": "https://admin.example.com, 'self'  https://ops.example.com",
	}))
	if err != nil || c.addr != ":9000" || !c.readOnly || c.auth == nil || c.auth.Username != "admin" || c.prefix != "/admin/jobs" {
		t.Fatalf("got %+v, %v", c, err)
	}
	if want := []string{"https://admin.example.com", "'self'", "https://ops.example.com"}; !slices.Equal(c.frameAncestors, want) {
		t.Fatalf("frame ancestors: got %q, want %q", c.frameAncestors, want)
	}

	if _, err := loadConfig(env(map[string]string{"TASKMILL_DATABASE_URL": "postgres://db", "TASKMILL_UI_READ_ONLY": "maybe"})); err == nil {
		t.Error("TASKMILL_UI_READ_ONLY=maybe was accepted")
	}
}

func TestHandlerServesItsSettings(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	schema := testdb.Schema(t, pool)
	if err := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema}); err != nil {
		t.Fatal(err)
	}
	client, err := taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(env(map[string]string{
		"TASKMILL_DATABASE_URL":       "postgres://db",
		"TASKMILL_UI_PREFIX":          "/admin/jobs",
		"TASKMILL_UI_FRAME_ANCESTORS": "https://admin.example.com",
	}))
	if err != nil {
		t.Fatal(err)
	}
	h, err := handler(pool, client, cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}

	// A proxy forwards the path unchanged; the page's links keep the prefix.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/jobs/", nil))
	body, _ := io.ReadAll(rec.Body)
	if rec.Code != http.StatusOK || !strings.Contains(string(body), `href="/admin/jobs/tasks"`) {
		t.Fatalf("got %d; links under the prefix: %v", rec.Code, strings.Contains(string(body), `href="/admin/jobs/tasks"`))
	}
	if csp := rec.Header().Get("Content-Security-Policy"); !strings.Contains(csp, "frame-ancestors https://admin.example.com") {
		t.Fatalf("content security policy %q does not let the admin app frame the dashboard", csp)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/tasks", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("a path outside the prefix: got %d, want 404", rec.Code)
	}

	// The dashboard refuses settings that would serve it wrongly.
	for name, value := range map[string]string{
		"TASKMILL_UI_PREFIX":          "admin/jobs",
		"TASKMILL_UI_FRAME_ANCESTORS": "https://admin.example.com;script-src *",
	} {
		cfg, err := loadConfig(env(map[string]string{"TASKMILL_DATABASE_URL": "postgres://db", name: value}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := handler(pool, client, cfg, slog.New(slog.DiscardHandler)); !errors.Is(err, taskmill.ErrInvalidArgument) {
			t.Errorf("%s=%q: got %v, want ErrInvalidArgument", name, value, err)
		}
	}
}

func TestRunAnswersProbesAndLogsJSON(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	schema := testdb.Schema(t, pool)
	if err := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema}); err != nil {
		t.Fatal(err)
	}
	db := newProxy(t) // taskmill-ui reaches the database through it, and the test cuts it
	logs := &jsonLog{}
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	stopped := make(chan error, 1)
	go func() {
		stopped <- run(ctx, env(map[string]string{
			"TASKMILL_DATABASE_URL":  db.url,
			"TASKMILL_SCHEMA":        schema,
			"TASKMILL_UI_ADDR":       "127.0.0.1:0",
			"TASKMILL_UI_PASSWORD":   "s3cret",
			"TASKMILL_UI_PREFIX":     "/admin/jobs",
			"TASKMILL_UI_LOG_FORMAT": "json",
		}), logs)
	}()
	addr, _ := logs.wait(t, "taskmill-ui listening")["addr"].(string)
	base := "http://" + addr

	// The probes need no login, and are at the root whatever the prefix.
	waitStatus(t, base+"/admin/jobs/", http.StatusUnauthorized)
	waitStatus(t, base+"/livez", http.StatusOK)
	waitStatus(t, base+"/readyz", http.StatusOK)

	// Without the database, the pod is not ready, but it is alive: a restart
	// would not bring the database back.
	db.cut(true)
	if body := waitStatus(t, base+"/readyz", http.StatusServiceUnavailable); strings.Contains(body, db.addr) {
		t.Errorf("/readyz tells anyone where the database is: %q", body)
	}
	waitStatus(t, base+"/livez", http.StatusOK)
	if line := logs.wait(t, "taskmill-ui not ready"); line["level"] != "WARN" || line["error"] == nil {
		t.Errorf("got %v, want a warning with the error", line)
	}

	db.cut(false)
	waitStatus(t, base+"/readyz", http.StatusOK)
	logs.wait(t, "taskmill-ui ready again")

	stop()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run did not stop")
	}
	if bad := logs.notJSON(); len(bad) > 0 {
		t.Errorf("lines that are not JSON: %q", bad)
	}
}

func TestRunLogsWhyItStoppedInItsFormat(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	err := run(t.Context(), env(map[string]string{"TASKMILL_UI_LOG_FORMAT": "json"}), &out)
	var line map[string]any
	if err == nil || json.Unmarshal(out.Bytes(), &line) != nil || line["msg"] != "taskmill-ui stopped" || line["level"] != "ERROR" {
		t.Fatalf("no database URL: got %v, logged %q", err, out.String())
	}

	out.Reset()
	err = run(t.Context(), env(map[string]string{"TASKMILL_UI_LOG_FORMAT": "yaml"}), &out)
	if err == nil || !strings.HasPrefix(out.String(), "time=") || !strings.Contains(out.String(), "TASKMILL_UI_LOG_FORMAT") {
		t.Fatalf("an unknown format: got %v, logged %q", err, out.String())
	}
}

// waitStatus gets url until it answers with the status want, and returns the
// body of that answer.
func waitStatus(t *testing.T, url string, want int) string {
	t.Helper()
	var got int
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if got = resp.StatusCode; got == want {
			return string(body)
		}
	}
	t.Fatalf("%s: got %d, want %d", url, got, want)
	return ""
}

// jsonLog keeps the lines logged to it, each of which must be a JSON object.
type jsonLog struct {
	mu    sync.Mutex
	lines []map[string]any
	bad   []string
}

func (l *jsonLog) Write(p []byte) (int, error) {
	var line map[string]any
	err := json.Unmarshal(p, &line)
	l.mu.Lock()
	defer l.mu.Unlock()
	if err != nil {
		l.bad = append(l.bad, string(p))
	} else {
		l.lines = append(l.lines, line)
	}
	return len(p), nil
}

// wait returns the first line with the message msg, once it is logged.
func (l *jsonLog) wait(t *testing.T, msg string) map[string]any {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if line := l.find(msg); line != nil {
			return line
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	t.Fatalf("no line %q was logged; the lines: %v", msg, l.lines)
	return nil
}

func (l *jsonLog) find(msg string) map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if line["msg"] == msg {
			return line
		}
	}
	return nil
}

func (l *jsonLog) notJSON() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.bad
}

// proxy forwards connections to the test database, and can cut them all, as
// a database that goes away does.
type proxy struct {
	addr string // where it listens
	url  string // the test database's URL, through the proxy

	mu    sync.Mutex
	down  bool
	conns []net.Conn
}

func newProxy(t *testing.T) *proxy {
	t.Helper()
	u, err := url.Parse(testdb.URL(t))
	if err != nil || u.Host == "" {
		t.Fatalf("%s is not a URL with a host: %v", testdb.EnvURL, err)
	}
	target := u.Host
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	u.Host = ln.Addr().String()
	p := &proxy{addr: u.Host, url: u.String()}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.forward(c, target)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		p.cut(true)
	})
	return p
}

// cut closes every connection and refuses new ones, or accepts them again.
func (p *proxy) cut(down bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = down
	if down {
		for _, c := range p.conns {
			_ = c.Close()
		}
		p.conns = nil
	}
}

func (p *proxy) forward(client net.Conn, target string) {
	server, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(context.Background(), "tcp", target)
	p.mu.Lock()
	if err != nil || p.down {
		p.mu.Unlock()
		_ = client.Close()
		if server != nil {
			_ = server.Close()
		}
		return
	}
	p.conns = append(p.conns, client, server)
	p.mu.Unlock()
	go func() {
		_, _ = io.Copy(server, client)
		_ = server.Close()
	}()
	_, _ = io.Copy(client, server)
	_ = client.Close()
}
