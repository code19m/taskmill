package main

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

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
	h, err := handler(client, cfg, slog.New(slog.DiscardHandler))
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
		if _, err := handler(client, cfg, slog.New(slog.DiscardHandler)); !errors.Is(err, taskmill.ErrInvalidArgument) {
			t.Errorf("%s=%q: got %v, want ErrInvalidArgument", name, value, err)
		}
	}
}
