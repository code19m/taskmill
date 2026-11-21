package ui

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	requestTimeout = 10 * time.Second // bound on one request, so a slow database cannot hold it
	gzipMin        = 1024             // smallest response worth compressing
)

// liveHeader marks the requests of the page's script: its live updates, the
// pages that it shows in place, and the actions that it sends. They are never
// answered with a login prompt: the page shows that the session ended, or the
// browser loads the page and asks then.
const liveHeader = "X-Taskmill-Live"

// underPrefix serves h for the paths under prefix, with the prefix stripped,
// as http.StripPrefix does, and answers itself the requests that h would send
// out of the prefix. The prefix alone, and a path that is not clean, such as
// one with a double slash, redirect to the clean path under the prefix: h's
// mux would redirect them to that path at the host's root. A path that only
// starts like the prefix, such as /taskmillx for /taskmill, is not found.
func underPrefix(prefix string, h http.Handler) http.Handler {
	escaped := (&url.URL{Path: prefix}).EscapedPath()
	strip := http.StripPrefix(prefix, h)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The escaped path decides, as in the mux: an escaped slash or dot
		// is part of a name, not of the path's shape.
		rest, ok := strings.CutPrefix(r.URL.EscapedPath(), escaped)
		if !ok || rest != "" && rest[0] != '/' {
			http.NotFound(w, r)
			return
		}

		clean := path.Clean("/" + rest)
		if strings.HasSuffix(rest, "/") && clean != "/" {
			clean += "/" // which path.Clean removes
		}
		if clean != rest {
			target := escaped + clean
			if r.URL.RawQuery != "" {
				target += "?" + r.URL.RawQuery
			}
			// The status keeps the request's method, as the mux's does.
			http.Redirect(w, r, target, http.StatusTemporaryRedirect) //nolint:gosec // the request adds only a path and a query after the prefix
			return
		}

		strip.ServeHTTP(w, r)
	})
}

// deadline bounds every request, so that a slow database cannot hold the
// dashboard's requests, and the connections they use, for long.
func deadline(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// basicAuth requires the one user of auth. It compares hashes of the
// credentials in constant time, so neither their content nor their length
// leaks through timing.
func basicAuth(next http.Handler, auth BasicAuth) http.Handler {
	wantUser, wantPass := sha256.Sum256([]byte(auth.Username)), sha256.Sum256([]byte(auth.Password))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		gotUser, gotPass := sha256.Sum256([]byte(user)), sha256.Sum256([]byte(pass))
		match := subtle.ConstantTimeCompare(gotUser[:], wantUser[:]) & subtle.ConstantTimeCompare(gotPass[:], wantPass[:])
		if !ok || match != 1 {
			if r.Header.Get(liveHeader) == "" {
				w.Header().Set("WWW-Authenticate", `Basic realm="taskmill", charset="UTF-8"`)
			}
			http.Error(w, "taskmill: log in to continue", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// securityHeaders sets headers that keep the dashboard from being framed by
// pages not listed, sniffed, or made to load anything but its own assets.
func securityHeaders(next http.Handler, ancestors string) http.Handler {
	csp := "default-src 'none'; style-src 'self'; script-src 'self'; img-src 'self'; connect-src 'self'; " +
		"form-action 'self'; base-uri 'none'; frame-ancestors " + ancestors
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", csp)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "same-origin")
		h.Set("Cache-Control", "no-store")
		if ancestors == "'none'" {
			h.Set("X-Frame-Options", "DENY") // for browsers without frame-ancestors
		}

		next.ServeHTTP(w, r)
	})
}

// buffers and gzipWriters are reused across requests: live updates render
// pages every few seconds.
var buffers, gzipWriters sync.Pool

func getBuffer() *bytes.Buffer {
	if b, ok := buffers.Get().(*bytes.Buffer); ok {
		b.Reset()
		return b
	}
	return new(bytes.Buffer)
}

func getGzip(w io.Writer) *gzip.Writer {
	if zw, ok := gzipWriters.Get().(*gzip.Writer); ok {
		zw.Reset(w)
		return zw
	}
	zw, _ := gzip.NewWriterLevel(w, gzip.BestSpeed) // the level is valid
	return zw
}

// render executes a page into a buffer before it answers, so that a failed
// template never sends half a page. A page that the client already has, by
// its ETag, is answered with 304 Not Modified, and others are compressed when
// the client accepts it: live updates fetch the page every few seconds. The
// page names its time of rendering, from which the script keeps its
// relative times counting. That time is no part of the ETag: a page that
// says what the client's page says is the page that the client has.
func (s *server) render(w http.ResponseWriter, r *http.Request, code int, name string, p page) {
	p.ReadOnly = s.readOnly
	p.Notice = notice(r.URL.Query())
	p.Now = time.Now()
	buf := getBuffer()
	defer buffers.Put(buf)
	if err := s.pages.ExecuteTemplate(buf, name, p); err != nil {
		s.logger.LogAttrs(r.Context(), slog.LevelError, "render page failed",
			slog.String(logKeyPage, name), slog.Any(logKeyError, err))
		http.Error(w, "taskmill: the page failed to render; the server log has the details", http.StatusInternalServerError)
		return
	}

	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Vary", "Accept-Encoding")
	if code == http.StatusOK {
		before, after, _ := bytes.Cut(buf.Bytes(), []byte(`data-now="`+instant(p.Now)+`"`))
		sum := sha256.New()
		sum.Write(before)
		sum.Write(after)
		etag := `W/"` + hex.EncodeToString(sum.Sum(nil)[:8]) + `"`
		h.Set("ETag", etag)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	body := buf.Bytes()
	if len(body) >= gzipMin && acceptsGzip(r) {
		zbuf := getBuffer()
		defer buffers.Put(zbuf)
		zw := getGzip(zbuf)
		_, _ = zw.Write(body) // writes to a buffer cannot fail
		_ = zw.Close()
		gzipWriters.Put(zw)
		body = zbuf.Bytes()
		h.Set("Content-Encoding", "gzip")
	}

	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(code)
	_, _ = w.Write(body) // the client may be gone; there is no one to tell
}

// acceptsGzip reports whether a request accepts a gzip response, as RFC 9110
// reads Accept-Encoding, over all its lines: by the weight of gzip, or else
// of "*", the other codings. A weight of 0 refuses, and so does one that is
// not a number from 0 to 1.
func acceptsGzip(r *http.Request) bool {
	named, others := -1.0, -1.0 // the weights; -1 if not named
	for _, line := range r.Header.Values("Accept-Encoding") {
		for part := range strings.SplitSeq(line, ",") {
			coding, params, _ := strings.Cut(part, ";")
			switch strings.ToLower(strings.TrimSpace(coding)) {
			case "gzip":
				named = weight(params)
			case "*":
				others = weight(params)
			}
		}
	}

	if named < 0 {
		return others > 0
	}
	return named > 0
}

// weight reads the weight of a coding, such as " q=0.5": 1 if it has none,
// and 0 if it is not a number from 0 to 1.
func weight(params string) float64 {
	name, value, ok := strings.Cut(params, "=")
	if !ok || !strings.EqualFold(strings.TrimSpace(name), "q") {
		return 1
	}
	if q, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil && q >= 0 && q <= 1 {
		return q
	}
	return 0
}

// path returns an absolute path inside the dashboard.
func (s *server) path(p string) string {
	return s.prefix + p
}

// asset returns the path of a static file, versioned by its content so that
// browsers may cache it forever.
func (s *server) asset(name string) string {
	return s.path("/static/" + name + "?v=" + s.versions[name])
}

// staticFile is a static file, kept with its compressed form.
type staticFile struct {
	body, gzipped []byte
	contentType   string
}

// loadStatic reads every static file, compresses it once, and hashes its
// content for its versioned URL.
func loadStatic() (map[string]staticFile, map[string]string) {
	files, versions := map[string]staticFile{}, map[string]string{}
	entries, _ := assets.ReadDir("static") // the directory is embedded, in ui.go
	for _, e := range entries {
		body, _ := assets.ReadFile("static/" + e.Name())
		sum := sha256.Sum256(body)
		versions[e.Name()] = hex.EncodeToString(sum[:4])
		var z bytes.Buffer
		zw, _ := gzip.NewWriterLevel(&z, gzip.BestCompression) // the level is valid
		_, _ = zw.Write(body)
		_ = zw.Close()
		f := staticFile{body: body, contentType: mime.TypeByExtension(path.Ext(e.Name()))}
		if z.Len() < len(body) {
			f.gzipped = z.Bytes()
		}
		files[e.Name()] = f
	}

	return files, versions
}

// static serves the static files. A URL with the current version of a file
// may be cached for a year; any other, such as one from a page that an older
// server rendered, must be checked again.
func (s *server) static() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		f, ok := s.files[name]
		if !ok {
			http.NotFound(w, r)
			return
		}

		h := w.Header()
		h.Set("Content-Type", f.contentType)
		h.Set("Vary", "Accept-Encoding")
		if r.URL.Query().Get("v") == s.versions[name] {
			h.Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			h.Set("Cache-Control", "no-cache")
		}

		body := f.body
		if f.gzipped != nil && acceptsGzip(r) {
			body = f.gzipped
			h.Set("Content-Encoding", "gzip")
		}

		h.Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body) // the client may be gone; there is no one to tell
	})
}
