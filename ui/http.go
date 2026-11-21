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
// the client accepts it: live updates fetch the page every few seconds.
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
		tag := etag(buf.Bytes(), p.Now)
		h.Set("ETag", tag)
		if r.Header.Get("If-None-Match") == tag {
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

// timeAttrs are the attributes of a page that name a time: when the page
// was rendered, and the time of an element whose relative time counts from
// then, with the function that words it.
var timeAttrs = []struct {
	name  []byte
	words func(now, t time.Time) string
}{
	{[]byte(` data-now="`), nil},
	{[]byte(` data-time="`), ago},
	{[]byte(` data-since="`), lasting},
}

// etag returns the ETag of a page rendered at now: a hash of what the page
// says, less what the script keeps up to date on the page that it shows. So
// a page that says what the client's page says, once its times counted on,
// is the page that the client has. The script counts from the time of
// rendering, in data-now, the relative times of the page: the words that end
// the text of an element with a time, in data-time or data-since, if they
// are that time's words (see count in times.js). Other words, such as
// "paused", and the times themselves stay in the hash. Each part left out
// leaves a zero byte in its place, which no page holds.
func etag(page []byte, now time.Time) string {
	sum := sha256.New()
	hashed := 0 // page[:hashed] is in the hash
	leaveOut := func(from, to int) {
		sum.Write(page[hashed:from])
		sum.Write([]byte{0})
		hashed = to
	}

	for at := 0; ; {
		i := bytes.Index(page[at:], []byte(" data-"))
		if i < 0 {
			break
		}
		at += i

		a := 0
		for a < len(timeAttrs) && !bytes.HasPrefix(page[at:], timeAttrs[a].name) {
			a++
		}
		if a == len(timeAttrs) {
			at++
			continue
		}

		start := at + len(timeAttrs[a].name)
		end := bytes.IndexByte(page[start:], '"')
		if end < 0 {
			break
		}
		end += start
		at = end
		words := timeAttrs[a].words
		if words == nil {
			leaveOut(start, end)
			continue
		}

		// An element whose text counts names its time last.
		t, err := time.Parse(time.RFC3339, string(page[start:end]))
		if err == nil && bytes.HasPrefix(page[end:], []byte(`">`)) {
			if from, to, ok := endsWith(page, end+2, words(now, t)); ok {
				leaveOut(from, to)
				at = to
			}
		}
	}

	sum.Write(page[hashed:])
	return `W/"` + hex.EncodeToString(sum.Sum(nil)[:8]) + `"`
}

// endsWith finds words at the end of the text of the element whose content
// starts at from, after its first child elements, such as an icon. It
// returns where the words are, and whether they are there.
func endsWith(page []byte, from int, words string) (start, end int, ok bool) {
	for from < len(page) && page[from] == '<' {
		gt := bytes.IndexByte(page[from:], '>')
		if gt < 0 {
			return 0, 0, false
		}
		from += gt + 1
	}

	end = bytes.IndexByte(page[from:], '<') + from
	if end < from || !bytes.HasPrefix(page[end:], []byte("</")) || !bytes.HasSuffix(page[from:end], []byte(words)) {
		return 0, 0, false
	}
	return end - len(words), end, true
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

// asset returns the path of a static file, in the folder of the current
// version, so that browsers may cache it forever.
func (s *server) asset(name string) string {
	return s.path("/static/" + s.statics.version + "/" + name)
}

// statics are the static files, kept with their compressed forms, under one
// version.
type statics struct {
	files   map[string]staticFile
	version string   // a hash of every static file: the folder of their paths
	modules []string // the modules of the script that the page preloads
}

// staticFile is a static file, kept with its compressed form.
type staticFile struct {
	body, gzipped []byte
	contentType   string
}

// contentTypes are the types of the static files, by extension. They are
// set here, as mime.TypeByExtension reads the host's tables: one that names
// another type for .js or .css would stop the script or the styles, which a
// browser runs or applies only with their own types. A static file of
// another extension needs its type here: without one, loadStatic panics.
var contentTypes = map[string]string{
	".css": "text/css; charset=utf-8",
	".js":  "text/javascript; charset=utf-8",
	".svg": "image/svg+xml",
}

// loadStatic reads every static file, compresses it once, and hashes them
// all for their version.
func loadStatic() statics {
	st := statics{files: map[string]staticFile{}}
	sum := sha256.New()
	entries, _ := assets.ReadDir("static") // the directory is embedded, in ui.go; its entries come by name
	for _, e := range entries {
		name := e.Name()
		typ, ok := contentTypes[path.Ext(name)]
		if !ok {
			// The files are embedded, so every test run finds such a file,
			// as template.Must finds a broken template.
			panic("ui: the static file " + name + " has no type in contentTypes")
		}

		body, _ := assets.ReadFile("static/" + name)
		hash := sha256.Sum256(body)
		sum.Write([]byte(name + "\x00"))
		sum.Write(hash[:])

		var z bytes.Buffer
		zw, _ := gzip.NewWriterLevel(&z, gzip.BestCompression) // the level is valid
		_, _ = zw.Write(body)
		_ = zw.Close()
		f := staticFile{body: body, contentType: typ}
		if z.Len() < len(body) {
			f.gzipped = z.Bytes()
		}
		st.files[name] = f

		// The page loads app.js, and preloads the modules that it imports.
		// theme.js is a classic script, which runs before the page paints.
		if path.Ext(name) == ".js" && name != "app.js" && name != "theme.js" {
			st.modules = append(st.modules, name)
		}
	}

	st.version = hex.EncodeToString(sum.Sum(nil)[:4])
	return st
}

// static serves the static files, at /static/{version}/{name}. One version
// covers every file, so a module's import of another, such as "./morph.js",
// stays in its version, and a page gets the files of one version. Those of
// the current version may be cached for a year. A request for another
// version, such as one from a page that an older server rendered, gets the
// current file, which the browser must check again: the page works, and its
// script reloads it once a live update brings a page of the current version.
func (s *server) static() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, ok := s.statics.files[r.PathValue("name")]
		if !ok {
			http.NotFound(w, r)
			return
		}

		h := w.Header()
		h.Set("Content-Type", f.contentType)
		h.Set("Vary", "Accept-Encoding")
		if r.PathValue("version") == s.statics.version {
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
