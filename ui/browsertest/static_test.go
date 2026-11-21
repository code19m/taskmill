package browsertest

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/chromedp"

	"github.com/code19m/taskmill/ui"
)

// started is true on a page once its script started: the script says
// whether the page is live.
const started = `document.querySelector("[data-live-toggle]").dataset.state === "live"`

// The page loads the script's modules from the folder of its version, with
// no error, such as a module that does not load or that the content
// security policy refuses: newTab fails the test on one. The browser asks
// for each file once: the page preloads every module that the script
// imports, as the script then imports it.
func TestScriptLoadsAsModules(t *testing.T) {
	s := newSite(t)
	p := s.open(t, "/tasks")
	p.waitFor(started, "the script starting")

	want := eval[[]string](p, `[...document.head.querySelectorAll("script[src], link[rel=stylesheet], link[rel=modulepreload]")]
		.map((el) => el.getAttribute("src") ?? el.getAttribute("href")).sort()`)
	if len(want) < 4 {
		t.Fatalf("the page links only %q", want)
	}

	var got []string
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		// A headless browser may not ask for the icon.
		got = slices.DeleteFunc(s.staticFiles(), func(path string) bool { return strings.HasSuffix(path, "/favicon.svg") })
		if len(got) >= len(want) || time.Now().After(deadline) {
			break
		}
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("the browser asked for %q, want each file of the page once: %q", got, want)
	}
}

// Behind basic authentication, as the command taskmill-ui serves the
// dashboard, the browser sends the login with its requests for the modules
// too, so the script starts.
func TestScriptLoadsBehindBasicAuth(t *testing.T) {
	s := newSiteWith(t, &ui.Config{BasicAuth: &ui.BasicAuth{Username: "ops", Password: "s3cret"}})
	p := newTab(t)
	p.do(chromedp.Navigate(strings.Replace(s.url, "://", "://ops:s3cret@", 1) + "/tasks"))
	p.waitFor(started, "the script starting")
}

// When a module does not load, as during a rolling deploy, the script does
// not start, and the page shows the controls of a page without it: the
// Apply button of the filters and the browser's fields of a custom range,
// and no Live button.
func TestPageWhoseModuleFailsWorksWithoutItsScript(t *testing.T) {
	s := newSite(t)
	s.missing.Store("tips.js")
	// Not newTab, which fails the test when a static file does not load.
	ctx, cancel := chromedp.NewContext(browser)
	t.Cleanup(cancel)
	p := &page{t, ctx}
	p.do(chromedp.Navigate(s.url + "/tasks"))
	p.waitFor(`document.readyState === "complete"`, "the page loading")

	if !slices.ContainsFunc(s.staticFiles(), func(path string) bool { return strings.HasSuffix(path, "/tips.js") }) {
		t.Fatal("the page did not ask for tips.js")
	}
	shown := eval[map[string]bool](p, `(() => {
		const shows = (selector) => getComputedStyle(document.querySelector(selector)).display !== "none";
		return { apply: shows(".filters [data-apply]"), field: shows(".field"), live: shows("[data-live-toggle]") };
	})()`)
	if !shown["apply"] || !shown["field"] || shown["live"] {
		t.Errorf("without its script, the page shows %v; want the Apply button and the fields, and no Live button", shown)
	}
}

// A page that a server of another version rendered links the static files
// of that version. This server answers with its own, which the browser must
// check again, so the page and its script work. The first live update
// brings a page of this version, and the page loads anew, so that new
// markup never meets old code.
func TestPageOfAnotherVersionWorksAndLoadsAnew(t *testing.T) {
	s := newSite(t)
	s.enqueue(t, 1)
	s.old.Store(true)
	p := s.open(t, "/tasks")
	p.waitFor(started, "the script of the page of another version starting")
	if !eval[bool](p, `document.querySelector("script[type=module]").getAttribute("src") === "/static/`+oldVersion+`/app.js" &&
		document.querySelector("tbody td[data-time]").title !== ""`) {
		t.Error("the page of another version does not show its times in the reader's time zone")
	}
	for _, path := range s.staticFiles() {
		if !strings.HasPrefix(path, "/static/"+oldVersion+"/") {
			t.Errorf("the page of another version asked for %s", path)
		}
	}

	p.run(`window.old = true`)
	s.old.Store(false)
	p.update()
	// The page loads anew: until it does, an expression may meet the page
	// that leaves.
	const loaded = `!window.old && !document.querySelector("script[type=module]").getAttribute("src").startsWith("/static/` +
		oldVersion + `/") && ` + started
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if ok, err := chromedp.Run(p.ctx, chromedp.Evaluate[bool](loaded)); err == nil && ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("within 10 seconds, the page of another version did not load anew in this version")
		}
	}
}
