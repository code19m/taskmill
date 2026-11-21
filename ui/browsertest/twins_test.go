package browsertest

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/chromedp"

	"github.com/code19m/taskmill/ui/internal/twins"
)

// twin returns the source of a word function of the script: from its
// declaration, which starts with decl, to the blank line after it. The
// script keeps its functions to itself, so the test reads them from it.
func twin(t *testing.T, script, decl string) string {
	t.Helper()
	start := strings.Index(script, "\n  "+decl)
	if start < 0 {
		t.Fatalf("the script declares no %q", decl)
	}
	end := strings.Index(script[start:], "\n\n")
	if end < 0 {
		t.Fatalf("the script's %q has no end", decl)
	}
	return script[start : start+end]
}

// The script words a relative time, a length of time that counts, and a
// name with characters that no one could see as the server does: it keeps
// up to date, or adds to, a page that the server wrote. Its word functions
// get the cases of their Go twins, as the script calls them: ago with the
// seconds between two times read to the second, and span with whole
// seconds.
func TestScriptWordsAsTheServerDoes(t *testing.T) {
	source, err := os.ReadFile("../static/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := string(source)

	var agoIn, spanIn []int64
	var visibleIn, agoWant, spanWant, visibleWant []string
	for _, c := range twins.Ago {
		agoIn, agoWant = append(agoIn, twins.Now.Unix()-twins.Now.Add(-c.Before).Unix()), append(agoWant, c.Want)
	}
	for _, c := range twins.Span {
		if c.D%time.Second == 0 {
			spanIn, spanWant = append(spanIn, int64(c.D/time.Second)), append(spanWant, c.Want)
		}
	}
	for _, c := range twins.Visible {
		visibleIn, visibleWant = append(visibleIn, c.Name), append(visibleWant, c.Want)
	}

	// A blank page has no content security policy, which would refuse the
	// functions that the test gives it.
	p := newTab(t)
	p.do(chromedp.Navigate("about:blank"))
	got := eval[struct{ Ago, Span, Visible []string }](p, `(() => {
		`+twin(t, script, "function ago(")+`
		`+twin(t, script, "function span(")+`
		`+twin(t, script, "const visible = ")+`
		return {
			Ago: `+js(agoIn)+`.map((s) => ago(s)),
			Span: `+js(spanIn)+`.map((s) => span(s)),
			Visible: `+js(visibleIn)+`.map((name) => visible(name)),
		};
	})()`)
	sameWords(t, "ago", agoIn, got.Ago, agoWant)
	sameWords(t, "span", spanIn, got.Span, spanWant)
	sameWords(t, "visible", visibleIn, got.Visible, visibleWant)
}

// sameWords compares the words that the script's function name gave for
// each input with those of its twin.
func sameWords[T any](t *testing.T, name string, in []T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("the script's %s gave %d words for %d cases", name, len(got), len(want))
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("the script's %s(%s) is %q, want %q, as the server's", name, js(in[i]), got[i], w)
		}
	}
}
