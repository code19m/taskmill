package browsertest

import (
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/chromedp"

	"github.com/code19m/taskmill/ui/internal/twins"
)

// twin returns the source of a word function of the script, from the
// module that declares it: from its declaration, which starts with decl, to
// the blank line after it. The modules keep these functions to themselves,
// so the test reads them from the source.
func twin(t *testing.T, module, decl string) string {
	t.Helper()
	source, err := fs.ReadFile(os.DirFS("../static"), module)
	if err != nil {
		t.Fatal(err)
	}

	script := string(source)
	start := strings.Index(script, "\n"+decl)
	if start < 0 {
		t.Fatalf("%s declares no %q", module, decl)
	}
	end := strings.Index(script[start+1:], "\n\n")
	if end < 0 {
		t.Fatalf("%s's %q has no end", module, decl)
	}
	return script[start+1 : start+1+end]
}

// The script words a relative time, a length of time that counts, and a
// name with characters that no one could see as the server does: it keeps
// up to date, or adds to, a page that the server wrote. Its word functions
// get the cases of their Go twins, as the script calls them: ago with the
// seconds between two times read to the second, and span with whole
// seconds.
func TestScriptWordsAsTheServerDoes(t *testing.T) {
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
		`+twin(t, "times.js", "function ago(")+`
		`+twin(t, "times.js", "function span(")+`
		`+twin(t, "menus.js", "const visible = ")+`
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
