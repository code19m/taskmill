package docs

import (
	"flag"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
)

// The tests in this file keep the short ways into the docs true: llms.txt,
// which names every package and every page in one line each, and the
// paragraph that opens each page. A reader with little time, or a tool with
// a small budget, reads only those.

// llmsFile is the index for tools, at the module's root. It ships with the
// module, so its paths lead to the pages of the version in use.
const llmsFile = "llms.txt"

// update makes TestLLMs write llmsFile instead of comparing it.
var update = flag.Bool("update", false, "write llms.txt from the section indexes and the package comments")

// llmsNote says how to read llmsFile. It is the only text of the file that
// comes from no index and no package comment.
const llmsNote = "The Go doc comments are the API reference: the comment of each identifier\n" +
	"states its default, its limits, and its errors. Run `go doc <package>` for the\n" +
	"map of a package, and `go doc <package>.<Name>` for one identifier. The pages\n" +
	"teach, and hold what has no identifier. Their paths start at the module's root.\n"

var (
	// rowLink is a link with its text in a cell of a table.
	rowLink = regexp.MustCompile(`\[([^\]]+)\]\(([^)\s]+)\)`)

	// counter is a cell that only counts or measures: a row's number in the
	// reading order, or its minutes.
	counter = regexp.MustCompile(`^\d+(?: min)?$`)
)

// purposes returns what the tables of a section's index say about each page
// of the section: for a row with a cell that holds only a link to the page,
// the text of the row's other cells. A page's first row counts.
func purposes(t *testing.T, index string) map[string]string {
	t.Helper()
	text := read(t, index)
	prose, _ := parse(text)
	out := make(map[string]string)
	for _, tb := range tablesOf(page{name: index, text: text, prose: prose}) {
		for _, row := range tb.rows {
			dest, purpose := rowPurpose(index, row)
			if _, seen := out[dest]; dest != "" && !seen {
				out[dest] = purpose
			}
		}
	}
	return out
}

// rowPurpose returns the page of index's directory that a row links to, in
// a cell of its own, and the text of the other cells. It returns no page if
// the row has none.
func rowPurpose(index string, row []string) (dest, purpose string) {
	var rest []string
	for _, cell := range row {
		if m := rowLink.FindStringSubmatch(cell); dest == "" && m != nil && m[0] == cell {
			target, _, err := resolve(index, m[2])
			if err == nil && path.Dir(target) == path.Dir(index) && path.Ext(target) == ".md" {
				dest = target
				continue
			}
		}
		if text := rowLink.ReplaceAllString(cell, "$1"); !counter.MatchString(text) {
			rest = append(rest, text)
		}
	}
	return dest, strings.Join(rest, ". ")
}

// llms returns the text of llmsFile: what taskmill is, from the home of the
// docs, the packages with the first sentence of their comments, and every
// page of every section with what its section's index says about it. It
// also returns the pages that no index describes.
func llms(t *testing.T) (text string, missing []string) {
	t.Helper()
	var b strings.Builder
	intro := strings.Join(strings.Fields(strings.SplitN(read(t, home), "\n\n", 3)[1]), " ")
	b.WriteString("# taskmill\n\n> " + intro + "\n\n" + llmsNote + "\n## Packages\n\n")

	for _, p := range apiPackages(t) {
		b.WriteString("- `" + importPath(p.dir) + "`: " + p.synopsis + "\n")
	}

	order := readingOrder(t)
	for _, section := range sections {
		index := "docs/" + section + "/README.md"
		purpose := purposes(t, index)
		b.WriteString("\n## " + title(t, index) + "\n\n")
		for _, name := range order {
			if path.Dir(name) != path.Dir(index) || name == index {
				continue
			}
			if purpose[name] == "" {
				missing = append(missing, name)
				continue
			}
			b.WriteString("- [" + title(t, name) + "](" + name + "): " + purpose[name] + "\n")
		}
	}
	return b.String(), missing
}

// TestLLMs checks that llms.txt is what the sections' indexes and the
// package comments give, so that nobody writes it by hand and it cannot go
// stale. It also checks that an index says what each page of its section is
// for. Run the test with -update to write the file.
func TestLLMs(t *testing.T) {
	t.Parallel()
	want, missing := llms(t)
	for _, name := range missing {
		t.Errorf("%s: no table in the README.md of its folder says what the page is for, so %s cannot list it. "+
			"Add a row there: a link to the page alone in one cell, and in another cell when to read it",
			name, llmsFile)
	}

	if *update {
		if err := os.WriteFile("../../"+llmsFile, []byte(want), 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}

	got, err := os.ReadFile("../../" + llmsFile)
	if err != nil {
		t.Fatalf("%v. The file is generated: run go test ./internal/docs/ -run TestLLMs -update, and commit it", err)
	}

	gotLines, wantLines := strings.Split(string(got), "\n"), strings.Split(want, "\n")
	for i, line := range wantLines {
		if i >= len(gotLines) || gotLines[i] != line {
			t.Fatalf("%s:%d: the line differs from what the indexes and the package comments give: %q. "+
				"The file is generated: run go test ./internal/docs/ -run TestLLMs -update, and commit it", llmsFile, i+1, line)
		}
	}
	if len(gotLines) > len(wantLines) {
		t.Errorf("%s:%d: the file has lines that no index and no package comment gives. "+
			"It is generated: run go test ./internal/docs/ -run TestLLMs -update, and commit it", llmsFile, len(wantLines)+1)
	}
}

// maxSummary is the most words of the paragraph that opens a page.
const maxSummary = 80

// TestSummary checks that every page opens as "Page shape" in the guide
// says: the title, an empty line, and then a paragraph that says what the
// page is for. A reader who opens a page, or a tool that reads only its
// first lines, learns from them whether to read on.
func TestSummary(t *testing.T) {
	t.Parallel()
	for _, name := range readingOrder(t) {
		lines := strings.Split(read(t, name), "\n")
		if len(lines) < 3 || lines[1] != "" || lines[2] == "" {
			t.Errorf(`%s:2: "Page shape" in %s wants the title, one empty line, and then the paragraph that says what the page is for. Write it on line 3`,
				name, guide)
			continue
		}

		words := 0
		for _, line := range lines[2:] {
			if line == "" {
				break
			}
			words += len(strings.Fields(line))
		}
		switch {
		case strings.ContainsAny(lines[2][:1], "#|->`<!"):
			t.Errorf(`%s:3: the page opens with %q. "Page shape" in %s wants plain sentences first: write what the page helps the reader do, then the table, the list, or the code`,
				name, lines[2], guide)
		case words > maxSummary:
			t.Errorf(`%s:3: the opening paragraph has %d words. "Page shape" in %s wants one or two sentences, %d words at most: say what the page is for, and move the rest below`,
				name, words, guide, maxSummary)
		}
	}
}
