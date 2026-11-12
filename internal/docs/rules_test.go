package docs

import (
	"io/fs"
	"os"
	"path"
	"regexp"
	"slices"
	"strings"
	"testing"
	"unicode"
)

// The tests in this file check the rules of the style guide that a machine
// can check. Each rule's list is next to its test, with the place of the
// rule in the guide: change a rule there and here together.
const guide = "docs/design/writing-docs.md"

// root is the repository, as the tests see it from internal/docs.
var root = os.DirFS("../..")

// page is a Markdown file that the style guide binds.
type page struct {
	name  string // its path in the repository
	text  string // its text
	prose string // its text with the code blanked out, on the same lines
}

// record is the path of a decision record. An accepted record is never
// edited, so the rules skip the records.
var record = regexp.MustCompile(`^docs/adr/\d{4}-[^/]+\.md$`)

// files returns the paths of the repository's Markdown files.
func files(t *testing.T) []string {
	t.Helper()
	names, err := markdownFiles(root)
	if err != nil || !slices.Contains(names, guide) {
		t.Fatalf("%s is missing: is the test run from internal/docs? %v", guide, err)
	}
	return names
}

// read returns the text of a file of the repository.
func read(t *testing.T, name string) string {
	t.Helper()
	text, err := fs.ReadFile(root, name)
	if err != nil {
		t.Fatal(err)
	}
	return string(text)
}

// boundPages returns the Markdown files that the style guide binds: all but
// the decision records. Of a changelog, only the Unreleased section counts:
// its released sections are history.
func boundPages(t *testing.T) []page {
	t.Helper()
	var pages []page
	for _, name := range files(t) {
		if record.MatchString(name) {
			continue
		}
		text := read(t, name)
		if path.Base(name) == "CHANGELOG.md" {
			text = unreleased(text)
		}
		prose, _ := parse(text)
		pages = append(pages, page{name: name, text: text, prose: prose})
	}
	return pages
}

// unreleased blanks out every line of a changelog that is not in its
// Unreleased section.
func unreleased(text string) string {
	lines := strings.SplitAfter(text, "\n")
	keep := false
	for i, line := range lines {
		if strings.HasPrefix(line, "## ") {
			keep = strings.HasPrefix(line, "## [Unreleased]")
		}
		if !keep {
			lines[i] = blank(line)
		}
	}
	return strings.Join(lines, "")
}

// ref is a link from one file of the repository to another.
type ref struct {
	line           int    // where it is
	target         string // as the page writes it
	dest, fragment string // the file that it leads to, and the anchor there
}

// refs returns the links without a scheme in prose, the text of the file
// name with its code blanked out.
func refs(name, prose string) []ref {
	var out []ref
	for _, l := range linksOf(prose) {
		if external.MatchString(l.target) {
			continue
		}
		if dest, fragment, err := resolve(name, l.target); err == nil {
			out = append(out, ref{line: l.line, target: l.target, dest: dest, fragment: fragment})
		}
	}
	return out
}

// refsOf returns the links without a scheme in a file of the repository.
func refsOf(t *testing.T, name string) []ref {
	t.Helper()
	prose, _ := parse(read(t, name))
	return refs(name, prose)
}

// banned are the words and phrases that the guide bans, each with what to
// write instead: the examples of rule 3 (plain, literal words), the "Not"
// column of rule 4 (one name for one thing), and the examples of rule 14
// (write for now). Rule 4 also bans "job" for a task and "heartbeat" for a
// check-in, but only a reader can tell a wrong use from a cron job or from
// the maintainer's heartbeat.
var banned = []struct {
	phrase  *regexp.Regexp
	rule    int
	instead string
	except  string // the one page that may use the word, to explain it
}{
	{phrase(`without a word`), 3, `"silently"`, ""},
	{phrase(`mind`), 3, `"check"`, ""},
	{phrase(`go(?:es|ing)? on|went on`), 3, `"continues"`, ""},
	{phrase(`h[oe]ld(?:s|ing)? up`), 3, `"blocks"`, ""},
	{phrase(`give or take`), 3, `"±"`, ""},
	{phrase(`by a hair`), 3, `"just"`, ""},
	{phrase(`web interface|web UI`), 4, `"dashboard"`, ""},
	{phrase(`installations?`), 4, `"schema"`, "docs/concepts/glossary.md"},
	{phrase(`at once`), 4, `"immediately", "right away", or "in one go". If you mean "at the same time", write that`, ""},
	{phrase(`since v\d|before \d{4}-\d\d-\d\d|revised from`), 14, `how it works now: history goes in CHANGELOG.md`, ""},
}

// phrase returns the pattern of some words, in any case. The words match
// also where a line break in a paragraph, or in a quote block, splits them.
func phrase(words string) *regexp.Regexp {
	return regexp.MustCompile(`(?i)\b(?:` + strings.ReplaceAll(words, " ", `[\s>]+`) + `)\b`)
}

// TestWords checks that no page uses a banned word or phrase. Text in code
// stays as the code prints it, so the test skips it. It also skips a phrase
// that opens a quotation, as in "at once": the page names it there.
func TestWords(t *testing.T) {
	t.Parallel()
	for _, p := range boundPages(t) {
		for _, b := range banned {
			for _, m := range b.phrase.FindAllStringIndex(p.prose, -1) {
				before := p.prose[:m[0]]
				if p.name == b.except || strings.HasSuffix(before, `"`) || strings.HasSuffix(before, "“") {
					continue
				}
				words := strings.FieldsFunc(p.prose[m[0]:m[1]], func(r rune) bool { return unicode.IsSpace(r) || r == '>' })
				t.Errorf("%s:%d: %q: rule %d of %s bans it. Write %s",
					p.name, 1+strings.Count(before, "\n"), strings.Join(words, " "), b.rule, guide, b.instead)
			}
		}
	}
}

// Rule 9 of the guide: the alerts that have a meaning, and how many of them
// a page may have.
var (
	alertKinds = []string{"TIP", "NOTE", "WARNING"}
	maxAlerts  = 3

	// alert is the first line of an alert, such as "> [!NOTE]".
	alert = regexp.MustCompile(`(?m)^[ \t]*>[ \t]*\[!(\w+)\]`)
)

// TestAlerts checks that every alert is one of the three kinds, and that no
// page has more than three alerts.
func TestAlerts(t *testing.T) {
	t.Parallel()
	for _, p := range boundPages(t) {
		for i, m := range alert.FindAllStringSubmatchIndex(p.prose, -1) {
			kind, line := p.prose[m[2]:m[3]], 1+strings.Count(p.prose[:m[0]], "\n")
			if !slices.Contains(alertKinds, kind) {
				t.Errorf("%s:%d: [!%s]: rule 9 of %s has only [!TIP], [!NOTE], and [!WARNING]. "+
					"Write [!WARNING] if the reader can lose data or break production, and [!NOTE] if not",
					p.name, line, kind, guide)
			}
			if i == maxAlerts {
				t.Errorf("%s:%d: this is alert %d of the page: rule 9 of %s allows %d. Write one of them as plain text",
					p.name, line, i+1, guide, maxAlerts)
			}
		}
	}
}

// The limits table of the guide: the most lines that a page may have, and
// the one page that may be longer.
const (
	maxLines = 250
	longPage = "docs/reference/log-messages.md"
)

// TestPageLength checks that no page is longer than the limit. A changelog
// is not a page: it grows with every release.
func TestPageLength(t *testing.T) {
	t.Parallel()
	for _, p := range boundPages(t) {
		if p.name == longPage || path.Base(p.name) == "CHANGELOG.md" {
			continue
		}
		if n := len(strings.Split(strings.TrimRight(p.text, "\n"), "\n")); n > maxLines {
			t.Errorf("%s: %d lines: the limits table of %s allows %d. Split the page, or move its rare cases to godoc (rule 11)",
				p.name, n, guide, maxLines)
		}
	}
}

// Rule 15 of the guide, under "Page shape": the documentation home, and the
// sections in the order of the Next lines. Within a section, Next follows
// the order of the links in the section's README.md.
const home = "docs/README.md"

var sections = []string{"start", "tutorial", "concepts", "guides", "recipes", "production", "reference", "design"}

// readingOrder returns the pages in the order of the Next lines: the home,
// then each section's README.md, followed by the pages of its folder in the
// order in which that README.md first links to them.
func readingOrder(t *testing.T) []string {
	t.Helper()
	order := []string{home}
	for _, section := range sections {
		index := "docs/" + section + "/README.md"
		order = append(order, index)
		for _, r := range refsOf(t, index) {
			if path.Dir(r.dest) == path.Dir(index) && path.Ext(r.dest) == ".md" && !slices.Contains(order, r.dest) {
				order = append(order, r.dest)
			}
		}
	}
	return order
}

// linkTo returns the link from one page to another, with the other page's
// title as its text. The title is the page's first line, without the "# ".
func linkTo(t *testing.T, from, to string) string {
	t.Helper()
	first, _, _ := strings.Cut(read(t, to), "\n")
	title, _ := strings.CutPrefix(first, "# ")
	a, b := strings.Split(path.Dir(from), "/"), strings.Split(to, "/")
	for len(a) > 0 && len(b) > 1 && a[0] == b[0] {
		a, b = a[1:], b[1:]
	}
	return "[" + title + "](" + strings.Repeat("../", len(a)) + strings.Join(b, "/") + ")"
}

// TestNextChain checks that the Next lines make one chain. Every page of
// docs/, except the decision records, has one place in the reading order.
// It starts with its title, and its last line leads to the page after it,
// by that page's title. The last page leads back to the home. So a reader
// who starts at the home and follows Next visits every page exactly once.
func TestNextChain(t *testing.T) {
	t.Parallel()
	order := readingOrder(t)
	for _, name := range files(t) {
		if strings.HasPrefix(name, "docs/") && !strings.HasPrefix(name, "docs/adr/") && !slices.Contains(order, name) {
			t.Errorf("%s: no Next line leads to it, because the README.md of a section does not link to it. "+
				"Add a row for it to the README.md of its folder, which must be one of docs/{%s}",
				name, strings.Join(sections, ","))
		}
	}
	for i, name := range order {
		want := "**Next:** " + linkTo(t, name, order[(i+1)%len(order)])
		lines := strings.Split(strings.TrimRight(read(t, name), "\n"), "\n")
		if !strings.HasPrefix(lines[0], "# ") {
			t.Errorf(`%s:1: the first line is %q. "Page shape" in %s wants the title there: write "# Title"`, name, lines[0], guide)
		}
		if last := lines[len(lines)-1]; last != want {
			t.Errorf("%s:%d: the last line is %q. Rule 15 of %s wants the next page in the order of the section's README.md, by its title: write %q",
				name, len(lines), last, guide, want)
		}
	}
}

// titled is a link with its text, in a line outside code.
var titled = regexp.MustCompile(`\[([^\]]+)\]\(([^)\s]+)\)`)

// listed returns the links to pages of the reading order under a "## "
// heading of the home, as linkTo writes them, each with its line. With
// rows, it returns only the links in the rows of a table.
func listed(t *testing.T, order []string, heading string, rows bool) (links []string, lines []int) {
	t.Helper()
	in := false
	for i, line := range strings.Split(read(t, home), "\n") {
		if h, ok := strings.CutPrefix(line, "## "); ok {
			in = h == heading
		}
		if !in || rows && !strings.HasPrefix(line, "|") {
			continue
		}
		for _, m := range titled.FindAllStringSubmatch(line, -1) {
			if dest, _, err := resolve(home, m[2]); err == nil && slices.Contains(order[1:], dest) {
				links, lines = append(links, "["+m[1]+"]("+strings.TrimPrefix(dest, "docs/")+")"), append(lines, i+1)
			}
		}
	}
	return links, lines
}

// TestAllPages checks that "All pages" on the home names every section and
// every page, in the reading order and by their titles.
func TestAllPages(t *testing.T) {
	t.Parallel()
	order := readingOrder(t)
	links, lines := listed(t, order, "All pages", false)
	for i, name := range order[1:] {
		want := linkTo(t, home, name)
		if i == len(links) {
			t.Fatalf(`%s: "All pages" ends before %s. Add it: the list follows the README.md of each section`, home, want)
		}
		if links[i] != want { // the entries after the first wrong one are only shifted
			t.Fatalf(`%s:%d: "All pages" has %s here. The README.md of the section, and the page's title, want %s`,
				home, lines[i], links[i], want)
		}
	}
	if len(links) >= len(order) {
		t.Errorf(`%s:%d: "All pages" lists %s again. Remove it`, home, lines[len(order)-1], links[len(order)-1])
	}
}

// TestLearningPath checks that the learning path on the home names its
// pages by their titles, in the reading order: rule 15 makes no exception
// for it.
func TestLearningPath(t *testing.T) {
	t.Parallel()
	order := readingOrder(t)
	links, lines := listed(t, order, "Learning path", true)
	if len(links) == 0 {
		t.Fatalf(`%s: the table under "## Learning path" links to no page`, home)
	}
	at := 0 // the place in the reading order of the step before
	for i, link := range links {
		place := slices.IndexFunc(order, func(name string) bool { return linkTo(t, home, name) == link })
		switch {
		case place < 0:
			t.Errorf("%s:%d: the learning path has %s, which is not the page's title. Write the title, as \"All pages\" has it", home, lines[i], link)
		case place < at:
			t.Errorf("%s:%d: the learning path has %s after %s, but the Next lines lead the other way. Put the steps in the reading order",
				home, lines[i], link, linkTo(t, home, order[at]))
		default:
			at = place
		}
	}
}

// TestRecordsIndex checks that the index of the decision records links to
// every record.
func TestRecordsIndex(t *testing.T) {
	t.Parallel()
	const index = "docs/adr/README.md"
	var linked []string
	for _, r := range refsOf(t, index) {
		linked = append(linked, r.dest)
	}
	for _, name := range files(t) {
		if record.MatchString(name) && !slices.Contains(linked, name) {
			t.Errorf("%s: its table does not link to %s. Add a row for it", index, name)
		}
	}
}

// entryPoints are the Markdown files that need no link from another one:
// GitHub, or a coding agent, opens them by their names.
var entryPoints = []string{"README.md", "CLAUDE.md", ".github/pull_request_template.md"}

// TestOrphans checks that a reader can reach every Markdown file: another
// file links to it. Rule 15 of the guide gives every page a way forward;
// this is the way in.
func TestOrphans(t *testing.T) {
	t.Parallel()
	reached := make(map[string]bool)
	for _, name := range files(t) {
		for _, r := range refsOf(t, name) {
			if r.dest != name {
				reached[r.dest] = true
			}
		}
	}
	for _, name := range files(t) {
		if !reached[name] && !slices.Contains(entryPoints, name) {
			t.Errorf("%s: no other Markdown file links to it. Link to it from the README.md of its folder, or from the page that a reader comes from", name)
		}
	}
}

// Rule 5 of the guide: a page links a term to its entry in the glossary.
// The terms are the "###" headings there.
const glossary = "docs/concepts/glossary.md"

// TestGlossaryLinks checks that every link to the glossary names a term's
// anchor. Only a link whose text is "Glossary" may lead to the page itself.
func TestGlossaryLinks(t *testing.T) {
	t.Parallel()
	terms := make(map[string]bool)
	for line := range strings.SplitSeq(read(t, glossary), "\n") {
		if term, ok := strings.CutPrefix(line, "### "); ok {
			terms[slug(term)] = true
		}
	}
	if len(terms) == 0 {
		t.Fatalf(`%s has no "###" heading: where are its terms?`, glossary)
	}
	for _, p := range boundPages(t) {
		lines := strings.Split(p.text, "\n")
		for _, r := range refs(p.name, p.prose) {
			toPage := r.fragment == "" && strings.Contains(strings.ToLower(lines[r.line-1]), "[glossary]("+strings.ToLower(r.target)+")")
			if r.dest != glossary || terms[r.fragment] || toPage {
				continue
			}
			file, _, _ := strings.Cut(r.target, "#")
			t.Errorf("%s:%d: the link to %s names no term of the glossary. Rule 5 of %s wants the term's anchor: write %s#term, such as %s#lease",
				p.name, r.line, r.target, guide, file, file)
		}
	}
}
