// Package docs checks the links between the repository's Markdown files.
package docs

import (
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

// TestLinks checks every relative link in the repository's Markdown files:
// the file it points to must exist, and its #anchor must match a heading
// there, as GitHub makes anchors. Links with a scheme, such as https: or
// mailto:, and links in code are not checked.
func TestLinks(t *testing.T) {
	t.Parallel()
	c := &checker{repo: os.DirFS("../.."), anchors: make(map[string]map[string]int)}
	files, err := markdownFiles(c.repo)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(files, "README.md") {
		t.Fatal("the repository's README.md is missing: is the test run from internal/docs?")
	}
	for _, file := range files {
		text, err := fs.ReadFile(c.repo, file)
		if err != nil {
			t.Fatal(err)
		}
		prose, _ := parse(string(text))
		for _, l := range linksOf(prose) {
			if err := c.check(file, l.target); err != nil {
				t.Errorf("%s:%d: link %q: %v", file, l.line, l.target, err)
			}
		}
	}
}

// skipped are the directories whose Markdown is not part of the repository:
// git's, the worktrees of coding agents, and downloaded tools.
var skipped = map[string]bool{".git": true, ".claude": true, "bin": true}

// markdownFiles returns the paths of the Markdown files in repo.
func markdownFiles(repo fs.FS) ([]string, error) {
	var files []string
	err := fs.WalkDir(repo, ".", func(name string, d fs.DirEntry, err error) error {
		switch {
		case err != nil:
			return err
		case d.IsDir() && skipped[d.Name()]:
			return fs.SkipDir
		case !d.IsDir() && path.Ext(name) == ".md":
			files = append(files, name)
		}
		return nil
	})
	return files, err
}

var (
	// fence opens or closes a fenced code block, also in a list or a quote.
	fence = regexp.MustCompile("^[ \t>]*(`{3,}|~{3,})(.*)$")

	// heading is an ATX heading, such as "## Title", the kind the docs use.
	heading = regexp.MustCompile(`^ {0,3}#{1,6}[ \t]+(.*?)(?:[ \t]+#+)?[ \t]*$`)

	// The places a link can be: inline, as in [text](target) and
	// ![alt](target); a reference definition, [label]: target; and an HTML
	// attribute.
	inline     = regexp.MustCompile(`\]\(\s*<?([^\s)>]+)`)
	definition = regexp.MustCompile(`(?m)^ {0,3}\[[^\]^][^\]]*\]:\s*<?([^\s>]+)`)
	attribute  = regexp.MustCompile(`\b(?:href|src|srcset)="([^"]+)"`)

	// external is a link with a scheme, such as https: or mailto:.
	external = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9+.-]*:`)

	// linkText is a link in a heading, whose anchor keeps only the text.
	linkText = regexp.MustCompile(`!?\[([^\]]*)\]\([^)]*\)`)
)

// parse returns text with its code blanked out, and its headings. Fenced
// code blocks and code spans become spaces, so that links in code are not
// checked, while line numbers stay as they were.
func parse(text string) (prose string, headings []string) {
	lines := strings.SplitAfter(text, "\n")
	open := "" // the fence of the code block that the line is in, if any
	for i, line := range lines {
		bare := strings.TrimRight(line, "\r\n")
		if m := fence.FindStringSubmatch(bare); m != nil {
			switch {
			case open == "":
				open = m[1]
			case m[1][0] == open[0] && len(m[1]) >= len(open) && strings.TrimSpace(m[2]) == "":
				open = ""
			}
			lines[i] = blank(line)
			continue
		}
		if open != "" {
			lines[i] = blank(line)
			continue
		}
		if m := heading.FindStringSubmatch(bare); len(m) == 2 && m[1] != "" {
			headings = append(headings, m[1])
		}
	}
	return blankCodeSpans(strings.Join(lines, "")), headings
}

// blank replaces every character of s with a space, except line breaks.
func blank(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' {
			return r
		}
		return ' '
	}, s)
}

// blankCodeSpans blanks out the code spans of s: a run of backticks, up to
// the next run of the same length in the same paragraph.
func blankCodeSpans(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] != '`' {
			b.WriteByte(s[i])
			i++
			continue
		}
		n := backticks(s, i)
		end := spanEnd(s, i+n, n)
		if end < 0 {
			b.WriteString(s[i : i+n])
			i += n
			continue
		}
		b.WriteString(blank(s[i:end]))
		i = end
	}
	return b.String()
}

// backticks returns the length of the run of backticks at s[i].
func backticks(s string, i int) int {
	n := 0
	for i+n < len(s) && s[i+n] == '`' {
		n++
	}
	return n
}

// spanEnd returns where the code span whose content starts at s[from] ends,
// after its closing run of n backticks, or -1 if a blank line comes first.
func spanEnd(s string, from, n int) int {
	empty := false // the current line holds only spaces so far
	for j := from; j < len(s); {
		switch c := s[j]; c {
		case '\n':
			if empty {
				return -1
			}
			empty = true
			j++
		case '`':
			m := backticks(s, j)
			if m == n {
				return j + m
			}
			empty = false
			j += m
		default:
			if c != ' ' && c != '\t' {
				empty = false
			}
			j++
		}
	}
	return -1
}

// link is a link target and the line it is on.
type link struct {
	line   int
	target string
}

// linksOf returns the links of prose, in the order of their lines.
func linksOf(prose string) []link {
	var links []link
	for _, re := range []*regexp.Regexp{inline, definition, attribute} {
		for _, m := range re.FindAllStringSubmatchIndex(prose, -1) {
			links = append(links, link{line: 1 + strings.Count(prose[:m[2]], "\n"), target: prose[m[2]:m[3]]})
		}
	}
	slices.SortStableFunc(links, func(a, b link) int { return a.line - b.line })
	return links
}

// checker checks the links of the Markdown files in repo.
type checker struct {
	repo    fs.FS
	anchors map[string]map[string]int // the anchors of each file read so far
}

// check returns why target, a link in file, is broken, or nil.
func (c *checker) check(file, target string) error {
	if external.MatchString(target) {
		return nil
	}
	ref, fragment, _ := strings.Cut(target, "#")
	ref, _, _ = strings.Cut(ref, "?")
	ref, err := url.PathUnescape(ref)
	if err != nil {
		return err
	}
	dest := file
	switch {
	case strings.HasPrefix(ref, "/"): // GitHub reads it from the repository's root
		dest = path.Clean(ref[1:])
	case ref != "":
		dest = path.Join(path.Dir(file), ref)
	}
	if dest == ".." || strings.HasPrefix(dest, "../") {
		return errors.New("it points outside the repository")
	}
	if _, err := fs.Stat(c.repo, dest); err != nil {
		return fmt.Errorf("%s does not exist", dest)
	}
	if fragment == "" || path.Ext(dest) != ".md" {
		return nil
	}
	if fragment, err = url.PathUnescape(fragment); err != nil {
		return err
	}
	anchors, err := c.anchorsOf(dest)
	if err != nil {
		return err
	}
	if _, ok := anchors[fragment]; !ok {
		return fmt.Errorf("%s has no heading with the anchor #%s", dest, fragment)
	}
	return nil
}

// anchorsOf returns the anchors of file's headings.
func (c *checker) anchorsOf(file string) (map[string]int, error) {
	if anchors, ok := c.anchors[file]; ok {
		return anchors, nil
	}
	text, err := fs.ReadFile(c.repo, file)
	if err != nil {
		return nil, err
	}
	_, headings := parse(string(text))
	anchors := make(map[string]int) // as GitHub counts them: each anchor, and how often its slug repeats
	for _, h := range headings {
		base := slug(h)
		anchor := base
		for _, taken := anchors[anchor]; taken; _, taken = anchors[anchor] {
			anchors[base]++
			anchor = base + "-" + strconv.Itoa(anchors[base])
		}
		anchors[anchor] = 0
	}
	c.anchors[file] = anchors
	return anchors, nil
}

// slug returns the anchor that GitHub makes of a heading's text: lowercase,
// without punctuation except hyphens and underscores, and with a hyphen for
// each space.
func slug(heading string) string {
	heading = linkText.ReplaceAllString(heading, "$1")
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(heading)) {
		switch {
		case r == ' ':
			b.WriteByte('-')
		case r == '-' || r == '_' || unicode.In(r, unicode.L, unicode.M, unicode.N):
			b.WriteRune(r)
		}
	}
	return b.String()
}
