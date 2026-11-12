package docs

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// codePages are the pages whose reader writes or runs the code they show,
// each with the Go files of its example program. The examples compile and
// have tests, so code that a page takes from them is code that works.
var codePages = map[string][]string{
	"docs/start/quickstart.md":                   {"examples/quickstart/main.go"},
	"docs/tutorial/1-first-task.md":              {"examples/quickstart/main.go", "examples/quickstart/main_test.go"},
	"docs/tutorial/2-tasks-with-your-data.md":    {"examples/quickstart/main.go", "examples/quickstart/main_test.go"},
	"docs/tutorial/3-when-a-task-fails.md":       {"examples/quickstart/main.go"},
	"docs/tutorial/4-queues-delays-schedules.md": {"examples/tutorial/main.go"},
	"docs/tutorial/5-outbox.md":                  {"examples/tutorial/main.go"},
	"docs/tutorial/6-test-it.md":                 {"examples/tutorial/main.go", "examples/tutorial/main_test.go"},
	"docs/tutorial/7-ready-to-ship.md":           {"examples/tutorial/main.go"},
}

// builtPrograms are the files that the tutorial's reader has written, each
// with the first and the last of the parts that write it.
var builtPrograms = []struct {
	file     string
	from, to int
}{
	{"examples/quickstart/main.go", 1, 3},
	{"examples/tutorial/main.go", 1, 7},
	{"examples/tutorial/main_test.go", 6, 6},
}

const (
	// omitted is the line of a block that stands for lines it leaves out.
	omitted = "// ..."

	// grows, on the line above a block, says that a later step of the
	// tutorial adds lines inside the block.
	grows = "<!-- grows -->"
)

var (
	// stepHeading is a step's heading in a part of the tutorial, such as
	// "Step 2. Run it".
	stepHeading = regexp.MustCompile(`^Step (\d+)\. \S`)

	// codeSpan is a code span in a line of Markdown.
	codeSpan = regexp.MustCompile("`([^`]+)`")

	// packageClause starts the code of an example. The comment above it is
	// the example's own, and no page shows it.
	packageClause = regexp.MustCompile(`(?m)^package main$`)
)

// TestCode checks that every go block of the pages in codePages is in one of
// the page's example files: the same lines, next to each other, in the same
// order. Indentation, the spacing in a line, empty lines, and the files'
// //nolint comments do not count.
//
// A page can loosen the check in two ways, both visible in its source. A
// line "// ..." in a block stands for lines that the block leaves out. And
// "<!-- grows -->" above a block says that a later step adds lines inside
// it: the block's lines must then be in the file in order, with any lines
// between them.
func TestCode(t *testing.T) {
	t.Parallel()
	repo := os.DirFS("../..")
	pages, err := fs.Glob(repo, "docs/tutorial/*.md")
	if err != nil || len(pages) == 0 {
		t.Fatalf("docs/tutorial has no pages: %v", err)
	}
	for page := range codePages {
		if !slices.Contains(pages, page) {
			pages = append(pages, page)
		}
	}
	slices.Sort(pages)

	files := make(map[string][]string) // the lines of each example file read so far
	for _, page := range pages {
		text, err := fs.ReadFile(repo, page)
		if err != nil {
			t.Error(err)
			continue
		}
		blocks, _ := goBlocks(string(text))
		examples, listed := codePages[page]
		if !listed && len(blocks) > 0 {
			t.Errorf("%s has go blocks, but codePages does not name its example", page)
			continue
		}
		for _, name := range examples {
			if _, read := files[name]; read {
				continue
			}
			src, err := fs.ReadFile(repo, name)
			if err != nil {
				t.Fatal(err)
			}
			files[name] = codeLines(string(src), true)
		}
		for _, b := range blocks {
			if why := b.missing(examples, files); why != "" {
				t.Errorf("%s:%d: the go block %s", page, b.line, why)
			}
		}
	}
}

// TestTutorialShowsAllCode checks the other direction of TestCode for the
// programs that the tutorial's reader writes: every line of them is on a
// page of the tutorial, in a go block or in a code span. So a reader who
// types what the pages show can end with the example.
func TestTutorialShowsAllCode(t *testing.T) {
	t.Parallel()
	repo := os.DirFS("../..")
	parts, err := fs.Glob(repo, "docs/tutorial/[0-9]*.md")
	if err != nil || len(parts) != 7 {
		t.Fatalf("docs/tutorial has %d parts, want 7: %v", len(parts), err)
	}
	for _, built := range builtPrograms {
		shown, err := shownBy(repo, parts[built.from-1:built.to])
		if err != nil {
			t.Fatal(err)
		}
		src, err := fs.ReadFile(repo, built.file)
		if err != nil {
			t.Fatal(err)
		}
		at := packageClause.FindIndex(src)
		if at == nil {
			t.Fatalf("%s has no package clause", built.file)
		}
		for _, line := range codeLines(string(src[at[0]:]), true) {
			if !shown[line] {
				t.Errorf("%s: parts %d to %d of the tutorial do not show the line %s", built.file, built.from, built.to, quote(line))
			}
		}
	}
}

// TestTutorialParts checks that every part of the tutorial has the shape of
// the template in docs/design/writing-docs.md: the title "Part N: …", steps
// numbered from 1, "What you learned", "Go deeper", and a Next line at the
// end. The tutorial's README must link to every part.
func TestTutorialParts(t *testing.T) {
	t.Parallel()
	repo := os.DirFS("../..")
	parts, err := fs.Glob(repo, "docs/tutorial/[0-9]*.md")
	if err != nil || len(parts) == 0 {
		t.Fatalf("docs/tutorial has no parts: %v", err)
	}
	index, err := fs.ReadFile(repo, "docs/tutorial/README.md")
	if err != nil {
		t.Fatal(err)
	}
	for i, part := range parts {
		text, err := fs.ReadFile(repo, part)
		if err != nil {
			t.Error(err)
			continue
		}
		if name := path.Base(part); !strings.Contains(string(index), "]("+name+")") {
			t.Errorf("docs/tutorial/README.md does not link to %s", name)
		}
		if why := partShape(path.Base(part), string(text), i+1); why != "" {
			t.Errorf("%s: %s", part, why)
		}
	}
}

// partShape returns how the nth part of the tutorial differs from the
// template, or "" if it has the template's shape.
func partShape(name, text string, n int) string {
	_, headings := parse(text)
	title := "Part " + strconv.Itoa(n) + ": "
	if !strings.HasPrefix(name, strconv.Itoa(n)+"-") || len(headings) == 0 || !strings.HasPrefix(headings[0], title) {
		return fmt.Sprintf("the file of part %d is named %d-….md, and its title starts with %q", n, n, title)
	}
	steps := 0
	for _, h := range headings[1:] {
		m := stepHeading.FindStringSubmatch(h)
		if m == nil {
			break
		}
		if steps++; m[1] != strconv.Itoa(steps) {
			return fmt.Sprintf("step %s comes where step %d belongs", m[1], steps)
		}
	}
	if rest := headings[steps+1:]; steps == 0 || !slices.Equal(rest, []string{"What you learned", "Go deeper"}) {
		return fmt.Sprintf(`after the title come the steps, then "What you learned" and "Go deeper"; after %d steps come %q`, steps, rest)
	}
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	if last := lines[len(lines)-1]; !strings.HasPrefix(last, "**Next:** [") {
		return fmt.Sprintf("the last line is %q, not a Next line", last)
	}
	return ""
}

// block is a fenced go block of a page.
type block struct {
	line  int      // where its opening fence is
	lines []string // its code, as codeLines returns it
	grows bool     // the grows comment is above it
}

// goBlocks returns the go blocks of a Markdown text, and its lines outside
// code blocks.
func goBlocks(text string) (blocks []block, prose []string) {
	var b *block // the go block that the line is in, if any
	open := ""   // the fence of the code block that the line is in, if any
	above := ""  // the last line of prose since the block before
	var code []string
	for i, line := range strings.Split(text, "\n") {
		mark, info := "", ""
		if m := fence.FindStringSubmatch(strings.TrimRight(line, "\r")); m != nil {
			mark, info = m[1], strings.TrimSpace(m[2])
		}
		switch {
		case mark != "" && open == "":
			open = mark
			if info == "go" {
				b, code = &block{line: i + 1, grows: above == grows}, nil
			}
		case mark != "" && mark[0] == open[0] && len(mark) >= len(open) && info == "":
			open, above = "", ""
			if b != nil {
				b.lines = codeLines(strings.Join(code, "\n"), false)
				blocks, b = append(blocks, *b), nil
			}
		case open != "":
			code = append(code, line)
		case strings.TrimSpace(line) != "":
			above = strings.TrimSpace(line)
			prose = append(prose, line)
		}
	}
	return blocks, prose
}

// shownBy returns the lines of code that the pages show: the lines of their
// go blocks, and their code spans.
func shownBy(repo fs.FS, pages []string) (map[string]bool, error) {
	shown := make(map[string]bool)
	for _, page := range pages {
		text, err := fs.ReadFile(repo, page)
		if err != nil {
			return nil, err
		}
		blocks, prose := goBlocks(string(text))
		for _, b := range blocks {
			for _, line := range b.lines {
				shown[line] = true
			}
		}
		for _, m := range codeSpan.FindAllStringSubmatch(strings.Join(prose, "\n"), -1) {
			shown[strings.Join(strings.Fields(m[1]), " ")] = true
		}
	}
	return shown, nil
}

// codeLines returns the lines of Go code that have text, each with one
// space between its words. For an example file, it cuts the comments that
// only the linter reads.
func codeLines(src string, example bool) []string {
	var lines []string
	for line := range strings.SplitSeq(src, "\n") {
		if example {
			line, _, _ = strings.Cut(line, "//nolint:")
		}
		if fields := strings.Fields(line); len(fields) > 0 {
			lines = append(lines, strings.Join(fields, " "))
		}
	}
	return lines
}

// missing returns why the block is in none of the example files, or "" if
// it is in one of them.
func (b block) missing(examples []string, files map[string][]string) string {
	// The runs of lines that must be next to each other: whole runs between
	// the omitted lines, or single lines for a block that grows.
	var whole, single [][]string
	start := true // the next line starts a run
	for _, line := range b.lines {
		switch {
		case line == omitted:
			start = true
			continue
		case start:
			whole, start = append(whole, []string{line}), false
		default:
			whole[len(whole)-1] = append(whole[len(whole)-1], line)
		}
		single = append(single, []string{line})
	}
	if len(whole) == 0 {
		return "has no code"
	}

	var why []string
	for _, name := range examples {
		runs := whole
		if b.grows {
			if findRuns(files[name], whole) == "" {
				return fmt.Sprintf("is in %s as it is: remove the %s above it", name, grows)
			}
			runs = single
		}
		reason := findRuns(files[name], runs)
		if reason == "" {
			return ""
		}
		why = append(why, "is not in "+name+": "+reason)
	}
	return strings.Join(why, "; and it ")
}

// findRuns looks for each run of lines in the lines of a file, after the run
// before it. It returns "" if all are there, or else what is missing.
func findRuns(lines []string, runs [][]string) string {
	from := 0
	for _, run := range runs {
		at, most := -1, 0 // where the run is, or else how many of its lines are somewhere
		for j := from; j < len(lines) && at < 0; j++ {
			n := 0
			for n < len(run) && j+n < len(lines) && lines[j+n] == run[n] {
				n++
			}
			if n == len(run) {
				at = j
			}
			most = max(most, n)
		}
		switch {
		case at >= 0:
			from = at + len(run)
		case most == 0:
			return "the file has no line " + quote(run[0]) + " there"
		default:
			return "after " + quote(run[most-1]) + ", the file has no line " + quote(run[most])
		}
	}
	return ""
}

// quote returns a line in quotes, cut to a length that fits a message.
func quote(line string) string {
	if r := []rune(line); len(r) > 70 {
		line = string(r[:70]) + "…"
	}
	return "`" + line + "`"
}
