package docs

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"maps"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

// The tests in this file check that the reference pages name what the code
// logs and measures, and nothing else: operators search the log page for a
// line they see, and build alerts on the exact names of metrics.

const (
	// logPage lists every message that taskmill logs.
	logPage = "docs/reference/log-messages.md"

	// metricsPage lists every metric and attribute of the otel module.
	metricsPage = "docs/reference/metrics.md"
)

// wholeCodeSpan is a cell that is one code span, and nothing else.
var wholeCodeSpan = regexp.MustCompile("^`([^`]+)`$")

// TestLogMessages checks that the log page has a row for every message
// that the packages log, and for every operation whose failures they log,
// and that it lists no other.
func TestLogMessages(t *testing.T) {
	t.Parallel()
	logged, operations := map[string]string{}, map[string]string{} // message or operation -> where in the code
	for _, dir := range apiDirs {
		scanLogs(t, dir, logged, operations)
	}

	text := read(t, logPage)
	prose, _ := parse(text)
	listed := map[string]map[string]int{"Message": {}, "Operation": {}} // the first column's header -> its cells, with their lines
	for _, tb := range tablesOf(page{name: logPage, text: text, prose: prose}) {
		cells, ok := listed[tb.header[0]]
		if !ok {
			continue
		}
		for i, row := range tb.rows {
			if m := wholeCodeSpan.FindStringSubmatch(row[0]); m != nil {
				cells[m[1]] = tb.line + 2 + i
			}
		}
	}

	compareNames(t, logPage, "Message", logged, listed["Message"],
		"Add a row to the table of the part that logs it: the message, its level, what it means, and what to do")
	compareNames(t, logPage, "Operation", operations, listed["Operation"],
		"Add a row to the table of operations: the operation, what it does, how often, and what to check")
}

// TestMetricNames checks that the metrics page names every metric and
// attribute of taskmill's own that the otel module reports, and every
// metric that it takes from OpenTelemetry's conventions, and no other.
func TestMetricNames(t *testing.T) {
	t.Parallel()
	ours, conventional := map[string]string{}, map[string]string{} // name -> where in the code
	scanMetrics(t, "otel", ours, conventional)

	text := read(t, metricsPage)
	prose, _ := parse(text)
	named := map[string]int{} // taskmill's names, in any code span, with their lines
	for i, line := range strings.Split(text, "\n") {
		for _, m := range taskmillName.FindAllStringSubmatch(line, -1) {
			named[m[1]] = i + 1
		}
	}
	metrics := map[string]int{} // the names in the first column of the tables of metrics
	for _, tb := range tablesOf(page{name: metricsPage, text: text, prose: prose}) {
		if tb.header[0] != "Metric" {
			continue
		}
		for i, row := range tb.rows {
			if name, _, _ := strings.Cut(row[0], "<br/>"); strings.HasPrefix(name, "`messaging.") {
				metrics[strings.Trim(name, "`")] = tb.line + 2 + i
			}
		}
	}

	compareNames(t, metricsPage, "taskmill name", ours, named,
		"Add it to the table of its spans, metrics, or gauges, or of their attributes")
	compareNames(t, metricsPage, "conventional metric", conventional, metrics,
		"Add a row to the table of metrics: its two names, its type, its unit, and what it measures")
}

// compareNames reports the names of code that page does not list, and the
// names that it lists and code does not have.
func compareNames(t *testing.T, page, what string, code map[string]string, listed map[string]int, fix string) {
	t.Helper()
	if len(code) == 0 || len(listed) == 0 {
		t.Fatalf("found %d of %s in the code and %d on %s: has the way to write or to list them changed?",
			len(code), what, len(listed), page)
	}
	for _, name := range slices.Sorted(maps.Keys(code)) {
		if _, ok := listed[name]; !ok {
			t.Errorf("%s: no row for the %s %q, of %s. %s", page, what, name, code[name], fix)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(listed)) {
		if _, ok := code[name]; !ok {
			t.Errorf("%s:%d: the %s %q is not in the code. Remove it, or write it as the code has it", page, listed[name], what, name)
		}
	}
}

// logIndex is the position of the message among the arguments of each
// method of slog.Logger that logs.
var logIndex = map[string]int{
	"Log": 2, "LogAttrs": 2,
	"Debug": 0, "Info": 0, "Warn": 0, "Error": 0,
	"DebugContext": 1, "InfoContext": 1, "WarnContext": 1, "ErrorContext": 1,
}

// incidentOperation is the position of the operation among the arguments of
// newIncidentLog, which logs the failures of an operation that repeats.
const incidentOperation = 1

// scanLogs adds the messages that the package in dir logs to logged, and
// the operations whose failures it logs to operations, each with where it
// is logged. A message must be a constant: a linter makes it one.
func scanLogs(t *testing.T, dir string, logged, operations map[string]string) {
	t.Helper()
	fset, files, consts := parseSource(t, dir)
	for _, f := range files {
		packages := importNames(f)
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}

			add, at := logged, messageAt(call, packages)
			if at < 0 {
				add, at = operations, operationAt(call)
			}
			if at < 0 || at >= len(call.Args) {
				return true
			}

			where := fset.Position(call.Pos()).String()
			text, ok := constString(call.Args[at], consts)
			if !ok {
				t.Errorf("%s: the message is not a constant string, so this test cannot check that %s lists it", where, logPage)
				return true
			}
			add[text] = where
			return true
		})
	}
}

// messageAt returns the position of the message among the arguments of
// call, if it calls a logger's method, and else -1. A function of an
// imported package, such as http.Error, is not a logger's method.
func messageAt(call *ast.CallExpr, packages map[string]bool) int {
	fn, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return -1
	}
	if x, ok := fn.X.(*ast.Ident); ok && packages[x.Name] {
		return -1
	}
	if i, ok := logIndex[fn.Sel.Name]; ok {
		return i
	}
	return -1
}

// operationAt returns the position of the operation among the arguments of
// call, if it calls newIncidentLog, and else -1.
func operationAt(call *ast.CallExpr) int {
	if fn, ok := call.Fun.(*ast.Ident); ok && fn.Name == "newIncidentLog" {
		return incidentOperation
	}
	return -1
}

// taskmillName is a name of taskmill's own in a code span: of a metric, a
// span's attribute, or a metric's attribute, such as taskmill.task.kind.
var taskmillName = regexp.MustCompile("`(taskmill\\.[a-z_.]*[a-z_])`")

// conventional is a function of OpenTelemetry's messaging conventions that
// creates one of their metrics: messagingconv.NewProcessDuration creates
// messaging.process.duration.
var conventional = regexp.MustCompile(`^New([A-Z][A-Za-z]+)$`)

// scanMetrics adds the names of taskmill's own that the package in dir
// writes to ours, and the metrics of the messaging conventions that it
// creates to conventions, each with where it does.
func scanMetrics(t *testing.T, dir string, ours, conventions map[string]string) {
	t.Helper()
	fset, files, _ := parseSource(t, dir)
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.BasicLit:
				if s, err := strconv.Unquote(x.Value); err == nil && x.Kind == token.STRING && strings.HasPrefix(s, "taskmill.") {
					ours[s] = fset.Position(x.Pos()).String()
				}
			case *ast.SelectorExpr:
				pkg, ok := x.X.(*ast.Ident)
				if m := conventional.FindStringSubmatch(x.Sel.Name); ok && pkg.Name == "messagingconv" && m != nil {
					conventions["messaging."+dotted(m[1])] = fset.Position(x.Pos()).String()
				}
			}
			return true
		})
	}
}

// dotted writes a name in mixed caps as OpenTelemetry's conventions write
// it: ClientSentMessages is client.sent.messages.
func dotted(name string) string {
	var b strings.Builder
	for i, r := range name {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteByte('.')
			}
			r = unicode.ToLower(r)
		}
		b.WriteRune(r)
	}
	return b.String()
}

// parseSource parses the Go files of dir that are not tests, and returns
// them with the string constants that they declare.
func parseSource(t *testing.T, dir string) (*token.FileSet, []*ast.File, map[string]ast.Expr) {
	t.Helper()
	names, err := fs.Glob(root, path.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}

	fset := token.NewFileSet()
	var files []*ast.File
	consts := map[string]ast.Expr{}
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := fs.ReadFile(root, name)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
		addConsts(f, consts)
	}
	return fset, files, consts
}

// addConsts adds the constants that f declares to consts, by name.
func addConsts(f *ast.File, consts map[string]ast.Expr) {
	for _, d := range f.Decls {
		g, ok := d.(*ast.GenDecl)
		if !ok || g.Tok != token.CONST {
			continue
		}
		for _, spec := range g.Specs {
			v, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range v.Names {
				if i < len(v.Values) {
					consts[name.Name] = v.Values[i]
				}
			}
		}
	}
}

// importNames returns the names by which f refers to the packages it
// imports.
func importNames(f *ast.File) map[string]bool {
	names := map[string]bool{}
	for _, imp := range f.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		name := path.Base(p)
		if imp.Name != nil {
			name = imp.Name.Name
		}
		names[name] = true
	}
	return names
}

// constString returns the value of e, a string literal, a constant of
// consts, or a sum of them, and reports whether it is one.
func constString(e ast.Expr, consts map[string]ast.Expr) (string, bool) {
	switch x := e.(type) {
	case *ast.BasicLit:
		s, err := strconv.Unquote(x.Value)
		return s, err == nil && x.Kind == token.STRING
	case *ast.Ident:
		if v, ok := consts[x.Name]; ok {
			return constString(v, consts)
		}
	case *ast.BinaryExpr:
		a, aok := constString(x.X, consts)
		b, bok := constString(x.Y, consts)
		return a + b, aok && bok && x.Op == token.ADD
	case *ast.ParenExpr:
		return constString(x.X, consts)
	}
	return "", false
}
