package docs

import (
	"cmp"
	"fmt"
	"go/ast"
	"go/doc"
	"go/parser"
	"go/token"
	"io/fs"
	"path"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
)

// The tests in this file check the doc comments, which are the reference of
// the API, and the links between them and the pages. They read the packages
// from their source, so none of them uses the network.

// godocGuide is the page with the rules for doc comments. Its section "What
// the tests check" names the tests of this file: change a rule there and
// here together.
const godocGuide = "docs/design/writing-godoc.md"

// module is the module's import path. Every link to its API on pkg.go.dev
// starts with it.
const module = "github.com/code19m/taskmill"

// apiDirs are the directories of the packages that users import or run. The
// import path of each is the module's, followed by the directory.
var apiDirs = []string{".", "outbox", "ui", "cmd/taskmill-ui", "taskmilltest", "otel"}

// apiPackage is what pkg.go.dev shows of one package.
type apiPackage struct {
	dir      string             // its directory in the repository
	doc      string             // its package comment
	docFile  string             // the file with the package comment
	names    map[string]bool    // what a link can name after "#": "Client", "Client.Enqueue", "EnqueueOptions.Queue", "example-Client.Cancel"
	structs  map[string][]field // its exported structs, each with its exported fields
	comments []comment          // the comments of its files, line by line
}

// field is an exported field of a struct.
type field struct {
	name    string
	comment string // its doc comment and its line comment
	isBool  bool
	file    string // where it is declared
	line    int
}

// comment is one line of a comment in a Go file.
type comment struct {
	file string
	line int
	text string
}

// importPath returns the import path of the package in dir.
func importPath(dir string) string {
	if dir == "." {
		return module
	}
	return module + "/" + dir
}

// qualified returns the name of an identifier of p as the pages write it:
// "Schedule" in the main package, and "outbox.Message" in another.
func (p *apiPackage) qualified(name string) string {
	if p.dir == "." {
		return name
	}
	return path.Base(p.dir) + "." + name
}

// parsed holds the packages of apiDirs, which the tests parse once.
var parsed struct {
	once     sync.Once
	packages []*apiPackage
	err      error
}

// apiPackages returns the packages of apiDirs, in that order.
func apiPackages(t *testing.T) []*apiPackage {
	t.Helper()
	parsed.once.Do(func() {
		for _, dir := range apiDirs {
			p, err := parsePackage(root, dir)
			if err != nil {
				parsed.err = fmt.Errorf("%s: %w", dir, err)
				return
			}
			parsed.packages = append(parsed.packages, p)
		}
	})
	if parsed.err != nil {
		t.Fatalf("cannot read the packages: is the test run from internal/docs? %v", parsed.err)
	}
	return parsed.packages
}

// parsePackage reads the package in dir of repo: its Go files, with their
// tests, for the examples.
func parsePackage(repo fs.FS, dir string) (*apiPackage, error) {
	names, err := fs.Glob(repo, path.Join(dir, "*.go"))
	if err != nil {
		return nil, err
	}
	p := &apiPackage{dir: dir, names: make(map[string]bool), structs: make(map[string][]field)}
	fset := token.NewFileSet()
	files := make([]*ast.File, 0, len(names))
	for _, name := range names {
		src, err := fs.ReadFile(repo, name)
		if err != nil {
			return nil, err
		}
		f, err := parser.ParseFile(fset, name, src, parser.ParseComments)
		if err != nil {
			return nil, err
		}
		files = append(files, f)
		p.readComments(fset, name, f)
	}
	// NewFromFiles takes the comments out of the files: they are read above.
	d, err := doc.NewFromFiles(fset, files, importPath(dir))
	if err != nil {
		return nil, err
	}
	p.doc = d.Doc
	p.readNames(fset, d)
	return p, nil
}

// readComments notes the comments of the file name, and whether it holds
// the package comment.
func (p *apiPackage) readComments(fset *token.FileSet, name string, f *ast.File) {
	if f.Doc != nil && !strings.HasSuffix(name, "_test.go") {
		p.docFile = name
	}
	for _, group := range f.Comments {
		for _, c := range group.List {
			p.comments = append(p.comments, comment{file: name, line: fset.Position(c.Pos()).Line, text: c.Text})
		}
	}
}

// readNames notes what d exports: its names, its structs, and its examples.
func (p *apiPackage) readNames(fset *token.FileSet, d *doc.Package) {
	p.addExamples("", d.Examples)
	p.addValues(d.Consts, d.Vars)
	p.addFuncs("", d.Funcs)
	for _, t := range d.Types {
		p.names[t.Name] = true
		p.addExamples(t.Name, t.Examples)
		p.addValues(t.Consts, t.Vars)
		p.addFuncs("", t.Funcs)
		p.addFuncs(t.Name+".", t.Methods)
		for _, spec := range t.Decl.Specs {
			if ts, ok := spec.(*ast.TypeSpec); ok {
				p.addMembers(fset, t.Name, ts.Type)
			}
		}
	}
}

func (p *apiPackage) addValues(groups ...[]*doc.Value) {
	for _, values := range groups {
		for _, v := range values {
			for _, name := range v.Names {
				p.names[name] = true
			}
		}
	}
}

// addFuncs notes functions, or with a prefix such as "Client." the methods
// of a type.
func (p *apiPackage) addFuncs(prefix string, funcs []*doc.Func) {
	for _, f := range funcs {
		p.names[prefix+f.Name] = true
		p.addExamples(prefix+f.Name, f.Examples)
	}
}

// addExamples notes the examples of the identifier id, or of the package if
// id is empty, as pkg.go.dev names their anchors: "example-Client.Enqueue"
// for ExampleClient_Enqueue, and "example-Client.Enqueue-Deduplicated" for
// ExampleClient_Enqueue_deduplicated.
func (p *apiPackage) addExamples(id string, examples []*doc.Example) {
	for _, ex := range examples {
		name := "example-" + cmp.Or(id, "package")
		if ex.Suffix != "" {
			name += "-" + strings.ToUpper(ex.Suffix[:1]) + ex.Suffix[1:]
		}
		p.names[name] = true
	}
}

// addMembers notes the exported fields of a struct type, or the exported
// methods of an interface type.
func (p *apiPackage) addMembers(fset *token.FileSet, typeName string, expr ast.Expr) {
	var members *ast.FieldList
	inStruct := false
	switch t := expr.(type) {
	case *ast.StructType:
		members, inStruct = t.Fields, true
		p.structs[typeName] = nil // it may export no field
	case *ast.InterfaceType:
		members = t.Methods
	default:
		return
	}
	for _, m := range members.List {
		for _, name := range m.Names {
			if !name.IsExported() {
				continue
			}
			p.names[typeName+"."+name.Name] = true
			if !inStruct {
				continue
			}
			at := fset.Position(name.Pos())
			kind, _ := m.Type.(*ast.Ident)
			p.structs[typeName] = append(p.structs[typeName], field{
				name:    name.Name,
				comment: m.Doc.Text() + m.Comment.Text(),
				isBool:  kind != nil && kind.Name == "bool",
				file:    at.Filename,
				line:    at.Line,
			})
		}
	}
}

// A config struct is one that the caller fills in. Its name ends as
// configSuffix says, or it is one of moreConfigs, by its package's
// directory.
var (
	configSuffix = regexp.MustCompile(`(?:Config|Options|Params|Settings)$`)
	moreConfigs  = map[string][]string{
		".":      {"ExponentialBackoff", "Schedule", "TaskFilter"},
		"outbox": {"Message"},
		"ui":     {"BasicAuth"},
	}
)

// configs returns the names of p's config structs, sorted.
func (p *apiPackage) configs() []string {
	var names []string
	for name := range p.structs {
		if configSuffix.MatchString(name) || slices.Contains(moreConfigs[p.dir], name) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// namesDefault is how the comment of a config field says what the caller
// gets without setting the field.
var namesDefault = regexp.MustCompile(`\bDefault: |\bRequired\b|\b(?:Zero|Empty)(?: or less)? means\b|\bmay be nil\b`)

// TestFieldDefaults checks that every field of a config struct has a
// comment that names its default: the comment is where a reader looks it
// up. A bool needs none: false means off.
func TestFieldDefaults(t *testing.T) {
	t.Parallel()
	for _, p := range apiPackages(t) {
		for _, name := range p.configs() {
			for _, f := range p.structs[name] {
				switch text := strings.Join(strings.Fields(f.comment), " "); {
				case text == "":
					t.Errorf(`%s:%d: %s.%s has no comment. %s makes the comment the home of the field's default and limits: `+
						`write what the field does, then "Default: <value>."`, f.file, f.line, p.qualified(name), f.name, godocGuide)
				case !f.isBool && !namesDefault.MatchString(text):
					t.Errorf(`%s:%d: the comment of %s.%s names no default, and %s makes it the home of the default. `+
						`Write "Default: <value>.", "Required.", or what the zero value means, such as "Empty means all queues."`,
						f.file, f.line, p.qualified(name), f.name, godocGuide)
				}
			}
		}
	}
}

// repoLink is a link from a Go comment to a file of the repository, as
// GitHub shows it on the main branch, with the path and the anchor.
var repoLink = regexp.MustCompile(`https://github\.com/code19m/taskmill/(?:blob|tree)/main/([^\s#)\]]+)(#[^\s)\]]*)?`)

// TestGodocLinks checks the links from the comments to the repository: the
// file must exist, and the link must name no heading. A released comment
// cannot follow a heading that changes. It also checks that every package
// comment leads to a page of the docs, where the reader learns the "how".
func TestGodocLinks(t *testing.T) {
	t.Parallel()
	for _, p := range apiPackages(t) {
		teaches := false
		for _, m := range repoLink.FindAllStringSubmatch(p.doc, -1) {
			teaches = teaches || strings.HasPrefix(m[1], "docs/")
		}
		if !teaches {
			t.Errorf("%s:1: the package comment of %s links to no page of docs/. A package comment says where to read more: "+
				"end it with a link such as https://github.com/code19m/taskmill/blob/main/docs/guides/setup.md",
				cmp.Or(p.docFile, p.dir), importPath(p.dir))
		}
		for _, c := range p.comments {
			for _, m := range repoLink.FindAllStringSubmatch(c.text, -1) {
				target, anchor := strings.TrimRight(m[1], ".,;:"), strings.TrimRight(m[2], ".,;:")
				switch _, err := fs.Stat(root, target); {
				case err != nil:
					t.Errorf("%s:%d: the comment links to %s, which is not in the repository. Write the path of a file that exists, such as docs/guides/enqueue.md",
						c.file, c.line, target)
				case anchor != "":
					t.Errorf("%s:%d: the comment links to the heading %s of %s. %s wants a link to the page, never to an anchor, which moves: remove %q",
						c.file, c.line, anchor, target, godocGuide, anchor)
				}
			}
		}
	}
}

// pkgLink is a link to one of the module's packages on pkg.go.dev, with the
// package's path below the module, and the name after the "#".
var pkgLink = regexp.MustCompile(`https://pkg\.go\.dev/` + regexp.QuoteMeta(module) + `((?:/[a-z][a-z0-9-]*)*)(?:#([A-Za-z0-9_.-]*[A-Za-z0-9_]))?`)

// TestPkgGoDevLinks checks every link from a page to the API on pkg.go.dev
// against the source: the package must be one of apiDirs, and the name after
// the "#" must be one that the package exports, a field or a method of such
// a name, or one of its examples. So a renamed identifier cannot leave a
// link that leads nowhere. Anchors of pkg.go.dev's own sections, such as
// #pkg-variables, are not checked.
func TestPkgGoDevLinks(t *testing.T) {
	t.Parallel()
	byDir := make(map[string]*apiPackage)
	for _, p := range apiPackages(t) {
		byDir[p.dir] = p
	}
	for _, page := range boundPages(t) {
		for _, m := range pkgLink.FindAllStringSubmatchIndex(page.prose, -1) {
			link, line := page.prose[m[0]:m[1]], 1+strings.Count(page.prose[:m[0]], "\n")
			dir := cmp.Or(strings.TrimPrefix(page.prose[m[2]:m[3]], "/"), ".")
			p, ok := byDir[dir]
			if !ok {
				t.Errorf("%s:%d: the link %s names the package %s, which is not one of the module's: %s. Write one of them",
					page.name, line, link, dir, strings.Join(apiDirs[1:], ", "))
				continue
			}
			if m[4] < 0 {
				continue // a link to the package
			}
			name := page.prose[m[4]:m[5]]
			if strings.HasPrefix(name, "pkg-") || strings.HasPrefix(name, "section-") || p.names[name] {
				continue
			}
			t.Errorf("%s:%d: the link %s names %s, but %s has no such identifier, field, method, or example. "+
				"Write the name as the source has it, such as #Client.Enqueue or #example-Client.Enqueue, or link to the package alone",
				page.name, line, link, name, importPath(dir))
		}
	}
}
