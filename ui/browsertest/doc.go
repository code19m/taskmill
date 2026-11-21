// Package browsertest runs the dashboard's script, ui/static/app.js, in
// Chrome, on the pages that the dashboard serves from a test database: how a
// live update changes a page, the navigation and the back and forward
// buttons, the dialog that confirms an action, the tooltip, and the words
// that the script shares with the server. Its tests are all it holds.
//
// It is a module of its own, so that the core's dependencies do not change
// (docs/design/dependencies.md). Run the tests with `make test-browser`,
// which finds Chrome.
package browsertest
