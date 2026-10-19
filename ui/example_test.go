package ui_test

import (
	"log"
	"net/http"
	"time"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/ui"
)

func requireAdmin(next http.Handler) http.Handler { return next } // your authentication

// This example mounts the web interface under /taskmill, behind the
// application's own authentication.
func ExampleHandler() {
	var client *taskmill.Client // from taskmill.NewClient
	mux := http.NewServeMux()
	mux.Handle("/taskmill/", requireAdmin(ui.Handler(client, &ui.Config{Prefix: "/taskmill"})))
	server := &http.Server{Addr: "localhost:8080", Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(server.ListenAndServe())
}

// This example serves the web interface as a dashboard of its own, with a
// password, and lets the company's admin panel show it in a frame.
func ExampleHandler_dashboard() {
	var client *taskmill.Client // from taskmill.NewClient
	handler := ui.Handler(client, &ui.Config{
		BasicAuth:      &ui.BasicAuth{Username: "ops", Password: "a long random password"},
		FrameAncestors: []string{"https://admin.example.com"},
	})
	server := &http.Server{Addr: ":8443", Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(server.ListenAndServeTLS("cert.pem", "key.pem"))
}
