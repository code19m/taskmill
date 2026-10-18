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
