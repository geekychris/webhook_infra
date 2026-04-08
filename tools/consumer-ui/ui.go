package main

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/index.html
var staticFiles embed.FS

// UIHandler returns an http.Handler that serves the embedded consumer UI.
func UIHandler() http.Handler {
	sub, _ := fs.Sub(staticFiles, "static")
	return http.FileServer(http.FS(sub))
}
