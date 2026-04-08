package main

import (
	"io"
	"net/http"
	"strings"
	"time"
)

// proxyHandler creates an HTTP handler that proxies requests to the registry service.
// This avoids CORS issues since the admin UI and these endpoints share the same origin.
func proxyHandler(registryURL string) http.HandlerFunc {
	client := &http.Client{Timeout: 10 * time.Second}

	return func(w http.ResponseWriter, r *http.Request) {
		// Strip the /v1/proxy prefix and forward to registry
		targetPath := strings.TrimPrefix(r.URL.Path, "/v1/proxy")
		targetURL := registryURL + targetPath
		if r.URL.RawQuery != "" {
			targetURL += "?" + r.URL.RawQuery
		}

		proxyReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, r.Body)
		if err != nil {
			http.Error(w, "proxy error", http.StatusBadGateway)
			return
		}

		// Forward content-type
		if ct := r.Header.Get("Content-Type"); ct != "" {
			proxyReq.Header.Set("Content-Type", ct)
		}

		resp, err := client.Do(proxyReq)
		if err != nil {
			http.Error(w, "registry unreachable: "+err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		// Copy response headers
		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	}
}
