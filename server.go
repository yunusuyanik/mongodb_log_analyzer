package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
)

//go:embed static/index.html
var indexHTML string

func serve(report *Report, port int) error {
	mux := http.NewServeMux()

	// Full report JSON – frontend fetches once and renders everything
	mux.HandleFunc("/api/report", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if err := json.NewEncoder(w).Encode(report); err != nil {
			http.Error(w, err.Error(), 500)
		}
	})

	// Serve the SPA
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, indexHTML)
	})

	addr := fmt.Sprintf(":%d", port)
	return http.ListenAndServe(addr, mux)
}
