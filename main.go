package main

import (
	"flag"
	"fmt"
	"log"
)

func main() {
	path := flag.String("path", ".", "directory path or glob pattern (e.g. logs/ or 'logs/mongo*.tar.gz')")
	// keep -dir as alias for backward compat
	dir  := flag.String("dir", "", "alias for -path (directory)")
	port := flag.Int("port", 8080, "web server port")
	k8s  := flag.Bool("k8s", false, "parse Kubernetes log format (lines wrapped as {\"file\":\"...\",\"log\":\"...\"})")
	flag.Parse()

	target := *path
	if *dir != "" {
		target = *dir
	}

	k8sMode = *k8s

	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  MongoDB Log Analyzer")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("  Pattern : %s\n\n", target)

	entries, err := parseFiles(target)
	if err != nil {
		log.Fatalf("parse error: %v", err)
	}

	if len(entries) == 0 {
		log.Fatal("no log entries found. Supported formats: .log  .log.N  .gz  .tar.gz  .tgz")
	}

	fmt.Printf("\n  Total entries parsed: %d\n", len(entries))
	fmt.Println("  Analyzing...")

	fileSizes := GetFileSizes(target)
	report := analyze(entries, fileSizes)

	fmt.Printf("  Slow queries found: %d unique patterns\n", len(report.SlowQueries))
	fmt.Printf("  Replication events: %d\n", len(report.ReplEvents))
	fmt.Println()
	fmt.Printf("  Dashboard  → http://localhost:%d\n", *port)
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	if err := serve(report, *port); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
