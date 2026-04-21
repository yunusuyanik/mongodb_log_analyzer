package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"
)

// LogEntry represents a single parsed MongoDB 4.4+ structured JSON log line.
type LogEntry struct {
	Timestamp      time.Time
	Severity       string // F, E, W, I, D1-D5
	Component      string // COMMAND, REPL, NETWORK, INDEX, STORAGE, etc.
	ID             int64
	Context        string
	Message        string
	FileName       string
	LineNum        int

	// attr fields – query performance
	DurationMillis      int64
	Namespace           string
	CommandType         string
	KeysExamined        int64
	DocsExamined        int64
	NReturned           int64
	QueryHash           string
	PlanSummary         string
	NumYields           int64
	ResLen              int64
	PlanningTimeMicros  int64
	RawCommand          string

	// attr fields – replication
	NewPrimary string
	ReplicaSet string
	Term       int64

	// attr fields – errors
	ErrMsg  string
	ErrCode int

	// attr fields – version / build info (msg: "Build Info")
	MongoVersion string
	GitVersion   string

	// attr fields – OS info (msg: "Operating System")
	OSName    string
	OSVersion string
	OSArch    string

	// attr fields – driver / client metadata (msg: "client metadata")
	DriverName     string
	DriverVersion  string
	AppName        string
	RemoteAddr     string

	// attr fields – sharded cluster (mongos)
	NShards            int64
	NMatched           int64
	NModified          int64
	RemoteOpWaitMillis int64

	// attr fields – connection tracking (NETWORK component)
	ConnectionID    int64
	ConnectionCount int64

	// attr fields – replica set config members (msg: "New replica set config in use")
	ReplMembers string // raw JSON array of member hosts

	// attr fields – member state heartbeat (msg: "Member is in new state")
	MemberHost  string // attr.hostAndPort
	MemberState string // attr.newState

	// attr fields – authentication (ACCESS component, msg: "Successfully authenticated")
	AuthUser        string
	AuthMechanism   string
	AuthDB          string
	AuthIsCluster   bool

	RawLine string // the original unparsed log line
}

// ─── File resolution ──────────────────────────────────────────────────────────

// parseFiles is the main entry point. pattern can be:
//   - an existing directory  → scans for all supported log files inside
//   - a glob pattern         → e.g. "logs/mongo*.tar.gz" or "logs/*.log"
//   - a plain file path      → single file
func parseFiles(pattern string) ([]LogEntry, error) {
	files, err := resolveFiles(pattern)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no files matched: %s", pattern)
	}
	return parseConcurrent(files)
}

// GetFileSizes returns a map from display name to file size in bytes.
func GetFileSizes(pattern string) map[string]int64 {
	files, err := resolveFiles(pattern)
	if err != nil {
		return map[string]int64{}
	}
	sizes := make(map[string]int64, len(files))
	for _, path := range files {
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		base := filepath.Base(path)
		lower := strings.ToLower(base)
		displayName := base
		if strings.HasSuffix(lower, ".tar.gz") || strings.HasSuffix(lower, ".tgz") {
			// tar archive: size is the archive itself; keyed by archive basename
		} else if strings.HasSuffix(lower, ".gz") {
			displayName = strings.TrimSuffix(base, ".gz")
		}
		sizes[displayName] = info.Size()
	}
	return sizes
}

func resolveFiles(pattern string) ([]string, error) {
	// Existing directory → scan it
	if info, err := os.Stat(pattern); err == nil && info.IsDir() {
		return scanDir(pattern)
	}
	// Glob expansion (handles *, ?, [...] )
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid glob pattern %q: %w", pattern, err)
	}
	var files []string
	for _, m := range matches {
		info, err := os.Stat(m)
		if err != nil || info.IsDir() {
			continue
		}
		if isSupportedFile(m) {
			files = append(files, m)
		}
	}
	return files, nil
}

func scanDir(dir string) ([]string, error) {
	patterns := []string{
		"*.log",
		"*.log.*",  // rotated: mongod.log.1, mongod.log.2024-11-01T…
		"*.log*",   // no-extension variants: server.log17thMarch
		"*.gz",     // single-file gzip
		"*.tar.gz",
		"*.tgz",
	}
	var files []string
	seen := map[string]bool{}
	for _, pat := range patterns {
		matches, _ := filepath.Glob(filepath.Join(dir, pat))
		for _, m := range matches {
			if !seen[m] && isSupportedFile(m) {
				seen[m] = true
				files = append(files, m)
			}
		}
	}
	return files, nil
}

func isSupportedFile(path string) bool {
	base := strings.ToLower(filepath.Base(path))
	// Skip split archive parts — they are incomplete and cannot be parsed standalone
	if strings.Contains(base, "_part_") {
		return false
	}
	return strings.HasSuffix(base, ".log") ||
		strings.Contains(base, ".log") ||
		strings.HasSuffix(base, ".gz") ||
		strings.HasSuffix(base, ".tgz") ||
		strings.HasSuffix(base, ".tar.gz")
}

// ─── Concurrent dispatch ──────────────────────────────────────────────────────

func parseConcurrent(files []string) ([]LogEntry, error) {
	type result struct {
		entries []LogEntry
		file    string
		err     error
	}

	ch := make(chan result, len(files))
	var wg sync.WaitGroup

	for _, f := range files {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			entries, err := parseFile(path)
			ch <- result{entries: entries, file: path, err: err}
		}(f)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	var all []LogEntry
	for r := range ch {
		if r.err != nil {
			fmt.Fprintf(os.Stderr, "  ⚠  warning: %s: %v\n", filepath.Base(r.file), r.err)
			continue
		}
		fmt.Printf("  ✓  %-44s %d entries\n", filepath.Base(r.file), len(r.entries))
		all = append(all, r.entries...)
	}
	return all, nil
}

// ─── Format dispatchers ───────────────────────────────────────────────────────

func parseFile(path string) ([]LogEntry, error) {
	base := strings.ToLower(filepath.Base(path))
	switch {
	case strings.HasSuffix(base, ".tar.gz"), strings.HasSuffix(base, ".tgz"):
		return parseTarGz(path)
	case strings.HasSuffix(base, ".gz"):
		return parseGz(path)
	default:
		return parsePlain(path)
	}
}

// parsePlain reads a raw (uncompressed) log file line by line.
func parsePlain(path string) ([]LogEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseReader(f, filepath.Base(path))
}

// parseGz reads a single gzip-compressed log file.
func parseGz(path string) ([]LogEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("gzip open: %w", err)
	}
	defer gr.Close()

	// Display name: strip .gz suffix so the file table shows the inner name
	name := strings.TrimSuffix(filepath.Base(path), ".gz")
	return parseReader(gr, name)
}

// parseTarGz iterates over entries in a .tar.gz archive and parses any
// regular file whose name looks like a MongoDB log (or every regular file
// if none match the naming heuristic – content-based detection via '{').
func parseTarGz(path string) ([]LogEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("gzip open: %w", err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	var all []LogEntry

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("tar read: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}

		name := filepath.Base(hdr.Name)

		// Skip known binary / non-log files inside archives
		if shouldSkipTarEntry(name) {
			continue
		}

		// Parse this tar entry directly (tr acts as an io.Reader for the current entry)
		entries, err := parseReader(tr, name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  ⚠  warning: %s!%s: %v\n", filepath.Base(path), name, err)
			continue
		}
		if len(entries) > 0 {
			all = append(all, entries...)
		}
	}

	return all, nil
}


func shouldSkipTarEntry(name string) bool {
	lower := strings.ToLower(name)
	for _, ext := range []string{".zip", ".png", ".jpg", ".pdf", ".bin", ".dat"} {
		if strings.HasSuffix(lower, ext) {
			return true
		}
	}
	return false
}

// ─── Core reader ──────────────────────────────────────────────────────────────

// k8sMode controls whether the parser unwraps Kubernetes log envelopes.
// Set this before calling parseFiles. It is safe to read from concurrent goroutines
// because it is written exactly once in main() before any goroutine is started.
var k8sMode bool

// parseReader is the shared parsing kernel. It reads lines from any io.Reader
// and parses each JSON log line. displayName is used in LogEntry.FileName.
func parseReader(r io.Reader, displayName string) ([]LogEntry, error) {
	scanner := bufio.NewScanner(r)
	// 4 MB line buffer – handles verbose command/plan documents
	scanner.Buffer(make([]byte, 4*1024*1024), 4*1024*1024)

	var entries []LogEntry
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if len(line) == 0 || line[0] != '{' {
			continue
		}
		if k8sMode {
			// K8s log envelope: {"file":"...","log":"{...mongodb json...}"}
			// Unwrap the inner log string before parsing.
			inner := gjson.Get(line, "log").String()
			if inner == "" {
				continue
			}
			line = inner
		}
		if entry, ok := parseLine(line, displayName, lineNum); ok {
			entries = append(entries, entry)
		}
	}
	return entries, scanner.Err()
}

// ─── Line parser ──────────────────────────────────────────────────────────────

func parseLine(line, fileName string, lineNum int) (LogEntry, bool) {
	if !gjson.Valid(line) {
		return LogEntry{}, false
	}

	r := gjson.ParseBytes([]byte(line))

	entry := LogEntry{
		RawLine:   line,
		Severity:  r.Get("s").String(),
		Component: r.Get("c").String(),
		ID:        r.Get("id").Int(),
		Context:   r.Get("ctx").String(),
		Message:   r.Get("msg").String(),
		FileName:  fileName,
		LineNum:   lineNum,
	}

	// MongoDB timestamp: {"t":{"$date":"2024-01-15T10:30:45.123+00:00"}}
	dateStr := r.Get(`t.$date`).String()
	if dateStr != "" {
		if t, err := time.Parse(time.RFC3339Nano, dateStr); err == nil {
			entry.Timestamp = t
		} else if t, err := time.Parse("2006-01-02T15:04:05.999-0700", dateStr); err == nil {
			entry.Timestamp = t
		} else if t, err := time.Parse("2006-01-02T15:04:05.999Z0700", dateStr); err == nil {
			entry.Timestamp = t
		}
	}

	attr := r.Get("attr")
	if attr.Exists() {
		entry.DurationMillis = attr.Get("durationMillis").Int()
		entry.Namespace      = attr.Get("ns").String()
		entry.CommandType    = attr.Get("type").String()
		entry.KeysExamined   = attr.Get("keysExamined").Int()
		entry.DocsExamined   = attr.Get("docsExamined").Int()
		entry.NReturned      = attr.Get("nreturned").Int()
		entry.QueryHash      = attr.Get("queryHash").String()
		entry.PlanSummary    = attr.Get("planSummary").String()
		entry.NumYields            = attr.Get("numYields").Int()
		entry.ResLen               = attr.Get("reslen").Int()
		entry.PlanningTimeMicros   = attr.Get("planningTimeMicros").Int()

		if cmd := attr.Get("command"); cmd.Exists() {
			entry.RawCommand = cmd.Raw
			// When attr.type is "command", extract the real operation from the
			// first non-$-prefixed key of the command object (find, update, aggregate…)
			if entry.CommandType == "command" || entry.CommandType == "" {
				cmd.ForEach(func(k, _ gjson.Result) bool {
					if !strings.HasPrefix(k.String(), "$") {
						entry.CommandType = k.String()
						return false
					}
					return true
				})
			}
		}

		entry.NewPrimary = attr.Get("newPrimary").String()
		if entry.NewPrimary == "" {
			entry.NewPrimary = attr.Get("primary").String()
		}
		entry.ReplicaSet = attr.Get("replicaSet").String()
		if entry.ReplicaSet == "" {
			entry.ReplicaSet = attr.Get("set").String()
		}
		entry.Term = attr.Get("term").Int()

		entry.ErrMsg = attr.Get("error").String()
		if entry.ErrMsg == "" {
			entry.ErrMsg = attr.Get("errorMessage").String()
		}
		entry.ErrCode = int(attr.Get("code").Int())

		// sharded cluster fields (mongos)
		entry.NShards            = attr.Get("nShards").Int()
		entry.NMatched           = attr.Get("nMatched").Int()
		entry.NModified          = attr.Get("nModified").Int()
		entry.RemoteOpWaitMillis = attr.Get("remoteOpWaitMillis").Int()
		// for updates, nreturned may be 0 — use nMatched as fallback
		if entry.NReturned == 0 && entry.NMatched > 0 {
			entry.NReturned = entry.NMatched
		}

		// connection fields (NETWORK component)
		entry.RemoteAddr      = attr.Get("remote").String()
		entry.ConnectionID    = attr.Get("connectionId").Int()
		entry.ConnectionCount = attr.Get("connectionCount").Int()
	}

	// ── message-specific extra fields ─────────────────────────────────────
	switch entry.Message {
	case "Build Info":
		bi := r.Get("attr.buildInfo")
		entry.MongoVersion = bi.Get("version").String()
		entry.GitVersion   = bi.Get("gitVersion").String()
	case "Operating System":
		os := r.Get("attr.os")
		entry.OSName    = os.Get("name").String()
		entry.OSVersion = os.Get("version").String()
		entry.OSArch    = os.Get("arch").String()
	case "client metadata":
		// MongoDB logs client metadata under attr.doc.* (most versions)
		// Fall back to attr.client.* for older formats
		entry.DriverName = r.Get("attr.doc.driver.name").String()
		if entry.DriverName == "" {
			entry.DriverName = r.Get("attr.client.driver.name").String()
		}
		entry.DriverVersion = r.Get("attr.doc.driver.version").String()
		if entry.DriverVersion == "" {
			entry.DriverVersion = r.Get("attr.client.driver.version").String()
		}
		if entry.AppName == "" {
			entry.AppName = r.Get("attr.doc.application.name").String()
			if entry.AppName == "" {
				entry.AppName = r.Get("attr.client.application.name").String()
			}
		}
	case "New replica set config in use", "Replica set reconfig done":
		if members := r.Get("attr.config.members"); members.IsArray() {
			entry.ReplMembers = members.Raw
		}
	case "Member is in new state":
		entry.MemberHost  = r.Get("attr.hostAndPort").String()
		entry.MemberState = r.Get("attr.newState").String()
		// Expose as NewPrimary so the frontend lastPrimary scan picks it up
		if entry.MemberState == "PRIMARY" {
			entry.NewPrimary = entry.MemberHost
		}
	case "Successfully authenticated":
		entry.AuthUser      = r.Get("attr.user").String()
		entry.AuthMechanism = r.Get("attr.mechanism").String()
		entry.AuthDB        = r.Get("attr.db").String()
		entry.AuthIsCluster = r.Get("attr.isClusterMember").Bool()
	}
	// Also catch "db version" style legacy startup line
	if entry.MongoVersion == "" && entry.Message == "MongoDB starting" {
		entry.MongoVersion = r.Get("attr.version").String()
	}

	return entry, true
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func containsStr(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
