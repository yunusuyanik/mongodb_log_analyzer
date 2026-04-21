package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// ─── Report ───────────────────────────────────────────────────────────────────

type Report struct {
	TotalEntries         int                `json:"totalEntries"`
	TimeRange            TimeRange          `json:"timeRange"`
	Files                []FileInfo         `json:"files"`
	Cluster              ClusterInfo        `json:"cluster"`
	SeverityCounts       map[string]int     `json:"severityCounts"`
	ComponentCounts      map[string]int     `json:"componentCounts"`
	SlowQueries          []SlowQuery        `json:"slowQueries"`
	ReplEvents           []ReplEvent        `json:"replEvents"`
	MessageGroups        []MessageGroup     `json:"messageGroups"`
	ErrorGroups          []ErrorGroup       `json:"errorGroups"`
	HourlyActivity       []HourlyBucket     `json:"hourlyActivity"`
	DurationDistribution []DurationBucket   `json:"durationDistribution"`
	SlowTimeSeries       []SlowTimePoint    `json:"slowTimeSeries"`
	SlowCommandTypes     []KV               `json:"slowCommandTypes"`
	SlowPlanCounts       []KV               `json:"slowPlanCounts"`
	SlowScatter          []SlowScatterPoint `json:"slowScatter"`
	ConnStats            ConnStats            `json:"connStats"`
	Timeline             []TimelineEvent      `json:"timeline"`
	MemberStateHistory   []MemberStateEvent   `json:"memberStateHistory"`
}

// SlowScatterPoint is a single slow query execution for the scatter chart.
// T is Unix milliseconds, D is durationMillis, NS is the namespace.
type SlowScatterPoint struct {
	T  int64  `json:"t"`
	D  int64  `json:"d"`
	NS string `json:"ns"`
}

// ClusterInfo holds MongoDB instance / cluster metadata parsed from startup log lines.
type ClusterInfo struct {
	MongoVersion string       `json:"mongoVersion"`
	GitVersion   string       `json:"gitVersion"`
	OSName       string       `json:"osName"`
	OSVersion    string       `json:"osVersion"`
	OSArch       string       `json:"osArch"`
	ReplicaSet   string       `json:"replicaSet"`
	Drivers      []DriverInfo `json:"drivers"`
	Members      []ReplMember `json:"members"`
}

type DriverInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	App     string `json:"app"`
}

type ReplMember struct {
	Host     string  `json:"host"`
	ID       int64   `json:"id"`
	Priority float64 `json:"priority"`
	Hidden   bool    `json:"hidden"`
	Arbiter  bool    `json:"arbiter"`
	State    string  `json:"state"` // last known state: PRIMARY, SECONDARY, RECOVERING, etc.
}

type TimeRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type FileInfo struct {
	Name       string    `json:"name"`
	EntryCount int       `json:"entryCount"`
	ErrorCount int       `json:"errorCount"`
	WarnCount  int       `json:"warnCount"`
	SlowCount  int       `json:"slowCount"`
	TimeRange  TimeRange `json:"timeRange"`
	Size       int64     `json:"size"`
}

type SlowQuery struct {
	QueryHash       string   `json:"queryHash"`
	Namespace       string   `json:"namespace"`
	CommandType     string   `json:"commandType"`
	PlanSummary     string   `json:"planSummary"`
	Count           int      `json:"count"`
	MaxDuration     int64    `json:"maxDuration"`
	MinDuration     int64    `json:"minDuration"`
	AvgDuration     float64  `json:"avgDuration"`
	TotalDuration   int64    `json:"totalDuration"`
	AvgDocsExamined float64  `json:"avgDocsExamined"`
	AvgKeysExamined float64  `json:"avgKeysExamined"`
	AvgNReturned    float64  `json:"avgNReturned"`
	Inefficiency    float64  `json:"inefficiency"`
	AvgRemoteWaitMs        float64  `json:"avgRemoteWaitMs"`
	AvgPlanningTimeMicros  float64  `json:"avgPlanningTimeMicros"`
	MaxRemoteWaitMs    int64    `json:"maxRemoteWaitMs"`
	RemoteWaitPct      float64  `json:"remoteWaitPct"`  // avg remote wait / avg duration * 100
	IsSharded          bool     `json:"isSharded"`
	ExampleCommand     string   `json:"exampleCommand"`
	LastRawLine        string   `json:"lastRawLine"`
	Files              []string `json:"files"`
}

// ErrorGroup groups log entries by error message + error code.
type ErrorGroup struct {
	ErrMsg        string    `json:"errMsg"`
	ErrCode       int       `json:"errCode"`
	Count         int       `json:"count"`
	Component     string    `json:"component"`
	Severity      string    `json:"severity"`
	Namespaces    []string  `json:"namespaces"`
	LastRawLine   string    `json:"lastRawLine"`
	LastTimestamp time.Time `json:"lastTimestamp"`
}

type ReplEvent struct {
	Timestamp  time.Time `json:"timestamp"`
	EventType  string    `json:"eventType"`
	Message    string    `json:"message"`
	Component  string    `json:"component"`
	NewPrimary string    `json:"newPrimary"`
	ReplicaSet string    `json:"replicaSet"`
	Term       int64     `json:"term"`
	Severity   string    `json:"severity"`
	FileName   string    `json:"fileName"`
	RawLine    string    `json:"rawLine"`
}

// MessageGroup groups log lines by their exact `msg` field value.
type MessageGroup struct {
	Message   string    `json:"message"`
	Count     int       `json:"count"`
	Component string    `json:"component"` // most common component
	Severity  string    `json:"severity"`  // most common severity
	FirstTs   time.Time `json:"firstTs"`
	LastTs    time.Time `json:"lastTs"`
}

type HourlyBucket struct {
	Hour   string `json:"hour"`
	Errors int    `json:"errors"`
	Warns  int    `json:"warns"`
	Slow   int    `json:"slow"`
	Total  int    `json:"total"`
}

// DurationBucket is one bar in the query duration histogram.
type DurationBucket struct {
	Label string `json:"label"`
	Min   int64  `json:"min"`
	Max   int64  `json:"max"` // -1 = unbounded
	Count int    `json:"count"`
}

// SlowTimePoint holds per-minute aggregates for slow queries.
type SlowTimePoint struct {
	Minute      string  `json:"minute"`
	AvgDocs     float64 `json:"avgDocs"`
	AvgKeys     float64 `json:"avgKeys"`
	AvgReturned float64 `json:"avgReturned"`
	AvgDuration float64 `json:"avgDuration"`
	Count       int     `json:"count"`
}

// KV is a generic key-value pair for sorted counts.
type KV struct {
	Key   string `json:"key"`
	Value int    `json:"value"`
}

// MemberStateEvent is a single state transition for a replica set member.
type MemberStateEvent struct {
	Host      string    `json:"host"`
	State     string    `json:"state"`
	Timestamp time.Time `json:"timestamp"`
}

// TimelineEvent represents a significant lifecycle or replication event on the timeline.
type TimelineEvent struct {
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"type"`     // crash, unclean_shutdown, shutdown, startup, restart, stepdown, primary_change, rollback, initial_sync, election, heartbeat_fail
	Message   string    `json:"message"`
	Details   string    `json:"details"`
	Severity  string    `json:"severity"` // critical, warning, info
	RawLine   string    `json:"rawLine"`  // original JSON log line
	FileName  string    `json:"fileName"`
}

// ConnStats holds aggregated connection metrics from the NETWORK component.
type ConnStats struct {
	TotalOpened   int          `json:"totalOpened"`
	TotalClosed   int          `json:"totalClosed"`
	PeakCount     int64        `json:"peakCount"`
	UniqueClients int          `json:"uniqueClients"`
	AuthFailures  int          `json:"authFailures"`
	GSSAPIErrors  int          `json:"gssapiErrors"`
	TopClients    []ConnClient `json:"topClients"`
	TopUsers      []ConnUser   `json:"topUsers"`
	TopApps       []ConnApp    `json:"topApps"`
	Hourly        []ConnHourly `json:"hourly"`
	Timeseries    []ConnPoint  `json:"timeseries"`
}

type ConnClient struct {
	Remote string `json:"remote"`
	Count  int    `json:"count"`
}

type ConnUser struct {
	User      string `json:"user"`
	Mechanism string `json:"mechanism"`
	DB        string `json:"db"`
	Count     int    `json:"count"`
	Failures  int    `json:"failures"`
	IsCluster bool   `json:"isCluster"`
}

type ConnApp struct {
	Name    string `json:"name"`
	Driver  string `json:"driver"`
	Count   int    `json:"count"`
}

type ConnHourly struct {
	Hour   string `json:"hour"`
	Opened int    `json:"opened"`
	Closed int    `json:"closed"`
}

// ConnPoint holds per-minute maximum connectionCount for the timeseries chart.
type ConnPoint struct {
	T string `json:"t"` // "2006-01-02T15:04"
	V int64  `json:"v"` // max connectionCount seen in that minute
}

// ─── Internal accumulators ────────────────────────────────────────────────────

type fileStats struct {
	Name      string
	Total     int
	Errors    int
	Warnings  int
	SlowCount int
	Start     time.Time
	End       time.Time
	Size      int64
}

type slowStats struct {
	QueryHash          string
	Namespace          string
	CommandType        string
	PlanSummary        string
	ExampleCmd         string
	LastRawLine        string
	Count              int
	MaxDuration        int64
	MinDuration        int64
	TotalDuration      int64
	TotalDocs          int64
	TotalKeys          int64
	TotalReturned      int64
	TotalResLen        int64
	TotalYields        int64
	TotalRemoteWait    int64
	MaxRemoteWait      int64
	TotalPlanningMicros int64
	ShardedCount       int   // entries where nShards > 0
	Files              []string
}

type errStats struct {
	Count         int
	Component     string
	Severity      string
	Namespaces    map[string]bool
	LastRawLine   string
	LastTimestamp time.Time
}

type msgStats struct {
	Count      int
	CompCounts map[string]int
	SevCounts  map[string]int
	FirstTs    time.Time
	LastTs     time.Time
}

type slowMinStats struct {
	TotalDocs     int64
	TotalKeys     int64
	TotalReturned int64
	TotalDuration int64
	Count         int
}

// ─── Duration buckets definition ─────────────────────────────────────────────

var durationBucketDefs = []struct {
	label string
	min   int64
	max   int64
}{
	{"100–250ms", 100, 250},
	{"250–500ms", 250, 500},
	{"500ms–1s", 500, 1000},
	{"1s–2s", 1000, 2000},
	{"2s–5s", 2000, 5000},
	{"5s–15s", 5000, 15000},
	{"15s–30s", 15000, 30000},
	{"30s+", 30000, -1},
}

// ─── Analyze ──────────────────────────────────────────────────────────────────

func analyze(entries []LogEntry, fileSizes map[string]int64) *Report {
	report := &Report{
		TotalEntries:    len(entries),
		SeverityCounts:  make(map[string]int),
		ComponentCounts: make(map[string]int),
	}

	if len(entries) == 0 {
		return report
	}

	fileMap     := make(map[string]*fileStats)
	slowMap     := make(map[string]*slowStats)
	msgMap      := make(map[string]*msgStats)
	errMap      := make(map[string]*errStats)
	hourlyMap   := make(map[string]*HourlyBucket)
	slowMinMap  := make(map[string]*slowMinStats)
	cmdTypeMap  := make(map[string]int)
	planMap     := make(map[string]int)
	durationCnt := make([]int, len(durationBucketDefs))
	seenDrivers := make(map[string]bool)
	var scatterRaw []SlowScatterPoint

	// connection tracking
	connClientMap  := make(map[string]int)
	connHourMap    := make(map[string]*ConnHourly)
	connTsMap      := make(map[string]int64)
	var connPeak   int64
	var connGSSAPI int

	// app name tracking (from client metadata)
	type appStats struct {
		Driver string
		Count  int
	}
	connAppMap := make(map[string]*appStats)

	// member state tracking (host -> last known state)
	memberStateMap := make(map[string]string)

	// auth user tracking
	type authStats struct {
		Mechanism string
		DB        string
		Count     int
		Failures  int
		IsCluster bool
	}
	authUserMap := make(map[string]*authStats)
	var connOpened, connClosed, connAuthFail int

	report.TimeRange.Start = entries[0].Timestamp
	report.TimeRange.End   = entries[0].Timestamp

	for i := range entries {
		e := &entries[i]

		// ── global time range ──────────────────────────────────────────────
		if !e.Timestamp.IsZero() {
			if e.Timestamp.Before(report.TimeRange.Start) {
				report.TimeRange.Start = e.Timestamp
			}
			if e.Timestamp.After(report.TimeRange.End) {
				report.TimeRange.End = e.Timestamp
			}
		}

		// ── cluster / version info ─────────────────────────────────────────────
		if e.MongoVersion != "" && report.Cluster.MongoVersion == "" {
			report.Cluster.MongoVersion = e.MongoVersion
			report.Cluster.GitVersion   = e.GitVersion
		}
		if e.OSName != "" && report.Cluster.OSName == "" {
			report.Cluster.OSName    = e.OSName
			report.Cluster.OSVersion = e.OSVersion
			report.Cluster.OSArch    = e.OSArch
		}
		if e.ReplicaSet != "" && report.Cluster.ReplicaSet == "" {
			report.Cluster.ReplicaSet = e.ReplicaSet
		}
		if e.DriverName != "" {
			k := e.DriverName + "|" + e.DriverVersion
			if !seenDrivers[k] {
				seenDrivers[k] = true
				report.Cluster.Drivers = append(report.Cluster.Drivers, DriverInfo{
					Name:    e.DriverName,
					Version: e.DriverVersion,
					App:     e.AppName,
				})
			}
		}
		if e.ReplMembers != "" {
			// last seen replica set config wins
			report.Cluster.Members = parseReplMembers(e.ReplMembers)
		}

		// ── severity / component counts ────────────────────────────────────
		report.SeverityCounts[e.Severity]++
		if e.Component != "" {
			report.ComponentCounts[e.Component]++
		}

		// ── per-file stats ─────────────────────────────────────────────────
		fs := fileMap[e.FileName]
		if fs == nil {
			fs = &fileStats{Name: e.FileName}
			fileMap[e.FileName] = fs
		}
		fs.Total++
		if e.Severity == "E" || e.Severity == "F" {
			fs.Errors++
		}
		if e.Severity == "W" {
			fs.Warnings++
		}
		if !e.Timestamp.IsZero() {
			if fs.Start.IsZero() || e.Timestamp.Before(fs.Start) {
				fs.Start = e.Timestamp
			}
			if e.Timestamp.After(fs.End) {
				fs.End = e.Timestamp
			}
		}

		// ── per-minute activity ────────────────────────────────────────────
		if !e.Timestamp.IsZero() {
			hour := e.Timestamp.UTC().Format("2006-01-02T15:04")
			hb := hourlyMap[hour]
			if hb == nil {
				hb = &HourlyBucket{Hour: hour}
				hourlyMap[hour] = hb
			}
			hb.Total++
			if e.Severity == "E" || e.Severity == "F" {
				hb.Errors++
			}
			if e.Severity == "W" {
				hb.Warns++
			}
			if e.DurationMillis > 0 {
				hb.Slow++
			}
		}

		// ── error grouping ─────────────────────────────────────────────────
		if e.ErrMsg != "" {
			key := fmt.Sprintf("%d|%s", e.ErrCode, truncate(e.ErrMsg, 200))
			es := errMap[key]
			if es == nil {
				es = &errStats{Namespaces: make(map[string]bool)}
				errMap[key] = es
			}
			es.Count++
			es.Component = e.Component
			es.Severity = e.Severity
			if e.Namespace != "" {
				es.Namespaces[e.Namespace] = true
			}
			// always overwrite — entries are processed in order, so the last one wins
			es.LastRawLine = e.RawLine
			es.LastTimestamp = e.Timestamp
		}

		// ── message grouping by exact msg field ────────────────────────────
		if e.Message != "" {
			ms := msgMap[e.Message]
			if ms == nil {
				ms = &msgStats{
					CompCounts: make(map[string]int),
					SevCounts:  make(map[string]int),
				}
				msgMap[e.Message] = ms
			}
			ms.Count++
			ms.CompCounts[e.Component]++
			ms.SevCounts[e.Severity]++
			if !e.Timestamp.IsZero() {
				if ms.FirstTs.IsZero() || e.Timestamp.Before(ms.FirstTs) {
					ms.FirstTs = e.Timestamp
				}
				if e.Timestamp.After(ms.LastTs) {
					ms.LastTs = e.Timestamp
				}
			}
		}

		// ── slow query processing ──────────────────────────────────────────
		if e.DurationMillis > 0 && e.Message == "Slow query" {
			fs.SlowCount++

			// duration histogram bucket
			for bi, b := range durationBucketDefs {
				if e.DurationMillis >= b.min && (b.max == -1 || e.DurationMillis < b.max) {
					durationCnt[bi]++
					break
				}
			}

			// command type & plan counts
			ct := e.CommandType
			if ct == "" {
				ct = "unknown"
			}
			cmdTypeMap[ct]++

			ps := planKey(e.PlanSummary)
			planMap[ps]++

			// slow query pattern grouping — skip entries with no namespace and no hash
			if e.Namespace == "" && e.QueryHash == "" {
				continue
			}
			key := slowQueryKey(e)
			ss := slowMap[key]
			if ss == nil {
				ss = &slowStats{
					QueryHash:   e.QueryHash,
					Namespace:   e.Namespace,
					CommandType: e.CommandType,
					PlanSummary: e.PlanSummary,
					ExampleCmd:  e.RawCommand,
				}
				slowMap[key] = ss
			}
			ss.Count++
			ss.TotalDuration += e.DurationMillis
			if e.DurationMillis > ss.MaxDuration {
				ss.MaxDuration = e.DurationMillis
			}
			if ss.MinDuration == 0 || e.DurationMillis < ss.MinDuration {
				ss.MinDuration = e.DurationMillis
			}
			ss.TotalResLen  += e.ResLen
			ss.TotalYields  += e.NumYields
			ss.TotalRemoteWait += e.RemoteOpWaitMillis
			ss.TotalPlanningMicros += e.PlanningTimeMicros
			if e.RemoteOpWaitMillis > ss.MaxRemoteWait {
				ss.MaxRemoteWait = e.RemoteOpWaitMillis
			}
			if e.NShards > 0 {
				ss.ShardedCount++
			}
			ss.TotalDocs += e.DocsExamined
			ss.TotalKeys += e.KeysExamined
			ss.TotalReturned += e.NReturned
			ss.LastRawLine = e.RawLine // always update → last occurrence
			if !e.Timestamp.IsZero() {
				scatterRaw = append(scatterRaw, SlowScatterPoint{T: e.Timestamp.UnixMilli(), D: e.DurationMillis, NS: e.Namespace})
			}
			if !containsStr(ss.Files, e.FileName) {
				ss.Files = append(ss.Files, e.FileName)
			}

			// per-minute time series for slow queries
			if !e.Timestamp.IsZero() {
				min := e.Timestamp.UTC().Format("2006-01-02T15:04")
				sm := slowMinMap[min]
				if sm == nil {
					sm = &slowMinStats{}
					slowMinMap[min] = sm
				}
				sm.Count++
				sm.TotalDocs += e.DocsExamined
				sm.TotalKeys += e.KeysExamined
				sm.TotalReturned += e.NReturned
				sm.TotalDuration += e.DurationMillis
			}
		}

		// ── connection events (NETWORK component) ─────────────────────────
		if e.Component == "NETWORK" {
			switch e.Message {
			case "Connection accepted":
				connOpened++
				if e.ConnectionCount > connPeak {
					connPeak = e.ConnectionCount
				}
				ip := extractIP(e.RemoteAddr)
				if ip != "" {
					connClientMap[ip]++
				}
				if !e.Timestamp.IsZero() {
					hour := e.Timestamp.UTC().Format("2006-01-02T15:04")
					if ch := connHourMap[hour]; ch != nil {
						ch.Opened++
					} else {
						connHourMap[hour] = &ConnHourly{Hour: hour, Opened: 1}
					}
					if e.ConnectionCount > connTsMap[hour] {
						connTsMap[hour] = e.ConnectionCount
					}
				}
			case "Connection ended":
				connClosed++
				if e.ConnectionCount > connPeak {
					connPeak = e.ConnectionCount
				}
				if !e.Timestamp.IsZero() {
					hour := e.Timestamp.UTC().Format("2006-01-02T15:04")
					if ch := connHourMap[hour]; ch != nil {
						ch.Closed++
					} else {
						connHourMap[hour] = &ConnHourly{Hour: hour, Closed: 1}
					}
					if e.ConnectionCount > connTsMap[hour] {
						connTsMap[hour] = e.ConnectionCount
					}
				}
			}
			lower := strings.ToLower(e.Message)
			if strings.Contains(lower, "authentication failed") || strings.Contains(lower, "sasl") && strings.Contains(lower, "failed") {
				connAuthFail++
			}
		}
		// also catch auth failures from ACCESS component
		if e.Component == "ACCESS" {
			lower := strings.ToLower(e.Message)
			if strings.Contains(lower, "authentication failed") || strings.Contains(lower, "unauthorized") {
				connAuthFail++
				if e.AuthUser != "" {
					if s := authUserMap[e.AuthUser]; s != nil {
						s.Failures++
					} else {
						authUserMap[e.AuthUser] = &authStats{Mechanism: e.AuthMechanism, DB: e.AuthDB, Failures: 1}
					}
				}
			}
			// GSSAPI / Kerberos step failures
			if strings.Contains(lower, "gssapi") || strings.Contains(lower, "sasl server message") {
				connGSSAPI++
			}
			if e.AuthUser != "" && e.Message == "Successfully authenticated" {
				if s := authUserMap[e.AuthUser]; s != nil {
					s.Count++
				} else {
					authUserMap[e.AuthUser] = &authStats{Mechanism: e.AuthMechanism, DB: e.AuthDB, Count: 1, IsCluster: e.AuthIsCluster}
				}
			}
		}

		// ── app name tracking (client metadata) ───────────────────────────
		if e.Message == "client metadata" && (e.AppName != "" || e.DriverName != "") {
			key := e.AppName
			if key == "" {
				key = "[" + e.DriverName + "]"
			}
			if s := connAppMap[key]; s != nil {
				s.Count++
			} else {
				connAppMap[key] = &appStats{Driver: e.DriverName, Count: 1}
			}
		}

		// ── member state tracking ──────────────────────────────────────────
		if e.MemberHost != "" && e.MemberState != "" {
			memberStateMap[e.MemberHost] = e.MemberState
			if !e.Timestamp.IsZero() {
				report.MemberStateHistory = append(report.MemberStateHistory, MemberStateEvent{
					Host:      e.MemberHost,
					State:     e.MemberState,
					Timestamp: e.Timestamp,
				})
			}
		}

		// ── replication events ─────────────────────────────────────────────
		comp := strings.ToUpper(e.Component)
		if comp == "REPL" || comp == "ELECTION" || comp == "REPL_HB" || comp == "RS" {
			if ev := classifyReplEvent(e); ev != nil {
				report.ReplEvents = append(report.ReplEvents, *ev)
			}
		}

		// ── timeline events ────────────────────────────────────────────────
		if te := classifyTimelineEvent(e); te != nil {
			report.Timeline = append(report.Timeline, *te)
		}
	}

	// ── FileInfo list ──────────────────────────────────────────────────────
	for _, fs := range fileMap {
		report.Files = append(report.Files, FileInfo{
			Name:       fs.Name,
			EntryCount: fs.Total,
			ErrorCount: fs.Errors,
			WarnCount:  fs.Warnings,
			SlowCount:  fs.SlowCount,
			TimeRange:  TimeRange{Start: fs.Start, End: fs.End},
			Size:       fileSizes[fs.Name],
		})
	}
	sort.Slice(report.Files, func(i, j int) bool {
		return report.Files[i].Name < report.Files[j].Name
	})

	// ── slow query list ────────────────────────────────────────────────────
	for _, ss := range slowMap {
		sq := SlowQuery{
			QueryHash:      ss.QueryHash,
			Namespace:      ss.Namespace,
			CommandType:    ss.CommandType,
			PlanSummary:    ss.PlanSummary,
			Count:          ss.Count,
			MaxDuration:    ss.MaxDuration,
			MinDuration:    ss.MinDuration,
			TotalDuration:  ss.TotalDuration,
			ExampleCommand: ss.ExampleCmd,
			LastRawLine:    ss.LastRawLine,
			Files:          ss.Files,
		}
		if ss.Count > 0 {
			sq.AvgDuration            = float64(ss.TotalDuration) / float64(ss.Count)
			sq.AvgDocsExamined        = float64(ss.TotalDocs) / float64(ss.Count)
			sq.AvgKeysExamined        = float64(ss.TotalKeys) / float64(ss.Count)
			sq.AvgNReturned           = float64(ss.TotalReturned) / float64(ss.Count)
			sq.Inefficiency           = float64(ss.TotalDocs) / float64(ss.TotalReturned+1)
			sq.AvgRemoteWaitMs        = float64(ss.TotalRemoteWait) / float64(ss.Count)
			sq.MaxRemoteWaitMs        = ss.MaxRemoteWait
			sq.AvgPlanningTimeMicros  = float64(ss.TotalPlanningMicros) / float64(ss.Count)
			if sq.AvgDuration > 0 && sq.AvgRemoteWaitMs > 0 {
				sq.RemoteWaitPct = sq.AvgRemoteWaitMs / sq.AvgDuration * 100
			}
			sq.IsSharded = ss.ShardedCount > 0
		}
		report.SlowQueries = append(report.SlowQueries, sq)
	}
	sort.Slice(report.SlowQueries, func(i, j int) bool {
		return report.SlowQueries[i].MaxDuration > report.SlowQueries[j].MaxDuration
	})

	// ── scatter points: sort by time, sample down to 10k if needed ──────
	sort.Slice(scatterRaw, func(i, j int) bool { return scatterRaw[i].T < scatterRaw[j].T })
	const maxScatter = 10000
	if len(scatterRaw) <= maxScatter {
		report.SlowScatter = scatterRaw
	} else {
		step := len(scatterRaw) / maxScatter
		report.SlowScatter = make([]SlowScatterPoint, 0, maxScatter)
		for i := 0; i < len(scatterRaw); i += step {
			report.SlowScatter = append(report.SlowScatter, scatterRaw[i])
		}
	}

	// ── replication events: sort then deduplicate cross-node duplicates ────
	sort.Slice(report.ReplEvents, func(i, j int) bool {
		return report.ReplEvents[i].Timestamp.Before(report.ReplEvents[j].Timestamp)
	})
	{
		deduped := report.ReplEvents[:0]
		seen := make(map[string]bool)
		for _, ev := range report.ReplEvents {
			// Key: eventType + newPrimary (if any) + timestamp truncated to 30s bucket.
			// This collapses the same election/primary-change seen by multiple nodes.
			bucket := ev.Timestamp.Unix() / 30
			key := fmt.Sprintf("%s|%s|%d", ev.EventType, ev.NewPrimary, bucket)
			if !seen[key] {
				seen[key] = true
				deduped = append(deduped, ev)
			}
		}
		report.ReplEvents = deduped
	}

	// ── timeline: sort then deduplicate ────────────────────────────────────
	sort.Slice(report.Timeline, func(i, j int) bool {
		return report.Timeline[i].Timestamp.Before(report.Timeline[j].Timestamp)
	})
	{
		deduped := report.Timeline[:0]
		seen := make(map[string]bool)
		for _, te := range report.Timeline {
			key := fmt.Sprintf("%d|%s|%s", te.Timestamp.UnixNano(), te.Type, te.Details)
			if !seen[key] {
				seen[key] = true
				deduped = append(deduped, te)
			}
		}
		report.Timeline = deduped
	}

	// ── message groups (by exact msg) ─────────────────────────────────────
	for msg, ms := range msgMap {
		report.MessageGroups = append(report.MessageGroups, MessageGroup{
			Message:   msg,
			Count:     ms.Count,
			Component: topKey(ms.CompCounts),
			Severity:  topKey(ms.SevCounts),
			FirstTs:   ms.FirstTs,
			LastTs:    ms.LastTs,
		})
	}
	sort.Slice(report.MessageGroups, func(i, j int) bool {
		return report.MessageGroups[i].Count > report.MessageGroups[j].Count
	})
	if len(report.MessageGroups) > 200 {
		report.MessageGroups = report.MessageGroups[:200]
	}

	// ── error groups ───────────────────────────────────────────────────────
	for key, es := range errMap {
		parts := strings.SplitN(key, "|", 2)
		errMsg := ""
		if len(parts) == 2 {
			errMsg = parts[1]
		}
		eg := ErrorGroup{
			ErrMsg:        errMsg,
			ErrCode:       es.Count, // placeholder – overwritten below
			Count:         es.Count,
			Component:     es.Component,
			Severity:      es.Severity,
			LastRawLine:   es.LastRawLine,
			LastTimestamp: es.LastTimestamp,
		}
		// parse code from key prefix
		var code int
		fmt.Sscanf(parts[0], "%d", &code)
		eg.ErrCode = code
		for ns := range es.Namespaces {
			eg.Namespaces = append(eg.Namespaces, ns)
		}
		report.ErrorGroups = append(report.ErrorGroups, eg)
	}
	sort.Slice(report.ErrorGroups, func(i, j int) bool {
		return report.ErrorGroups[i].Count > report.ErrorGroups[j].Count
	})

	// ── hourly activity sorted ─────────────────────────────────────────────
	for _, hb := range hourlyMap {
		report.HourlyActivity = append(report.HourlyActivity, *hb)
	}
	sort.Slice(report.HourlyActivity, func(i, j int) bool {
		return report.HourlyActivity[i].Hour < report.HourlyActivity[j].Hour
	})

	// ── duration distribution ──────────────────────────────────────────────
	for i, b := range durationBucketDefs {
		report.DurationDistribution = append(report.DurationDistribution, DurationBucket{
			Label: b.label,
			Min:   b.min,
			Max:   b.max,
			Count: durationCnt[i],
		})
	}

	// ── slow time series ───────────────────────────────────────────────────
	for min, sm := range slowMinMap {
		pt := SlowTimePoint{Minute: min, Count: sm.Count}
		if sm.Count > 0 {
			pt.AvgDocs     = float64(sm.TotalDocs) / float64(sm.Count)
			pt.AvgKeys     = float64(sm.TotalKeys) / float64(sm.Count)
			pt.AvgReturned = float64(sm.TotalReturned) / float64(sm.Count)
			pt.AvgDuration = float64(sm.TotalDuration) / float64(sm.Count)
		}
		report.SlowTimeSeries = append(report.SlowTimeSeries, pt)
	}
	sort.Slice(report.SlowTimeSeries, func(i, j int) bool {
		return report.SlowTimeSeries[i].Minute < report.SlowTimeSeries[j].Minute
	})
	// Cap at 500 points for chart readability; thin by keeping every Nth
	if len(report.SlowTimeSeries) > 500 {
		step := len(report.SlowTimeSeries) / 500
		var thin []SlowTimePoint
		for i := 0; i < len(report.SlowTimeSeries); i += step {
			thin = append(thin, report.SlowTimeSeries[i])
		}
		report.SlowTimeSeries = thin
	}

	// ── command type counts ────────────────────────────────────────────────
	for k, v := range cmdTypeMap {
		report.SlowCommandTypes = append(report.SlowCommandTypes, KV{Key: k, Value: v})
	}
	sort.Slice(report.SlowCommandTypes, func(i, j int) bool {
		return report.SlowCommandTypes[i].Value > report.SlowCommandTypes[j].Value
	})

	// ── plan summary counts ────────────────────────────────────────────────
	for k, v := range planMap {
		report.SlowPlanCounts = append(report.SlowPlanCounts, KV{Key: k, Value: v})
	}
	sort.Slice(report.SlowPlanCounts, func(i, j int) bool {
		return report.SlowPlanCounts[i].Value > report.SlowPlanCounts[j].Value
	})

	// ── connection stats ───────────────────────────────────────────────────
	var topClients []ConnClient
	for ip, cnt := range connClientMap {
		topClients = append(topClients, ConnClient{Remote: ip, Count: cnt})
	}
	sort.Slice(topClients, func(i, j int) bool {
		return topClients[i].Count > topClients[j].Count
	})
	if len(topClients) > 25 {
		topClients = topClients[:25]
	}

	var connHourly []ConnHourly
	for _, ch := range connHourMap {
		connHourly = append(connHourly, *ch)
	}
	sort.Slice(connHourly, func(i, j int) bool {
		return connHourly[i].Hour < connHourly[j].Hour
	})

	var connTimeseries []ConnPoint
	for t, v := range connTsMap {
		connTimeseries = append(connTimeseries, ConnPoint{T: t, V: v})
	}
	sort.Slice(connTimeseries, func(i, j int) bool {
		return connTimeseries[i].T < connTimeseries[j].T
	})

	var topUsers []ConnUser
	for user, s := range authUserMap {
		topUsers = append(topUsers, ConnUser{
			User:      user,
			Mechanism: s.Mechanism,
			DB:        s.DB,
			Count:     s.Count,
			Failures:  s.Failures,
			IsCluster: s.IsCluster,
		})
	}
	sort.Slice(topUsers, func(i, j int) bool {
		return topUsers[i].Count > topUsers[j].Count
	})
	if len(topUsers) > 50 {
		topUsers = topUsers[:50]
	}

	var topApps []ConnApp
	for name, s := range connAppMap {
		topApps = append(topApps, ConnApp{Name: name, Driver: s.Driver, Count: s.Count})
	}
	sort.Slice(topApps, func(i, j int) bool { return topApps[i].Count > topApps[j].Count })
	if len(topApps) > 30 {
		topApps = topApps[:30]
	}

	report.ConnStats = ConnStats{
		TotalOpened:   connOpened,
		TotalClosed:   connClosed,
		PeakCount:     connPeak,
		UniqueClients: len(connClientMap),
		AuthFailures:  connAuthFail,
		GSSAPIErrors:  connGSSAPI,
		TopClients:    topClients,
		TopUsers:      topUsers,
		TopApps:       topApps,
		Hourly:        connHourly,
		Timeseries:    connTimeseries,
	}

	// ── member state history: sort by time, then deduplicate ──────────────
	// Multiple observers (other RS members) log the same state change, producing
	// duplicate events. After sorting globally by time, we suppress any event
	// where the host's state hasn't actually changed since its last recorded event.
	sort.Slice(report.MemberStateHistory, func(i, j int) bool {
		return report.MemberStateHistory[i].Timestamp.Before(report.MemberStateHistory[j].Timestamp)
	})
	{
		lastState := make(map[string]string)
		deduped := report.MemberStateHistory[:0]
		for _, ev := range report.MemberStateHistory {
			if lastState[ev.Host] != ev.State {
				lastState[ev.Host] = ev.State
				deduped = append(deduped, ev)
			}
		}
		report.MemberStateHistory = deduped
	}

	// ── merge member states into cluster members ──────────────────────────
	for i := range report.Cluster.Members {
		if state, ok := memberStateMap[report.Cluster.Members[i].Host]; ok {
			report.Cluster.Members[i].State = state
		}
	}

	return report
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func slowQueryKey(e *LogEntry) string {
	if e.QueryHash != "" {
		return e.QueryHash
	}
	return fmt.Sprintf("%s|%s|%s", e.Namespace, e.CommandType, truncate(e.RawCommand, 80))
}

// planKey normalises a plan summary to its main scan type for bucketing.
func planKey(plan string) string {
	plan = strings.TrimSpace(plan)
	if plan == "" {
		return "UNKNOWN"
	}
	upper := strings.ToUpper(plan)
	switch {
	case strings.HasPrefix(upper, "COLLSCAN"):
		return "COLLSCAN"
	case strings.HasPrefix(upper, "IXSCAN"):
		return "IXSCAN"
	case strings.HasPrefix(upper, "IDHACK"):
		return "IDHACK"
	case strings.HasPrefix(upper, "COUNT"):
		return "COUNT"
	case strings.HasPrefix(upper, "EOF"):
		return "EOF"
	default:
		// Return first word
		if idx := strings.IndexAny(upper, " {"); idx > 0 {
			return upper[:idx]
		}
		return upper
	}
}

// parseReplMembers extracts host/priority info from a raw JSON members array.
func parseReplMembers(raw string) []ReplMember {
	result := gjson.Parse(raw)
	var members []ReplMember
	result.ForEach(func(_, v gjson.Result) bool {
		m := ReplMember{
			Host:     v.Get("host").String(),
			ID:       v.Get("_id").Int(),
			Priority: v.Get("priority").Float(),
			Hidden:   v.Get("hidden").Bool(),
			Arbiter:  v.Get("arbiterOnly").Bool(),
		}
		if m.Host != "" {
			members = append(members, m)
		}
		return true
	})
	return members
}

// extractIP strips the port from a "host:port" remote address string.
func extractIP(remote string) string {
	if remote == "" {
		return ""
	}
	// Handle IPv6 like "[::1]:27017"
	if remote[0] == '[' {
		if end := strings.Index(remote, "]"); end > 0 {
			return remote[1:end]
		}
	}
	if idx := strings.LastIndex(remote, ":"); idx > 0 {
		return remote[:idx]
	}
	return remote
}

// topKey returns the key with the highest count in a map.
func topKey(m map[string]int) string {
	best, bestV := "", 0
	for k, v := range m {
		if v > bestV {
			best, bestV = k, v
		}
	}
	return best
}

func classifyReplEvent(e *LogEntry) *ReplEvent {
	msg := strings.ToLower(e.Message)

	ev := &ReplEvent{
		Timestamp:  e.Timestamp,
		Component:  e.Component,
		Message:    truncate(e.Message, 200),
		NewPrimary: e.NewPrimary,
		ReplicaSet: e.ReplicaSet,
		Term:       e.Term,
		Severity:   e.Severity,
		FileName:   e.FileName,
		RawLine:    e.RawLine,
	}

	switch {
	case strings.Contains(msg, "election") && (strings.Contains(msg, "won") || strings.Contains(msg, "win")):
		ev.EventType = "election_won"
	case strings.Contains(msg, "election") && strings.Contains(msg, "start"):
		ev.EventType = "election_start"
	case strings.Contains(msg, "election"):
		ev.EventType = "election"
	case strings.Contains(msg, "stepdown") || strings.Contains(msg, "step down") || strings.Contains(msg, "stepping down"):
		ev.EventType = "stepdown"
	// "Member is in new state" with PRIMARY → treat as primary_change
	case msg == "member is in new state" && e.NewPrimary != "":
		ev.EventType = "primary_change"
	case strings.Contains(msg, "primary") && (strings.Contains(msg, "new") || strings.Contains(msg, "became")):
		ev.EventType = "primary_change"
	case strings.Contains(msg, "secondary") && strings.Contains(msg, "became"):
		ev.EventType = "secondary_change"
	case strings.Contains(msg, "heartbeat") && (e.Severity == "E" || e.Severity == "W"):
		ev.EventType = "heartbeat_error"
	case strings.Contains(msg, "heartbeat"):
		ev.EventType = "heartbeat"
	case strings.Contains(msg, "config"):
		ev.EventType = "config_change"
	case strings.Contains(msg, "sync"):
		ev.EventType = "initial_sync"
	case strings.Contains(msg, "rollback"):
		ev.EventType = "rollback"
	default:
		ev.EventType = "repl_other"
	}

	return ev
}

// classifyTimelineEvent maps a log entry to a significant timeline event, or returns nil.
func classifyTimelineEvent(e *LogEntry) *TimelineEvent {
	if e.Timestamp.IsZero() {
		return nil
	}
	msg := e.Message

	te := &TimelineEvent{
		Timestamp: e.Timestamp,
		Message:   msg,
		Severity:  "info",
		RawLine:   e.RawLine,
		FileName:  e.FileName,
	}

	switch {
	// ── crashes / abnormal termination ──────────────────────────────────────
	case strings.Contains(msg, "aborting after fassert") || strings.Contains(msg, "fassert()"):
		te.Type = "crash"
		te.Severity = "critical"
		te.Details = "Fatal assertion failure — process aborted"

	case msg == "Detected unclean shutdown - Lock file is not empty":
		te.Type = "unclean_shutdown"
		te.Severity = "critical"
		te.Details = "Previous shutdown was unclean (crash or kill -9)"

	case strings.Contains(msg, "Computed timestamp to truncate pre-images at after unclean shutdown") ||
		strings.Contains(msg, "Incrementing the rollback ID after unclean shutdown"):
		// secondary unclean shutdown indicators — skip, already captured above
		return nil

	// ── clean shutdown ───────────────────────────────────────────────────────
	case msg == "mongod shutdown complete":
		te.Type = "shutdown"
		te.Severity = "warning"
		te.Details = "Clean shutdown completed"

	case msg == "Entering quiesce mode for shutdown":
		te.Type = "shutdown_start"
		te.Severity = "warning"
		te.Details = "Shutdown initiated"

	// ── startup ─────────────────────────────────────────────────────────────
	case msg == "MongoDB starting":
		te.Type = "startup"
		te.Severity = "info"
		if e.MongoVersion != "" {
			te.Details = "v" + e.MongoVersion
		}

	case msg == "mongod startup complete":
		te.Type = "startup_done"
		te.Severity = "info"
		te.Details = "Instance ready to accept connections"

	case msg == "***** SERVER RESTARTED *****":
		te.Type = "restart"
		te.Severity = "warning"
		te.Details = "Server restarted"

	// ── replication topology ─────────────────────────────────────────────────
	case strings.Contains(msg, "stepdown") || strings.Contains(msg, "step down") || strings.Contains(msg, "stepping down"):
		comp := strings.ToUpper(e.Component)
		if comp != "REPL" && comp != "ELECTION" && comp != "REPL_HB" && comp != "RS" {
			return nil // only capture from repl components
		}
		te.Type = "stepdown"
		te.Severity = "warning"
		te.Details = msg

	// "Replica set primary server change detected" comes from NETWORK component
	// and has attr.primary with the actual new primary hostname
	case msg == "Replica set primary server change detected":
		te.Type = "primary_change"
		te.Severity = "warning"
		if e.NewPrimary != "" {
			te.Details = e.NewPrimary
		}
		if e.ReplicaSet != "" {
			te.Message = e.ReplicaSet
		}

	case (strings.Contains(msg, "primary") && (strings.Contains(msg, "new") || strings.Contains(msg, "became"))) ||
		msg == "Exiting drain mode" ||
		msg == "Restarting heartbeats after learning of a new primary":
		comp := strings.ToUpper(e.Component)
		if comp != "REPL" && comp != "ELECTION" && comp != "REPL_HB" && comp != "RS" {
			return nil
		}
		// Skip the "Restarting heartbeats" if we already have the NETWORK-level event above
		if msg == "Restarting heartbeats after learning of a new primary" {
			return nil
		}
		te.Type = "primary_change"
		te.Severity = "warning"
		if e.NewPrimary != "" {
			te.Details = e.NewPrimary
		}
		if e.ReplicaSet != "" {
			te.Message = e.ReplicaSet
		}

	case strings.Contains(msg, "rollback"):
		comp := strings.ToUpper(e.Component)
		if comp != "REPL" && comp != "ELECTION" && comp != "REPL_HB" && comp != "RS" {
			return nil
		}
		te.Type = "rollback"
		te.Severity = "critical"
		te.Details = msg

	case strings.Contains(msg, "Starting initial sync"):
		te.Type = "initial_sync"
		te.Severity = "warning"
		te.Details = "Initial sync started"

	case msg == "Initial sync done":
		te.Type = "initial_sync_done"
		te.Severity = "info"
		te.Details = "Initial sync completed successfully"

	default:
		return nil
	}

	return te
}
