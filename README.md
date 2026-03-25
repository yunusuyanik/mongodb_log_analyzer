# MongoDB Log Analyzer

A fast, single-binary web dashboard for analyzing MongoDB log files. Parse and visualize slow queries, errors, replication events, connections, and server lifecycle — all from your browser.

![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)

---

## Features

- **Slow Queries** — grouped by normalized query shape, with execution plan, duration distribution, and time-series charts
- **Errors** — grouped error messages with severity breakdown and suggestions
- **Replication** — replica set topology, member states, and replication event log
- **Connections** — open/close stats, top client IPs, and application/driver breakdown
- **Timeline** — server lifecycle events (startup, shutdown, stepdown) across sessions
- **Messages** — all log message groups with severity and component filters
- **Overview** — top-level summary of the entire log set

Supports `.log`, `.log.N`, `.gz`, `.tar.gz`, and `.tgz` files.

---

## Installation

### Option 1 — Build from source

```bash
git clone https://github.com/yunusuyanik/mongodb_log_analyzer.git
cd mongodb_log_analyzer
go build -o mongodb-log-analyzer .
```

Requires Go 1.21+.

### Option 2 — Download binary

Download the latest release from the [Releases](https://github.com/yunusuyanik/mongodb_log_analyzer/releases) page.

---

## Usage

```bash
# Analyze a directory of log files
./mongodb-log-analyzer -path /var/log/mongodb/

# Analyze a single file
./mongodb-log-analyzer -path /var/log/mongodb/mongod.log

# Analyze compressed archives (glob pattern)
./mongodb-log-analyzer -path 'logs/mongo*.tar.gz'

# Use a custom port (default: 8080)
./mongodb-log-analyzer -path /var/log/mongodb/ -port 9090
```

Then open your browser at `http://localhost:8080`.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-path` | `.` | Directory, file path, or glob pattern |
| `-dir` | | Alias for `-path` |
| `-port` | `8080` | Web server port |

---

## Supported Log Formats

MongoDB structured JSON logs (version 4.4+). Make sure your MongoDB log verbosity includes the `NETWORK` component for connection tracking.

---

## License

MIT
