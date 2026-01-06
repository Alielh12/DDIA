# DIDA — Distributed Indexed Data (Minimal Server)

This repository contains a minimal HTTP server for a distributed key–value
store prototype. It focuses on the server scaffolding and configuration,
not on storage logic yet.

Features implemented:

- HTTP REST server with a health endpoint (`GET /health`) ✅
- Configuration via flags and environment variables ✅
- Ensures the data directory exists on startup ✅
- Durable logging to a file with per-write fsync ⚠️ (to be expanded)
- Graceful startup and shutdown using OS signals ✅
- Clear separation of concerns (api, logging, config, storage placeholder) ✅

Run the server:

- Build: `go build ./...`
- Example: `./dida -data-dir ./data -log-file ./dida.log -addr :8080`

Environment variables (alternative to flags):
- `DIDA_DATA_DIR` — data directory (default `./data`)
- `DIDA_LOG_FILE` — log file path (default `./dida.log`)
- `DIDA_ADDR` — server listen address (default `:8080`)

Endpoint:
- GET /health — returns `{"status":"ok"}`
- PUT /kv/{key} — stores the request body as the value for `key` (append-only log)
- GET /kv/{key} — returns the latest value for `key` or 404 if not found

Storage notes:
- Each write appends a record to `data/store.log` in the format: `<timestamp>,<key>,<base64(value)>\n`
- Values are base64-encoded to avoid delimiter collisions and ensure safe, line-delimited records.
- Reads scan the log start-to-end and return the latest value for a key (no in-memory index yet).

Notes
- Storage logic (append-only log, segment files, replication) is intentionally
  not implemented yet to keep concerns separated. We'll add it as a separate
  package under `internal/storage` in the next steps.

