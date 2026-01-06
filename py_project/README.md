DIDA (Python port)

This is a minimal port of the Go DIDA service to Python. It provides:
- Endpoints: /health, /kv/{key} (PUT/GET), /internal/replicate (POST), /internal/version (GET), /metrics (GET), /admin/replicate (POST)
- Storage: simple LSM-like engine with WAL, memtable with versions, SSTables (sorted files), and compaction
- Durability: WAL is fsynced per write; SSTables are written via temp file + rename and directory fsync
- Metrics: simple in-memory counters for write/read latencies, replication lag, compaction duration

Run:

    python3 -m venv .venv
    . .venv/bin/activate
    python -m pip install -r requirements.txt  # optional; only stdlib is used by default
    python main.py --data-dir ./data --addr :8080

Endpoints examples:

PUT /kv/foo -> returns {"version": 123}
GET /kv/foo -> returns {"value":"<base64>", "version": 123}
GET /metrics -> JSON snapshot of metrics
POST /admin/replicate?target=http://follower:8080 -> pushes WAL tail to follower

Notes:
- This is a minimal, correctness-first implementation for testing and learning (not optimized for scale).