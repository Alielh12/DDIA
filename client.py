#!/usr/bin/env python3
"""
KV Store Server for DDIA-style Lab

Endpoints:
  PUT /kv/{key}   (leader accepts writes; follower rejects unless --allow-follower-writes)
  GET /kv/{key}   (leader/follower both serve reads)
  GET /internal/stats
  GET /internal/changes?since=<offset>   (leader only, for followers replication pull)
  POST /internal/compact                (log compaction)
  POST /internal/rebuild_index          (rebuild in-memory index from log)
  POST /internal/reset                  (danger: clears all data)
  POST /internal/flush                  (flush/fsync)

Storage engines:
  - log     : append-only log, reads scan log unless index enabled
  - lsm     : simple LSM-like (memtable + SSTables) to compare workload behavior
  - sqlite  : SQLite B-tree-backed KV store (good approximation of B-tree)

Replication:
  - Leader keeps an in-memory oplog with monotonically increasing offset.
  - Followers poll /internal/changes and apply operations locally.
"""

from __future__ import annotations

import argparse
import os
import json
import time
import threading
import sqlite3
from dataclasses import dataclass
from typing import Dict, Optional, List, Tuple, Any

import requests
from fastapi import FastAPI, Response, HTTPException, Request
from fastapi.responses import PlainTextResponse, JSONResponse
import uvicorn


# -----------------------------
# Common helpers
# -----------------------------

def unix_ms() -> int:
    return int(time.time() * 1000)

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def atomic_write(path: str, data: bytes) -> None:
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


# -----------------------------
# Engine interface
# -----------------------------

class Engine:
    def put(self, key: str, value: bytes) -> None:
        raise NotImplementedError

    def get(self, key: str) -> Optional[bytes]:
        raise NotImplementedError

    def stats(self) -> Dict[str, Any]:
        return {}

    def compact(self) -> Dict[str, Any]:
        return {"ok": True, "message": "compact not supported"}

    def rebuild_index(self) -> Dict[str, Any]:
        return {"ok": True, "message": "rebuild_index not supported"}

    def reset(self) -> Dict[str, Any]:
        return {"ok": True, "message": "reset not supported"}

    def flush(self) -> Dict[str, Any]:
        return {"ok": True, "message": "flush not supported"}


# -----------------------------
# Append-only log engine
# -----------------------------

class LogEngine(Engine):
    """
    Log format: each record is a JSON line:
      {"k": "...", "v_b64": "...", "ts": 123}
    We store raw bytes in base64 for safety.

    Read behavior:
      - If index_enabled: use in-memory index key->file offset
      - Else: scan log from start (slow; shows read latency growth in Activity 1)
    """

    def __init__(self, data_dir: str, index_enabled: bool):
        ensure_dir(data_dir)
        self.data_dir = data_dir
        self.log_path = os.path.join(data_dir, "kv.log")
        self.index_enabled = index_enabled

        self._lock = threading.Lock()
        self._index: Dict[str, int] = {}  # key -> byte offset in log
        self._writes = 0
        self._reads = 0

        # Ensure file exists
        open(self.log_path, "ab").close()

        if self.index_enabled:
            self._rebuild_index_locked()

    @staticmethod
    def _encode_bytes(b: bytes) -> str:
        import base64
        return base64.b64encode(b).decode("ascii")

    @staticmethod
    def _decode_bytes(s: str) -> bytes:
        import base64
        return base64.b64decode(s.encode("ascii"))

    def _rebuild_index_locked(self) -> None:
        self._index.clear()
        offset = 0
        with open(self.log_path, "rb") as f:
            for line in f:
                try:
                    rec = json.loads(line.decode("utf-8"))
                    k = rec["k"]
                    self._index[k] = offset
                except Exception:
                    pass
                offset += len(line)

    def put(self, key: str, value: bytes) -> None:
        rec = {"k": key, "v_b64": self._encode_bytes(value), "ts": unix_ms()}
        line = (json.dumps(rec, separators=(",", ":")) + "\n").encode("utf-8")

        with self._lock:
            with open(self.log_path, "ab") as f:
                off = f.tell()
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
            self._writes += 1
            if self.index_enabled:
                self._index[key] = off

    def get(self, key: str) -> Optional[bytes]:
        with self._lock:
            self._reads += 1

            # Fast path: in-memory index lookup
            if self.index_enabled and key in self._index:
                off = self._index[key]
                with open(self.log_path, "rb") as f:
                    f.seek(off)
                    line = f.readline()
                try:
                    rec = json.loads(line.decode("utf-8"))
                    return self._decode_bytes(rec["v_b64"])
                except Exception:
                    return None

        # Slow path: scan entire log (no index)
        latest: Optional[bytes] = None
        with open(self.log_path, "rb") as f:
            for line in f:
                try:
                    rec = json.loads(line.decode("utf-8"))
                    if rec.get("k") == key:
                        latest = self._decode_bytes(rec["v_b64"])
                except Exception:
                    continue
        return latest

    def compact(self) -> Dict[str, Any]:
        """
        Compaction rewrites log with only latest values per key.
        This simulates Activity 2 "compaction".
        """
        t0 = time.time()
        with self._lock:
            latest: Dict[str, bytes] = {}
            with open(self.log_path, "rb") as f:
                for line in f:
                    try:
                        rec = json.loads(line.decode("utf-8"))
                        k = rec["k"]
                        v = self._decode_bytes(rec["v_b64"])
                        latest[k] = v
                    except Exception:
                        continue

            new_lines = []
            for k, v in latest.items():
                rec = {"k": k, "v_b64": self._encode_bytes(v), "ts": unix_ms()}
                new_lines.append((json.dumps(rec, separators=(",", ":")) + "\n").encode("utf-8"))

            atomic_write(self.log_path, b"".join(new_lines))

            if self.index_enabled:
                self._rebuild_index_locked()

            dur = time.time() - t0
            return {"ok": True, "keys": len(latest), "duration_s": dur}

    def rebuild_index(self) -> Dict[str, Any]:
        if not self.index_enabled:
            return {"ok": False, "message": "Index disabled. Start server with --index"}
        t0 = time.time()
        with self._lock:
            self._rebuild_index_locked()
        return {"ok": True, "duration_s": time.time() - t0, "index_size": len(self._index)}

    def reset(self) -> Dict[str, Any]:
        with self._lock:
            atomic_write(self.log_path, b"")
            self._index.clear()
            self._writes = 0
            self._reads = 0
        return {"ok": True}

    def flush(self) -> Dict[str, Any]:
        with self._lock:
            # fsync log file (best-effort)
            try:
                with open(self.log_path, "ab") as f:
                    f.flush()
                    os.fsync(f.fileno())
            except Exception:
                pass
        return {"ok": True}

    def stats(self) -> Dict[str, Any]:
        try:
            size = os.path.getsize(self.log_path)
        except Exception:
            size = None
        with self._lock:
            return {
                "engine": "log",
                "index_enabled": self.index_enabled,
                "index_size": len(self._index) if self.index_enabled else 0,
                "log_bytes": size,
                "reads": self._reads,
                "writes": self._writes,
            }


# -----------------------------
# LSM-like engine (simple)
# -----------------------------

class LsmEngine(Engine):
    """
    Very simplified LSM:
      - memtable dict
      - when memtable reaches threshold, flush to SSTable file (sorted lines)
      - get checks memtable then SSTables newest->oldest
    """
    def __init__(self, data_dir: str, memtable_max: int = 5000):
        ensure_dir(data_dir)
        self.data_dir = data_dir
        self.memtable_max = memtable_max
        self._lock = threading.Lock()
        self._mem: Dict[str, bytes] = {}
        self._sstables: List[str] = []
        self._writes = 0
        self._reads = 0

        self._load_sstables()

    def _load_sstables(self):
        # load existing sstables in directory (if any)
        files = [f for f in os.listdir(self.data_dir) if f.startswith("sst_") and f.endswith(".jsonl")]
        files.sort()  # older->newer
        self._sstables = [os.path.join(self.data_dir, f) for f in files]

    @staticmethod
    def _encode_bytes(b: bytes) -> str:
        import base64
        return base64.b64encode(b).decode("ascii")

    @staticmethod
    def _decode_bytes(s: str) -> bytes:
        import base64
        return base64.b64decode(s.encode("ascii"))

    def _flush_memtable_locked(self) -> None:
        if not self._mem:
            return
        # write sorted by key
        items = sorted(self._mem.items(), key=lambda kv: kv[0])
        name = f"sst_{unix_ms()}_{len(self._sstables)}.jsonl"
        path = os.path.join(self.data_dir, name)
        lines = []
        for k, v in items:
            rec = {"k": k, "v_b64": self._encode_bytes(v), "ts": unix_ms()}
            lines.append((json.dumps(rec, separators=(",", ":")) + "\n").encode("utf-8"))
        atomic_write(path, b"".join(lines))
        self._sstables.append(path)
        self._mem.clear()

    def put(self, key: str, value: bytes) -> None:
        with self._lock:
            self._mem[key] = value
            self._writes += 1
            if len(self._mem) >= self.memtable_max:
                self._flush_memtable_locked()

    def get(self, key: str) -> Optional[bytes]:
        with self._lock:
            self._reads += 1
            if key in self._mem:
                return self._mem[key]
            sstables = list(self._sstables)

        # Search newest to oldest
        for path in reversed(sstables):
            try:
                with open(path, "rb") as f:
                    for line in f:
                        rec = json.loads(line.decode("utf-8"))
                        if rec.get("k") == key:
                            return self._decode_bytes(rec["v_b64"])
            except Exception:
                continue
        return None

    def compact(self) -> Dict[str, Any]:
        """
        Merge SSTables into one (simple compaction).
        """
        t0 = time.time()
        with self._lock:
            # flush memtable first
            self._flush_memtable_locked()

            latest: Dict[str, bytes] = {}
            for path in self._sstables:
                try:
                    with open(path, "rb") as f:
                        for line in f:
                            rec = json.loads(line.decode("utf-8"))
                            latest[rec["k"]] = self._decode_bytes(rec["v_b64"])
                except Exception:
                    continue

            # write merged sstable
            name = f"sst_compacted_{unix_ms()}.jsonl"
            merged_path = os.path.join(self.data_dir, name)
            items = sorted(latest.items(), key=lambda kv: kv[0])
            lines = []
            for k, v in items:
                rec = {"k": k, "v_b64": self._encode_bytes(v), "ts": unix_ms()}
                lines.append((json.dumps(rec, separators=(",", ":")) + "\n").encode("utf-8"))
            atomic_write(merged_path, b"".join(lines))

            # remove old tables
            for path in self._sstables:
                try:
                    os.remove(path)
                except Exception:
                    pass

            self._sstables = [merged_path]

        return {"ok": True, "keys": len(latest), "duration_s": time.time() - t0}

    def reset(self) -> Dict[str, Any]:
        with self._lock:
            self._mem.clear()
            for p in self._sstables:
                try:
                    os.remove(p)
                except Exception:
                    pass
            self._sstables = []
            self._writes = 0
            self._reads = 0
        return {"ok": True}

    def flush(self) -> Dict[str, Any]:
        with self._lock:
            self._flush_memtable_locked()
        return {"ok": True}

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            disk = 0
            for p in self._sstables:
                try:
                    disk += os.path.getsize(p)
                except Exception:
                    pass
            return {
                "engine": "lsm",
                "memtable_keys": len(self._mem),
                "sstables": len(self._sstables),
                "sstable_bytes": disk,
                "reads": self._reads,
                "writes": self._writes,
            }


# -----------------------------
# SQLite engine (B-tree)
# -----------------------------

class SqliteEngine(Engine):
    """
    SQLite uses a B-tree internally. Great proxy for Activity 3 "B-tree" option.
    """
    def __init__(self, data_dir: str):
        ensure_dir(data_dir)
        self.db_path = os.path.join(data_dir, "kv.sqlite")
        self._lock = threading.Lock()
        self._reads = 0
        self._writes = 0
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v BLOB);")
        self._conn.commit()

    def put(self, key: str, value: bytes) -> None:
        with self._lock:
            self._conn.execute("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v;", (key, value))
            self._conn.commit()
            self._writes += 1

    def get(self, key: str) -> Optional[bytes]:
        with self._lock:
            cur = self._conn.execute("SELECT v FROM kv WHERE k=?;", (key,))
            row = cur.fetchone()
            self._reads += 1
            return row[0] if row else None

    def compact(self) -> Dict[str, Any]:
        t0 = time.time()
        with self._lock:
            self._conn.execute("VACUUM;")
            self._conn.commit()
        return {"ok": True, "duration_s": time.time() - t0}

    def reset(self) -> Dict[str, Any]:
        with self._lock:
            self._conn.execute("DELETE FROM kv;")
            self._conn.commit()
            self._reads = 0
            self._writes = 0
        return {"ok": True}

    def stats(self) -> Dict[str, Any]:
        try:
            size = os.path.getsize(self.db_path)
        except Exception:
            size = None
        with self._lock:
            cur = self._conn.execute("SELECT COUNT(*) FROM kv;")
            n = cur.fetchone()[0]
            return {
                "engine": "sqlite",
                "keys": n,
                "db_bytes": size,
                "reads": self._reads,
                "writes": self._writes,
            }


# -----------------------------
# Replication structures
# -----------------------------

@dataclass
class Change:
    offset: int
    key: str
    value_b64: str
    ts: int

def b64_encode(b: bytes) -> str:
    import base64
    return base64.b64encode(b).decode("ascii")

def b64_decode(s: str) -> bytes:
    import base64
    return base64.b64decode(s.encode("ascii"))


class LeaderOplog:
    """
    In-memory oplog for followers:
      offset increments for each PUT.
    """
    def __init__(self, max_entries: int = 2_000_000):
        self.max_entries = max_entries
        self._lock = threading.Lock()
        self._offset = 0
        self._changes: List[Change] = []

    def append(self, key: str, value: bytes) -> int:
        with self._lock:
            self._offset += 1
            ch = Change(offset=self._offset, key=key, value_b64=b64_encode(value), ts=unix_ms())
            self._changes.append(ch)
            # trim if needed
            if len(self._changes) > self.max_entries:
                self._changes = self._changes[-self.max_entries:]
            return self._offset

    def since(self, since_offset: int, limit: int = 5000) -> Tuple[int, List[Change]]:
        with self._lock:
            # Return changes with offset > since_offset
            out = [c for c in self._changes if c.offset > since_offset]
            out = out[:limit]
            new_offset = out[-1].offset if out else since_offset
            return new_offset, out

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {"oplog_latest_offset": self._offset, "oplog_entries": len(self._changes)}


class FollowerReplicator:
    """
    Pull-based replication:
      Every interval_ms:
        GET leader /internal/changes?since=X
        apply changes to local engine
    """
    def __init__(self, leader_url: str, engine: Engine, interval_ms: int, network_timeout_s: float):
        self.leader_url = leader_url.rstrip("/")
        self.engine = engine
        self.interval_ms = max(1, interval_ms)
        self.network_timeout_s = network_timeout_s
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._offset = 0
        self._applied = 0
        self._last_err: Optional[str] = None

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=2.0)

    def _loop(self):
        while not self._stop.is_set():
            try:
                url = f"{self.leader_url}/internal/changes"
                resp = requests.get(url, params={"since": self._offset}, timeout=self.network_timeout_s)
                if resp.status_code == 200:
                    data = resp.json()
                    new_offset = int(data.get("new_offset", self._offset))
                    changes = data.get("changes", [])
                    for ch in changes:
                        key = ch["key"]
                        val = b64_decode(ch["value_b64"])
                        self.engine.put(key, val)
                        self._applied += 1
                    self._offset = new_offset
                    self._last_err = None
                else:
                    self._last_err = f"leader status {resp.status_code}"
            except Exception as e:
                self._last_err = str(e)

            self._stop.wait(self.interval_ms / 1000.0)

    def stats(self) -> Dict[str, Any]:
        return {
            "replication_offset": self._offset,
            "replication_applied": self._applied,
            "replication_interval_ms": self.interval_ms,
            "replication_last_error": self._last_err,
        }


# -----------------------------
# App setup
# -----------------------------

def build_engine(engine_name: str, data_dir: str, index_enabled: bool) -> Engine:
    if engine_name == "log":
        return LogEngine(data_dir=data_dir, index_enabled=False)
    if engine_name == "log_indexed":
        return LogEngine(data_dir=data_dir, index_enabled=index_enabled)
    if engine_name == "lsm":
        return LsmEngine(data_dir=data_dir)
    if engine_name == "sqlite":
        return SqliteEngine(data_dir=data_dir)
    raise ValueError(f"Unknown engine: {engine_name}")


def create_app(
    role: str,
    engine: Engine,
    allow_follower_writes: bool,
    leader_oplog: Optional[LeaderOplog] = None,
    follower_repl: Optional[FollowerReplicator] = None,
) -> FastAPI:
    app = FastAPI()

    @app.put("/kv/{key}")
    async def put_kv(key: str, request: Request):
        if role != "leader" and not allow_follower_writes:
            raise HTTPException(status_code=403, detail="Writes must go to leader")
        value = await request.body()

        engine.put(key, value)

        if role == "leader" and leader_oplog is not None:
            leader_oplog.append(key, value)

        return JSONResponse({"ok": True})

    @app.get("/kv/{key}")
    async def get_kv(key: str):
        v = engine.get(key)
        if v is None:
            raise HTTPException(status_code=404, detail="Key not found")
        return Response(content=v, media_type="application/octet-stream")

    @app.get("/internal/stats")
    async def internal_stats():
        out = {"role": role, "time_ms": unix_ms()}
        out.update(engine.stats())
        if role == "leader" and leader_oplog is not None:
            out.update(leader_oplog.stats())
        if role == "follower" and follower_repl is not None:
            out.update(follower_repl.stats())
        return JSONResponse(out)

    @app.get("/internal/changes")
    async def internal_changes(since: int = 0, limit: int = 5000):
        if role != "leader" or leader_oplog is None:
            raise HTTPException(status_code=403, detail="changes endpoint is leader-only")
        new_offset, changes = leader_oplog.since(since_offset=since, limit=limit)
        return JSONResponse({
            "new_offset": new_offset,
            "changes": [c.__dict__ for c in changes]
        })

    @app.post("/internal/compact")
    async def internal_compact():
        return JSONResponse(engine.compact())

    @app.post("/internal/rebuild_index")
    async def internal_rebuild_index():
        return JSONResponse(engine.rebuild_index())

    @app.post("/internal/reset")
    async def internal_reset():
        return JSONResponse(engine.reset())

    @app.post("/internal/flush")
    async def internal_flush():
        return JSONResponse(engine.flush())

    return app


# -----------------------------
# CLI
# -----------------------------

def parse_args():
    p = argparse.ArgumentParser(description="KV Store Server (leader/follower)")

    p.add_argument("--role", choices=["leader", "follower"], required=True)
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, required=True)

    p.add_argument("--engine", choices=["log", "log_indexed", "lsm", "sqlite"], default="log")
    p.add_argument("--data-dir", default="./data")
    p.add_argument("--index", action="store_true", help="Enable in-memory index (only for log_indexed)")
    p.add_argument("--allow-follower-writes", action="store_true")

    # Replication (follower only)
    p.add_argument("--leader-url", default="", help="Follower only: leader base URL, e.g. http://localhost:8000")
    p.add_argument("--repl-interval-ms", type=int, default=50, help="Follower only: poll interval")
    p.add_argument("--net-timeout", type=float, default=2.0, help="Follower only: HTTP timeout")

    # Leader oplog settings
    p.add_argument("--oplog-max", type=int, default=2_000_000)

    return p.parse_args()


def main():
    args = parse_args()

    data_dir = os.path.abspath(args.data_dir)
    ensure_dir(data_dir)

    # Separate directories per port to avoid collision if you run multiple processes
    node_dir = os.path.join(data_dir, f"{args.role}_{args.port}")
    ensure_dir(node_dir)

    engine = build_engine(args.engine, node_dir, index_enabled=args.index)

    leader_oplog = None
    follower_repl = None

    if args.role == "leader":
        leader_oplog = LeaderOplog(max_entries=args.oplog_max)

    if args.role == "follower":
        if not args.leader_url:
            raise SystemExit("Follower requires --leader-url")
        follower_repl = FollowerReplicator(
            leader_url=args.leader_url,
            engine=engine,
            interval_ms=args.repl_interval_ms,
            network_timeout_s=args.net_timeout
        )
        follower_repl.start()

    app = create_app(
        role=args.role,
        engine=engine,
        allow_follower_writes=args.allow_follower_writes,
        leader_oplog=leader_oplog,
        follower_repl=follower_repl,
    )

    print(f"Starting {args.role} on {args.host}:{args.port} | engine={args.engine} | data={node_dir}")

    try:
        uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    finally:
        if follower_repl is not None:
            follower_repl.stop()


if __name__ == "__main__":
    main()
