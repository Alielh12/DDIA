import os
import time
import base64
import json
import threading
import tempfile
from typing import Dict, Tuple, List

ErrNotFound = Exception("key not found")

MEM_DEFAULT_BYTES = 1 << 20
MANIFEST = "manifest"
WAL = "wal.log"
SST_PREFIX = "sst-"
SST_SUFFIX = ".ldb"

class MemEntry:
    def __init__(self, value: bytes, version: int):
        self.value = value
        self.version = version

class Metrics:
    def __init__(self):
        self.mu = threading.Lock()
        self.write_count = 0
        self.write_total_ns = 0
        self.read_count = 0
        self.read_total_ns = 0
        self.replication_count = 0
        self.replication_total_lag_ns = 0
        self.last_replication_lag_ns = 0
        self.compaction_count = 0
        self.compaction_total_ns = 0

    def record_write(self, ns):
        with self.mu:
            self.write_count += 1
            self.write_total_ns += ns

    def record_read(self, ns):
        with self.mu:
            self.read_count += 1
            self.read_total_ns += ns

    def record_replication(self, lag_ns):
        with self.mu:
            self.replication_count += 1
            self.replication_total_lag_ns += lag_ns
            self.last_replication_lag_ns = lag_ns

    def record_compaction(self, ns):
        with self.mu:
            self.compaction_count += 1
            self.compaction_total_ns += ns

    def snapshot(self):
        with self.mu:
            ms = {
                "write_count": self.write_count,
                "write_total_ns": self.write_total_ns,
                "write_avg_ns": (self.write_total_ns / self.write_count) if self.write_count else 0,
                "read_count": self.read_count,
                "read_total_ns": self.read_total_ns,
                "read_avg_ns": (self.read_total_ns / self.read_count) if self.read_count else 0,
                "replication_count": self.replication_count,
                "replication_total_lag_ns": self.replication_total_lag_ns,
                "replication_avg_lag_ns": (self.replication_total_lag_ns / self.replication_count) if self.replication_count else 0,
                "last_replication_lag_ns": self.last_replication_lag_ns,
                "compaction_count": self.compaction_count,
                "compaction_total_ns": self.compaction_total_ns,
                "compaction_avg_ns": (self.compaction_total_ns / self.compaction_count) if self.compaction_count else 0,
            }
            return ms

class Storage:
    def __init__(self, dirpath: str, mem_threshold: int = MEM_DEFAULT_BYTES):
        if not dirpath:
            raise ValueError("storage dir required")
        os.makedirs(dirpath, exist_ok=True)
        self.dir = dirpath
        self.wal_path = os.path.join(dirpath, WAL)
        self.wal = open(self.wal_path, "a+", buffering=1)  # line buffered
        self.lock = threading.RLock()
        self.mem_table: Dict[str, MemEntry] = {}
        self.mem_size = 0
        self.mem_threshold = mem_threshold
        self.ssts: List[str] = []  # list of sst file names, oldest->newest
        self.next_id = 0
        self.applied_version = 0
        self.metrics = Metrics()
        self._load_ssts()
        self._replay_wal()

    def close(self):
        with self.lock:
            try:
                self.wal.flush()
                os.fsync(self.wal.fileno())
            except Exception:
                pass
            try:
                self.wal.close()
            except Exception:
                pass

    def _load_ssts(self):
        manifest_path = os.path.join(self.dir, MANIFEST)
        if os.path.exists(manifest_path):
            with open(manifest_path, "r") as f:
                lines = [l.strip() for l in f if l.strip()]
                self.ssts = lines
                maxid = -1
                for name in self.ssts:
                    try:
                        idnum = int(name[len(SST_PREFIX):-len(SST_SUFFIX)])
                        if idnum > maxid:
                            maxid = idnum
                    except Exception:
                        pass
                self.next_id = maxid + 1
        else:
            # discover
            files = [f for f in os.listdir(self.dir) if f.startswith(SST_PREFIX) and f.endswith(SST_SUFFIX)]
            files.sort()
            self.ssts = files
            maxid = -1
            for name in self.ssts:
                try:
                    idnum = int(name[len(SST_PREFIX):-len(SST_SUFFIX)])
                    if idnum > maxid:
                        maxid = idnum
                except Exception:
                    pass
            self.next_id = maxid + 1

    def _replay_wal(self):
        def b64decode_raw(s: str) -> bytes:
            # Accept both padded and unpadded base64 (RawStd from Go may be unpadded)
            if not s:
                return b""
            pad = -len(s) % 4
            if pad:
                s = s + ("=" * pad)
            return base64.b64decode(s)

        try:
            with open(self.wal_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split(",", 2)
                    if len(parts) != 3:
                        continue
                    ver = int(parts[0])
                    k = parts[1]
                    v = b64decode_raw(parts[2])
                    self.mem_table[k] = MemEntry(v, ver)
                    self.mem_size += len(k) + len(v)
                    if ver > self.applied_version:
                        self.applied_version = ver
        except FileNotFoundError:
            pass

    def _fsync_dir(self):
        fd = os.open(self.dir, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _write_manifest(self):
        tmp = os.path.join(self.dir, MANIFEST + ".tmp")
        with open(tmp, "w") as f:
            for name in self.ssts:
                f.write(name + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, os.path.join(self.dir, MANIFEST))
        self._fsync_dir()

    def append(self, key: str, value: bytes) -> int:
        if not key:
            raise ValueError("key required")
        if any(c in key for c in ",\n\r"):
            raise ValueError("invalid key")
        start = time.time_ns()
        with self.lock:
            v = time.time_ns()
            if v <= self.applied_version:
                v = self.applied_version + 1
            self.applied_version = v
            # use unpadded base64 (RawStd) to be compatible with the Go WAL/SST format
            enc = base64.b64encode(value).decode('ascii').rstrip('=')
            rec = f"{v},{key},{enc}\n"
            self.wal.write(rec)
            self.wal.flush()
            os.fsync(self.wal.fileno())
            self.mem_table[key] = MemEntry(value, v)
            self.mem_size += len(key) + len(value)
            if self.mem_size >= self.mem_threshold:
                self._flush_memtable()
        self.metrics.record_write(time.time_ns() - start)
        return v

    def read(self, key: str) -> Tuple[bytes, int]:
        start = time.time_ns()
        try:
            with self.lock:
                if key in self.mem_table:
                    e = self.mem_table[key]
                    return e.value, e.version
                # search sstables newest->oldest
                for name in reversed(self.ssts):
                    path = os.path.join(self.dir, name)
                    try:
                        with open(path, "r") as f:
                            for line in f:
                                line = line.strip()
                                if not line:
                                    continue
                                parts = line.split(",", 2)
                                if len(parts) != 3:
                                    continue
                                k = parts[0]
                                if k == key:
                                    ver = int(parts[1])
                                    # decode possibly-unpadded base64
                                    pad = -len(parts[2]) % 4
                                    valenc = parts[2] + ("=" * pad) if pad else parts[2]
                                    val = base64.b64decode(valenc)
                                    return val, ver
                                if k > key:
                                    break
                    except FileNotFoundError:
                        continue
            raise ErrNotFound
        finally:
            self.metrics.record_read(time.time_ns() - start)

    def _flush_memtable(self):
        if not self.mem_table:
            return
        keys = sorted(self.mem_table.keys())
        idnum = self.next_id
        name = f"{SST_PREFIX}{idnum:020d}{SST_SUFFIX}"
        tmpfd, tmppath = tempfile.mkstemp(dir=self.dir)
        with os.fdopen(tmpfd, "w") as wf:
            for k in keys:
                me = self.mem_table[k]
                enc = base64.b64encode(me.value).decode('ascii').rstrip('=')
                wf.write(f"{k},{me.version},{enc}\n")
            wf.flush()
            os.fsync(wf.fileno())
        final = os.path.join(self.dir, name)
        os.replace(tmppath, final)
        self.ssts.append(name)
        self._write_manifest()
        # truncate WAL
        with open(self.wal_path, "w") as f:
            f.truncate(0)
        self.wal = open(self.wal_path, "a+", buffering=1)
        self.mem_table = {}
        self.mem_size = 0
        self.next_id += 1

    def replication_batch(self, from_offset: int, max_bytes: int) -> Tuple[List[str], int]:
        out = []
        cur = from_offset
        try:
            with open(self.wal_path, "r") as f:
                f.seek(from_offset)
                while True:
                    line = f.readline()
                    if not line:
                        break
                    n = len(line)
                    if max_bytes > 0 and (sum(len(l) + 1 for l in out) + n) > max_bytes:
                        break
                    out.append(line.strip())
                    cur += n
        except FileNotFoundError:
            pass
        return out, cur

    def apply_replication(self, records: List[str]):
        for rec in records:
            parts = rec.split(",", 2)
            if len(parts) != 3:
                continue
            ver = int(parts[0])
            k = parts[1]
            # decode possibly-unpadded base64
            pad = -len(parts[2]) % 4
            valenc = parts[2] + ("=" * pad) if pad else parts[2]
            val = base64.b64decode(valenc)
            with self.lock:
                self.wal.write(rec + "\n")
                self.wal.flush()
                os.fsync(self.wal.fileno())
                self.mem_table[k] = MemEntry(val, ver)
                self.mem_size += len(k) + len(val)
                if ver > self.applied_version:
                    self.applied_version = ver
                if self.mem_size >= self.mem_threshold:
                    self._flush_memtable()
            # record replication lag
            lag = time.time_ns() - ver
            if lag < 0:
                lag = 0
            self.metrics.record_replication(lag)

    def compact(self):
        with self.lock:
            if len(self.ssts) <= 1:
                return
            # merge two oldest
            left = self.ssts.pop(0)
            right = self.ssts.pop(0)
            start = time.time_ns()
            left_path = os.path.join(self.dir, left)
            right_path = os.path.join(self.dir, right)
            merged = f"{SST_PREFIX}{self.next_id:020d}{SST_SUFFIX}"
            tmpfd, tmppath = tempfile.mkstemp(dir=self.dir)
            with os.fdopen(tmpfd, "w") as wf, open(left_path) as lf, open(right_path) as rf:
                llines = [l.strip() for l in lf if l.strip()]
                rlines = [l.strip() for l in rf if l.strip()]
                i, j = 0, 0
                while i < len(llines) or j < len(rlines):
                    if i < len(llines) and j < len(rlines):
                        la = llines[i].split(",", 1)[0]
                        ra = rlines[j].split(",", 1)[0]
                        if la == ra:
                            # prefer right (newer)
                            wf.write(rlines[j] + "\n")
                            i += 1
                            j += 1
                        elif la < ra:
                            wf.write(llines[i] + "\n")
                            i += 1
                        else:
                            wf.write(rlines[j] + "\n")
                            j += 1
                    elif i < len(llines):
                        wf.write(llines[i] + "\n")
                        i += 1
                    else:
                        wf.write(rlines[j] + "\n")
                        j += 1
                wf.flush()
                os.fsync(wf.fileno())
            final = os.path.join(self.dir, merged)
            os.replace(tmppath, final)
            # remove old
            try:
                os.remove(left_path)
            except Exception:
                pass
            try:
                os.remove(right_path)
            except Exception:
                pass
            self.ssts.insert(0, merged)
            self._write_manifest()
            self.next_id += 1
            self.metrics.record_compaction(time.time_ns() - start)

    def applied_version_snapshot(self) -> int:
        with self.lock:
            return self.applied_version

    def metrics_snapshot(self) -> dict:
        return self.metrics.snapshot()
