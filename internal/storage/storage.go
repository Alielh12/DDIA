package storage

import (
	"bufio"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrNotFound = errors.New("key not found")

const (
	manifestFilename = "manifest"
	walFilename      = "wal.log"
	sstPrefix        = "sst-"
	sstSuffix        = ".ldb"
	memDefaultBytes  = 1 << 20 // 1MB
)

// sstInfo stores metadata about an SSTable file.
type sstInfo struct {
	id   int64
	name string
	path string
	size int64
}

// Storage implements a minimal LSM-style engine: WAL + memtable + SSTables.
// Writes go to the WAL and memtable; when memtable exceeds threshold it's
// flushed to an immutable SSTable. Reads consult memtable, then SSTables from
// newest to oldest.

type memEntry struct {
	value   []byte
	version int64
}

// Simple in-memory metrics for instrumentation.
type metrics struct {
	mu                    sync.Mutex
	writeCount            int64
	writeTotalNs          int64
	readCount             int64
	readTotalNs           int64
	replicationCount      int64 // counts records applied
	replicationTotalLagNs int64 // cumulative lag in ns across records
	lastReplicationLagNs  int64 // last observed per-record lag
	compactionCount       int64
	compactionTotalNs     int64
}

func (m *metrics) recordWrite(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeCount++
	m.writeTotalNs += d.Nanoseconds()
}

func (m *metrics) recordRead(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readCount++
	m.readTotalNs += d.Nanoseconds()
}

func (m *metrics) recordReplication(lagNs int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replicationCount++
	m.replicationTotalLagNs += lagNs
	m.lastReplicationLagNs = lagNs
}

func (m *metrics) recordCompaction(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.compactionCount++
	m.compactionTotalNs += d.Nanoseconds()
}

// MetricsSnapshot is a snapshot view of metrics for reporting.
type MetricsSnapshot struct {
	WriteCount            int64   `json:"write_count"`
	WriteTotalNs          int64   `json:"write_total_ns"`
	WriteAvgNs            float64 `json:"write_avg_ns"`
	ReadCount             int64   `json:"read_count"`
	ReadTotalNs           int64   `json:"read_total_ns"`
	ReadAvgNs             float64 `json:"read_avg_ns"`
	ReplicationCount      int64   `json:"replication_count"`
	ReplicationTotalLagNs int64   `json:"replication_total_lag_ns"`
	ReplicationAvgLagNs   float64 `json:"replication_avg_lag_ns"`
	LastReplicationLagNs  int64   `json:"last_replication_lag_ns"`
	CompactionCount       int64   `json:"compaction_count"`
	CompactionTotalNs     int64   `json:"compaction_total_ns"`
	CompactionAvgNs       float64 `json:"compaction_avg_ns"`
}

type Storage struct {
	dir            string
	mu             sync.RWMutex
	wal            *os.File
	walPath        string
	memTable       map[string]memEntry // unsorted in-memory table with versions
	memSize        int                 // approximate size in bytes
	memThreshold   int
	ssts           []*sstInfo // ordered oldest->newest
	nextID         int64
	appliedVersion int64 // latest version applied (leader or follower)
	metrics        *metrics
}

// New creates storage, loads SSTables from manifest, replays the WAL to
// rebuild the memtable, and ensures the WAL is open for appends.
func New(dir string) (*Storage, error) {
	if dir == "" {
		return nil, fmt.Errorf("storage dir required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create storage dir: %w", err)
	}
	walPath := filepath.Join(dir, walFilename)
	wal, err := os.OpenFile(walPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	s := &Storage{
		dir:          dir,
		wal:          wal,
		walPath:      walPath,
		memTable:     make(map[string]memEntry),
		memThreshold: memDefaultBytes,
		metrics:      &metrics{},
	}
	if err := s.loadSSTables(); err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("load sstables: %w", err)
	}

	// set nextID
	var max int64 = -1
	for _, st := range s.ssts {
		if st.id > max {
			max = st.id
		}
	}
	s.nextID = max + 1

	// replay WAL into memtable (deterministic)
	if err := s.replayWAL(); err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("replay wal: %w", err)
	}

	// if memtable already exceeds threshold, flush synchronously
	if s.memSize >= s.memThreshold {
		if err := s.flushMemtable(); err != nil {
			_ = wal.Close()
			return nil, fmt.Errorf("flush memtable: %w", err)
		}
	}

	return s, nil
}

// Close closes the WAL and persists no in-memory state.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal != nil {
		if err := s.wal.Sync(); err != nil {
			_ = s.wal.Close()
			return err
		}
		return s.wal.Close()
	}
	return nil
}

// Append writes to WAL (durable) then to memtable. It returns the assigned
// version (monotonic) for the write. If the memtable exceeds the threshold
// the memtable is flushed to an SSTable synchronously.
func (s *Storage) Append(key string, value []byte) (int64, error) {
	start := time.Now()
	if key == "" {
		return 0, fmt.Errorf("key required")
	}
	if strings.ContainsAny(key, ",\n\r") {
		return 0, fmt.Errorf("key contains invalid characters")
	}

	// assign a monotonic version using time and last applied version
	s.mu.Lock()
	v := time.Now().UnixNano()
	if v <= s.appliedVersion {
		v = s.appliedVersion + 1
	}
	// update appliedVersion immediately for local writes
	s.appliedVersion = v
	s.mu.Unlock()

	enc := base64.RawStdEncoding.EncodeToString(value)
	rec := fmt.Sprintf("%d,%s,%s\n", v, key, enc)

	// write WAL (durable)
	if _, err := s.wal.WriteString(rec); err != nil {
		return 0, fmt.Errorf("write wal: %w", err)
	}
	if err := s.wal.Sync(); err != nil {
		return 0, fmt.Errorf("sync wal: %w", err)
	}

	// update memtable
	s.mu.Lock()
	defer s.mu.Unlock()
	s.memTable[key] = memEntry{value: value, version: v}
	s.memSize += len(key) + len(value)
	if s.memSize >= s.memThreshold {
		// flush synchronously
		if err := s.flushMemtable(); err != nil {
			return 0, fmt.Errorf("flush memtable: %w", err)
		}
	}
	// observe write latency (only on successful write)
	if s.metrics != nil {
		s.metrics.recordWrite(time.Since(start))
	}
	return v, nil
}

// Read checks the memtable first, then SSTables from newest to oldest.
// It returns the value and the version for the key.
func (s *Storage) Read(key string) ([]byte, int64, error) {
	start := time.Now()
	defer func() {
		if s.metrics != nil {
			s.metrics.recordRead(time.Since(start))
		}
	}()
	if key == "" {
		return nil, 0, fmt.Errorf("key required")
	}
	// memtable
	s.mu.RLock()
	if e, ok := s.memTable[key]; ok {
		v := e.value
		ver := e.version
		s.mu.RUnlock()
		return v, ver, nil
	}
	// copy sstables slice to avoid holding lock while reading files
	ssts := make([]*sstInfo, len(s.ssts))
	copy(ssts, s.ssts)
	s.mu.RUnlock()

	// search SSTables from newest to oldest
	for i := len(ssts) - 1; i >= 0; i-- {
		f, err := os.Open(ssts[i].path)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			parts := strings.SplitN(line, ",", 3)
			if len(parts) != 3 {
				continue
			}
			k := parts[0]
			if k == key {
				verStr := parts[1]
				vEnc := parts[2]
				_ = f.Close()
				ver, err := strconv.ParseInt(verStr, 10, 64)
				if err != nil {
					return nil, 0, fmt.Errorf("parse sstable version: %w", err)
				}
				val, err := base64.RawStdEncoding.DecodeString(vEnc)
				if err != nil {
					return nil, 0, fmt.Errorf("decode sstable value: %w", err)
				}
				return val, ver, nil
			}
			// since SSTable is sorted ascending by key, we can stop early
			if k > key {
				break
			}
		}
		_ = f.Close()
	}
	return nil, 0, ErrNotFound
}

// replayWAL reads the WAL and applies records to the memtable.
func (s *Storage) replayWAL() error {
	f, err := os.Open(s.walPath)
	if err != nil {
		return fmt.Errorf("open wal for replay: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		parts := strings.SplitN(line, ",", 3)
		if len(parts) != 3 {
			continue
		}
		verStr := parts[0]
		k := parts[1]
		vEnc := parts[2]
		ver, err := strconv.ParseInt(verStr, 10, 64)
		if err != nil {
			return fmt.Errorf("parse wal version: %w", err)
		}
		val, err := base64.RawStdEncoding.DecodeString(vEnc)
		if err != nil {
			return fmt.Errorf("decode wal value: %w", err)
		}
		s.memTable[k] = memEntry{value: val, version: ver}
		s.memSize += len(k) + len(val)
		if ver > s.appliedVersion {
			s.appliedVersion = ver
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("scan wal: %w", err)
	}
	return nil
}

// flushMemtable writes the memtable to an immutable SSTable and updates
// manifest atomically. It truncates the WAL after the manifest is updated.
func (s *Storage) flushMemtable() error {
	if len(s.memTable) == 0 {
		return nil
	}
	// prepare data
	keys := make([]string, 0, len(s.memTable))
	for k := range s.memTable {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	id := s.nextID
	name := fmt.Sprintf("%s%020d%s", sstPrefix, id, sstSuffix)
	tmpPath := filepath.Join(s.dir, name+".tmp")
	finalPath := filepath.Join(s.dir, name)
	wf, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("create sstable tmp: %w", err)
	}
	w := bufio.NewWriter(wf)
	var written int64 = 0
	for _, k := range keys {
		me := s.memTable[k]
		enc := base64.RawStdEncoding.EncodeToString(me.value)
		line := fmt.Sprintf("%s,%d,%s\n", k, me.version, enc)
		n, err := w.WriteString(line)
		if err != nil {
			_ = wf.Close()
			return fmt.Errorf("write sstable: %w", err)
		}
		written += int64(n)
	}
	if err := w.Flush(); err != nil {
		_ = wf.Close()
		return fmt.Errorf("flush sstable: %w", err)
	}
	if err := wf.Sync(); err != nil {
		_ = wf.Close()
		return fmt.Errorf("sync sstable: %w", err)
	}
	if err := wf.Close(); err != nil {
		return fmt.Errorf("close sstable tmp: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("rename sstable: %w", err)
	}
	if err := fsyncDir(s.dir); err != nil {
		return fmt.Errorf("fsync dir: %w", err)
	}

	// atomically update manifest to include the new sstable at the end
	s.mu.Lock()
	newSsts := append(append([]*sstInfo{}, s.ssts...), &sstInfo{id: id, name: name, path: finalPath, size: written})
	if err := writeManifestAtomic(s.dir, newSsts); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("write manifest: %w", err)
	}
	// update in-memory state
	s.ssts = newSsts
	s.nextID = id + 1
	// truncate WAL now that data is safely in SSTable and manifest
	if err := s.wal.Truncate(0); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("truncate wal: %w", err)
	}
	if _, err := s.wal.Seek(0, io.SeekStart); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("seek wal start: %w", err)
	}
	// clear memtable
	s.memTable = make(map[string]memEntry)
	s.memSize = 0
	s.mu.Unlock()
	return nil
}

// loadSSTables reads the manifest and loads SSTable metadata; if manifest is
// missing it discovers sstable files by glob.
func (s *Storage) loadSSTables() error {
	manifestPath := filepath.Join(s.dir, manifestFilename)
	if _, err := os.Stat(manifestPath); err == nil {
		b, err := os.ReadFile(manifestPath)
		if err == nil {
			lines := strings.Fields(string(b))
			var out []*sstInfo
			for _, name := range lines {
				path := filepath.Join(s.dir, name)
				st, err := os.Stat(path)
				if err != nil {
					return fmt.Errorf("manifest points to missing sstable %s: %w", name, err)
				}
				id, err := parseSSTName(name)
				if err != nil {
					return err
				}
				out = append(out, &sstInfo{id: id, name: name, path: path, size: st.Size()})
			}
			s.ssts = out
			return nil
		}
	}
	// fallback: glob
	pattern := filepath.Join(s.dir, sstPrefix+"*"+sstSuffix)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("glob sstables: %w", err)
	}
	var out []*sstInfo
	for _, p := range matches {
		_, name := filepath.Split(p)
		id, err := parseSSTName(name)
		if err != nil {
			continue
		}
		st, err := os.Stat(p)
		if err != nil {
			continue
		}
		out = append(out, &sstInfo{id: id, name: name, path: p, size: st.Size()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	s.ssts = out
	return nil
}

// writeManifestAtomic writes the list of SSTable file names to manifest
// atomically.
func writeManifestAtomic(dir string, ssts []*sstInfo) error {
	path := filepath.Join(dir, manifestFilename)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("create manifest tmp: %w", err)
	}
	w := bufio.NewWriter(f)
	for _, s := range ssts {
		if _, err := w.WriteString(s.name + "\n"); err != nil {
			_ = f.Close()
			return fmt.Errorf("write manifest: %w", err)
		}
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return fmt.Errorf("flush manifest: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync manifest: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close manifest tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename manifest: %w", err)
	}
	return fsyncDir(dir)
}

// ReplicationBatch reads the WAL starting at fromOffset (byte offset)
// and returns up to maxBytes of newline-delimited records and the new offset.
func (s *Storage) ReplicationBatch(fromOffset int64, maxBytes int) ([]string, int64, error) {
	f, err := os.Open(s.walPath)
	if err != nil {
		return nil, fromOffset, fmt.Errorf("open wal for replicate: %w", err)
	}
	defer f.Close()
	if _, err := f.Seek(fromOffset, io.SeekStart); err != nil {
		return nil, fromOffset, fmt.Errorf("seek wal: %w", err)
	}
	sc := bufio.NewScanner(f)
	var out []string
	var readBytes int
	var curOff = fromOffset
	for sc.Scan() {
		line := sc.Text()
		n := len(line) + 1 // newline
		if maxBytes > 0 && readBytes+n > maxBytes {
			break
		}
		out = append(out, line)
		readBytes += n
		curOff += int64(n)
	}
	if err := sc.Err(); err != nil {
		return nil, curOff, fmt.Errorf("scan wal: %w", err)
	}
	return out, curOff, nil
}

// ApplyReplication applies the given records (in order) as if they were
// written by the leader: it appends them to the WAL and updates the memtable.
func (s *Storage) ApplyReplication(records []string) error {
	if len(records) == 0 {
		return nil
	}
	for _, rec := range records {
		parts := strings.SplitN(rec, ",", 3)
		if len(parts) != 3 {
			continue // skip malformed
		}
		verStr := parts[0]
		k := parts[1]
		vEnc := parts[2]
		ver, err := strconv.ParseInt(verStr, 10, 64)
		if err != nil {
			return fmt.Errorf("parse replicate version: %w", err)
		}
		v, err := base64.RawStdEncoding.DecodeString(vEnc)
		if err != nil {
			return fmt.Errorf("decode replicate value: %w", err)
		}
		// append to WAL (preserve original record format)
		if _, err := s.wal.WriteString(rec + "\n"); err != nil {
			return fmt.Errorf("write wal replicate: %w", err)
		}
		if err := s.wal.Sync(); err != nil {
			return fmt.Errorf("sync wal replicate: %w", err)
		}
		// apply to memtable
		s.mu.Lock()
		s.memTable[k] = memEntry{value: v, version: ver}
		s.memSize += len(k) + len(v)
		if ver > s.appliedVersion {
			s.appliedVersion = ver
		}
		// flush if needed
		if s.memSize >= s.memThreshold {
			if err := s.flushMemtable(); err != nil {
				s.mu.Unlock()
				return fmt.Errorf("flush while applying replicate: %w", err)
			}
		}
		s.mu.Unlock()
		// measure replication lag for this record (leader version is a timestamp)
		lagNs := time.Now().UnixNano() - ver
		if lagNs < 0 {
			lagNs = 0
		}
		if s.metrics != nil {
			s.metrics.recordReplication(lagNs)
		}
	}
	return nil
}

// AppliedVersion returns the highest-applied version on this storage instance.
func (s *Storage) AppliedVersion() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.appliedVersion
}

// parseSSTName extracts the numeric id from an SSTable file name.
func parseSSTName(name string) (int64, error) {
	if !strings.HasPrefix(name, sstPrefix) || !strings.HasSuffix(name, sstSuffix) {
		return 0, fmt.Errorf("invalid sstable name %s", name)
	}
	mid := strings.TrimSuffix(strings.TrimPrefix(name, sstPrefix), sstSuffix)
	return strconv.ParseInt(mid, 10, 64)
}

// Compact merges the two oldest SSTables into one to reduce number of files.
func (s *Storage) Compact() error {
	// pick two oldest
	s.mu.RLock()
	if len(s.ssts) <= 1 {
		s.mu.RUnlock()
		return nil
	}
	left := s.ssts[0]
	right := s.ssts[1]
	s.mu.RUnlock()

	// merge left and right
	mergedID := s.nextID
	name := fmt.Sprintf("%s%020d%s", sstPrefix, mergedID, sstSuffix)
	tmp := filepath.Join(s.dir, name+".tmp")
	final := filepath.Join(s.dir, name)
	// start compaction timer
	start := time.Now()
	defer func() {
		if s.metrics != nil {
			s.metrics.recordCompaction(time.Since(start))
		}
	}()
	wf, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("create merge tmp: %w", err)
	}
	w := bufio.NewWriter(wf)

	lf, err := os.Open(left.path)
	if err != nil {
		_ = wf.Close()
		return fmt.Errorf("open left sst: %w", err)
	}
	rf, err := os.Open(right.path)
	if err != nil {
		_ = lf.Close()
		_ = wf.Close()
		return fmt.Errorf("open right sst: %w", err)
	}

	sl := bufio.NewScanner(lf)
	sr := bufio.NewScanner(rf)
	var okl, okr bool
	var lline, rline string
	// prime
	if sl.Scan() {
		okl = true
		lline = sl.Text()
	}
	if sr.Scan() {
		okr = true
		rline = sr.Text()
	}

	writeLine := func(line string) error {
		if _, err := w.WriteString(line + "\n"); err != nil {
			return err
		}
		return nil
	}

	for okl || okr {
		if okl && okr {
			// compare keys
			la := strings.SplitN(lline, ",", 2)[0]
			ra := strings.SplitN(rline, ",", 2)[0]
			if la == ra {
				// prefer right (newer)
				if err := writeLine(rline); err != nil {
					_ = lf.Close()
					_ = rf.Close()
					_ = wf.Close()
					return fmt.Errorf("write merge: %w", err)
				}
				if sl.Scan() {
					lline = sl.Text()
				} else {
					okl = false
				}
				if sr.Scan() {
					rline = sr.Text()
				} else {
					okr = false
				}
				continue
			}
			if la < ra {
				// write left
				if err := writeLine(lline); err != nil {
					_ = lf.Close()
					_ = rf.Close()
					_ = wf.Close()
					return fmt.Errorf("write merge: %w", err)
				}
				if sl.Scan() {
					lline = sl.Text()
				} else {
					okl = false
				}
				continue
			}
			// la > ra: write right
			if err := writeLine(rline); err != nil {
				_ = lf.Close()
				_ = rf.Close()
				_ = wf.Close()
				return fmt.Errorf("write merge: %w", err)
			}
			if sr.Scan() {
				rline = sr.Text()
			} else {
				okr = false
			}
			continue
		}
		if okl {
			// write lline and advance
			if err := writeLine(lline); err != nil {
				_ = lf.Close()
				_ = rf.Close()
				_ = wf.Close()
				return fmt.Errorf("write merge: %w", err)
			}
			if sl.Scan() {
				lline = sl.Text()
			} else {
				okl = false
			}
			continue
		}
		if okr {
			// write rline and advance
			if err := writeLine(rline); err != nil {
				_ = lf.Close()
				_ = rf.Close()
				_ = wf.Close()
				return fmt.Errorf("write merge: %w", err)
			}
			if sr.Scan() {
				rline = sr.Text()
			} else {
				okr = false
			}
		}
	}

	if err := w.Flush(); err != nil {
		_ = lf.Close()
		_ = rf.Close()
		_ = wf.Close()
		return fmt.Errorf("flush merge: %w", err)
	}
	if err := wf.Sync(); err != nil {
		_ = lf.Close()
		_ = rf.Close()
		_ = wf.Close()
		return fmt.Errorf("sync merge: %w", err)
	}
	_ = lf.Close()
	_ = rf.Close()
	if err := wf.Close(); err != nil {
		return fmt.Errorf("close merge tmp: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("rename merge: %w", err)
	}
	if err := fsyncDir(s.dir); err != nil {
		return fmt.Errorf("fsync dir: %w", err)
	}

	// publish new manifest replacing left and right with merged
	s.mu.Lock()
	var newSsts []*sstInfo
	newSsts = append(newSsts, &sstInfo{id: mergedID, name: name, path: final, size: 0})
	// append remaining sstables after right
	for i := 2; i < len(s.ssts); i++ {
		newSsts = append(newSsts, s.ssts[i])
	}
	if err := writeManifestAtomic(s.dir, newSsts); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("write manifest: %w", err)
	}
	// remove old files
	_ = os.Remove(left.path)
	_ = os.Remove(right.path)
	// update in-memory list and nextID
	s.ssts = newSsts
	s.nextID = mergedID + 1
	s.mu.Unlock()
	return nil
}

// fsyncDir ensures directory metadata is flushed to disk.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync dir: %w", err)
	}
	return nil
}

// MetricsSnapshot returns a snapshot of collected metrics for reporting.
func (s *Storage) MetricsSnapshot() MetricsSnapshot {
	var ms MetricsSnapshot
	if s.metrics == nil {
		return ms
	}
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()
	ms.WriteCount = s.metrics.writeCount
	ms.WriteTotalNs = s.metrics.writeTotalNs
	if s.metrics.writeCount > 0 {
		ms.WriteAvgNs = float64(s.metrics.writeTotalNs) / float64(s.metrics.writeCount)
	}
	ms.ReadCount = s.metrics.readCount
	ms.ReadTotalNs = s.metrics.readTotalNs
	if s.metrics.readCount > 0 {
		ms.ReadAvgNs = float64(s.metrics.readTotalNs) / float64(s.metrics.readCount)
	}
	ms.ReplicationCount = s.metrics.replicationCount
	ms.ReplicationTotalLagNs = s.metrics.replicationTotalLagNs
	if s.metrics.replicationCount > 0 {
		ms.ReplicationAvgLagNs = float64(s.metrics.replicationTotalLagNs) / float64(s.metrics.replicationCount)
	}
	ms.LastReplicationLagNs = s.metrics.lastReplicationLagNs
	ms.CompactionCount = s.metrics.compactionCount
	ms.CompactionTotalNs = s.metrics.compactionTotalNs
	if s.metrics.compactionCount > 0 {
		ms.CompactionAvgNs = float64(s.metrics.compactionTotalNs) / float64(s.metrics.compactionCount)
	}
	return ms
}
