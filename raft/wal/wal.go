// Package wal is a file-backed, fsync-on-every-mutation implementation of
// raft.Storage. Raft's safety argument assumes term/vote/log mutations reach
// stable storage before any RPC response (paper Figure 2: "Updated on stable
// storage before responding to RPCs"), so every method here syncs before
// returning.
//
// Layout (one directory per node):
//
//	wal.log   — append-only records: [len u32][crc32c u32][gob payload]
//	snapshot  — gob({Meta, Data}), replaced atomically via tmp+rename
//
// Recovery accepts any valid record prefix of wal.log: a torn tail (crash
// mid-write) is detected by length/CRC and truncated away, which is safe
// because a record is only acknowledged after fsync.
package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/Hellblazer704/raftkv/raft"
)

const (
	recHardState = 1 // durable currentTerm/votedFor update
	recAppend    = 2 // entries appended to the log tail
	recTruncate  = 3 // suffix truncation (conflict resolution)
	recLogStart  = 4 // base marker written when the WAL is rewritten after compaction
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type record struct {
	Type    uint8
	HS      raft.HardState
	Entries []raft.Entry
	From    int
	Meta    raft.SnapshotMeta
}

type snapshotFile struct {
	Meta raft.SnapshotMeta
	Data []byte
}

// WAL implements raft.Storage on the local filesystem.
type WAL struct {
	mu  sync.Mutex
	dir string
	f   *os.File

	// In-memory mirror of durable state, used for compaction rewrites and to
	// serve Load without re-reading files.
	hs       raft.HardState
	snapMeta raft.SnapshotMeta
	snapData []byte
	entries  []raft.Entry
	logBytes int
}

// Open creates or recovers a WAL in dir.
func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &WAL{dir: dir, hs: raft.HardState{Term: 0, VotedFor: -1}}
	if err := w.loadSnapshotFile(); err != nil {
		return nil, err
	}
	if err := w.replay(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(w.walPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	w.f = f
	return w, nil
}

func (w *WAL) walPath() string  { return filepath.Join(w.dir, "wal.log") }
func (w *WAL) snapPath() string { return filepath.Join(w.dir, "snapshot") }

func (w *WAL) loadSnapshotFile() error {
	f, err := os.Open(w.snapPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	var sf snapshotFile
	if err := gob.NewDecoder(f).Decode(&sf); err != nil {
		return fmt.Errorf("wal: corrupt snapshot file: %w", err)
	}
	w.snapMeta = sf.Meta
	w.snapData = sf.Data
	return nil
}

// replay rebuilds the in-memory mirror from wal.log, truncating a torn tail.
func (w *WAL) replay() error {
	data, err := os.ReadFile(w.walPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	valid := 0
	for off := 0; off+8 <= len(data); {
		n := int(binary.LittleEndian.Uint32(data[off:]))
		crc := binary.LittleEndian.Uint32(data[off+4:])
		if off+8+n > len(data) {
			break // torn tail
		}
		payload := data[off+8 : off+8+n]
		if crc32.Checksum(payload, crcTable) != crc {
			break // torn or corrupt tail
		}
		var rec record
		if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&rec); err != nil {
			break
		}
		w.applyRecord(&rec)
		off += 8 + n
		valid = off
	}
	if valid < len(data) {
		if err := os.Truncate(w.walPath(), int64(valid)); err != nil {
			return err
		}
	}
	// A snapshot file newer than the WAL base (crash between snapshot write
	// and WAL rewrite) supersedes the covered prefix.
	w.dropCoveredEntries()
	return nil
}

func (w *WAL) applyRecord(rec *record) {
	switch rec.Type {
	case recHardState:
		w.hs = rec.HS
	case recAppend:
		w.entries = append(w.entries, rec.Entries...)
	case recTruncate:
		keep := w.entries[:0]
		for _, e := range w.entries {
			if e.Index < rec.From {
				keep = append(keep, e)
			}
		}
		w.entries = keep
	case recLogStart:
		if rec.Meta.Index > w.snapMeta.Index {
			w.snapMeta = rec.Meta
		}
		w.entries = nil
	}
}

func (w *WAL) dropCoveredEntries() {
	keep := w.entries[:0]
	bytes := 0
	for _, e := range w.entries {
		if e.Index > w.snapMeta.Index {
			keep = append(keep, e)
			bytes += len(e.Command) + 16
		}
	}
	w.entries = keep
	w.logBytes = bytes
}

// appendRecord writes one record and fsyncs.
func (w *WAL) appendRecord(rec *record) error {
	payload, err := encodeRecord(rec)
	if err != nil {
		return err
	}
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:], crc32.Checksum(payload, crcTable))
	if _, err := w.f.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.f.Write(payload); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *WAL) SetHardState(hs raft.HardState) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.appendRecord(&record{Type: recHardState, HS: hs}); err != nil {
		return err
	}
	w.hs = hs
	return nil
}

func (w *WAL) Append(entries []raft.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(entries) == 0 {
		return nil
	}
	if err := w.appendRecord(&record{Type: recAppend, Entries: entries}); err != nil {
		return err
	}
	w.entries = append(w.entries, entries...)
	for _, e := range entries {
		w.logBytes += len(e.Command) + 16
	}
	return nil
}

func (w *WAL) TruncateSuffix(from int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.appendRecord(&record{Type: recTruncate, From: from}); err != nil {
		return err
	}
	keep := w.entries[:0]
	bytes := 0
	for _, e := range w.entries {
		if e.Index < from {
			keep = append(keep, e)
			bytes += len(e.Command) + 16
		}
	}
	w.entries = keep
	w.logBytes = bytes
	return nil
}

// SaveSnapshot atomically persists the snapshot, then rewrites the WAL to
// contain only the un-compacted suffix. Order matters for crash safety:
// snapshot first, so a crash in between leaves a WAL whose covered prefix is
// simply dropped on recovery (see replay).
func (w *WAL) SaveSnapshot(meta raft.SnapshotMeta, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if meta.Index <= w.snapMeta.Index {
		return nil
	}
	if err := w.writeSnapshotFile(snapshotFile{Meta: meta, Data: data}); err != nil {
		return err
	}
	w.snapMeta = meta
	w.snapData = data
	w.dropCoveredEntries()
	return w.rewriteWAL()
}

func (w *WAL) writeSnapshotFile(sf snapshotFile) error {
	tmp := w.snapPath() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := gob.NewEncoder(f).Encode(&sf); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, w.snapPath())
}

// rewriteWAL replaces wal.log with a compact base: logstart + hardstate +
// remaining entries.
func (w *WAL) rewriteWAL() error {
	tmp := w.walPath() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	old := w.f
	w.f = f
	if err := w.appendRecord(&record{Type: recLogStart, Meta: w.snapMeta}); err != nil {
		w.f = old
		f.Close()
		return err
	}
	if err := w.appendRecord(&record{Type: recHardState, HS: w.hs}); err != nil {
		w.f = old
		f.Close()
		return err
	}
	if len(w.entries) > 0 {
		if err := w.appendRecord(&record{Type: recAppend, Entries: w.entries}); err != nil {
			w.f = old
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		w.f = old
		return err
	}
	if old != nil {
		old.Close()
	}
	if err := os.Rename(tmp, w.walPath()); err != nil {
		w.f = nil
		return err
	}
	nf, err := os.OpenFile(w.walPath(), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		w.f = nil
		return err
	}
	w.f = nf
	return nil
}

func (w *WAL) Load() (raft.HardState, raft.SnapshotMeta, []byte, []raft.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	entries := make([]raft.Entry, len(w.entries))
	copy(entries, w.entries)
	var snap []byte
	if w.snapData != nil {
		snap = append([]byte(nil), w.snapData...)
	}
	return w.hs, w.snapMeta, snap, entries, nil
}

func (w *WAL) LogSizeBytes() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.logBytes
}

// Close releases the WAL file handle.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f != nil {
		return w.f.Close()
	}
	return nil
}

func encodeRecord(rec *record) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(rec); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
