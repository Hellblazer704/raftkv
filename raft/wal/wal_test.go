package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Hellblazer704/raftkv/raft"
)

func entry(index, term int, cmd string) raft.Entry {
	return raft.Entry{Index: index, Term: term, Command: []byte(cmd)}
}

// TestLogSizeBytes checks the accounting used by the snapshot trigger:
// appends grow it, truncation and compaction shrink it, and the value
// survives a reopen.
func TestLogSizeBytes(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := w.LogSizeBytes(); got != 0 {
		t.Fatalf("empty log size %d, want 0", got)
	}
	if err := w.Append([]raft.Entry{entry(1, 1, "aaaa"), entry(2, 1, "bbbb")}); err != nil {
		t.Fatal(err)
	}
	afterAppend := w.LogSizeBytes()
	if afterAppend <= 0 {
		t.Fatalf("size after append %d, want > 0", afterAppend)
	}
	if err := w.TruncateSuffix(2); err != nil {
		t.Fatal(err)
	}
	if got := w.LogSizeBytes(); got >= afterAppend {
		t.Fatalf("size after truncate %d, want < %d", got, afterAppend)
	}
	w.Close()

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if got := w2.LogSizeBytes(); got <= 0 {
		t.Fatalf("size after reopen %d, want > 0", got)
	}
}

// TestRepeatedSnapshots exercises the snapshot-then-WAL-rewrite path many
// times, with post-compaction appends between each, and verifies the log
// survives a reopen after the whole sequence — the rewriteWAL /
// writeSnapshotFile loop that a single snapshot doesn't fully cover.
func TestRepeatedSnapshots(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	next := 1
	for round := 0; round < 5; round++ {
		var batch []raft.Entry
		for i := 0; i < 10; i++ {
			batch = append(batch, entry(next, round+1, "payload"))
			next++
		}
		if err := w.Append(batch); err != nil {
			t.Fatal(err)
		}
		snapIndex := next - 5 // leave a live suffix after each snapshot
		if err := w.SaveSnapshot(raft.SnapshotMeta{Index: snapIndex, Term: round + 1}, []byte("snap")); err != nil {
			t.Fatal(err)
		}
		// A stale snapshot (index <= current) must be a no-op, not corrupt.
		if err := w.SaveSnapshot(raft.SnapshotMeta{Index: snapIndex - 1, Term: round + 1}, []byte("stale")); err != nil {
			t.Fatal(err)
		}
	}
	lastIndex := next - 1
	w.Close()

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	_, meta, snap, entries, _ := w2.Load()
	if string(snap) != "snap" {
		t.Fatalf("snapshot data %q, want \"snap\"", snap)
	}
	if len(entries) == 0 || entries[len(entries)-1].Index != lastIndex {
		t.Fatalf("last entry index = %v, want %d", entries, lastIndex)
	}
	if entries[0].Index != meta.Index+1 {
		t.Fatalf("first live entry %d, want %d (snapshot boundary %d)", entries[0].Index, meta.Index+1, meta.Index)
	}
}

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.SetHardState(raft.HardState{Term: 3, VotedFor: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]raft.Entry{entry(1, 1, "a"), entry(2, 3, "b")}); err != nil {
		t.Fatal(err)
	}
	if err := w.TruncateSuffix(2); err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]raft.Entry{entry(2, 3, "c")}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	hs, meta, snap, entries, err := w2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hs.Term != 3 || hs.VotedFor != 1 {
		t.Fatalf("hard state %+v", hs)
	}
	if meta.Index != 0 || snap != nil {
		t.Fatalf("unexpected snapshot %+v", meta)
	}
	if len(entries) != 2 || string(entries[1].Command) != "c" {
		t.Fatalf("entries %+v", entries)
	}
}

func TestSnapshotCompaction(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	var es []raft.Entry
	for i := 1; i <= 10; i++ {
		es = append(es, entry(i, 1, "x"))
	}
	if err := w.Append(es); err != nil {
		t.Fatal(err)
	}
	if err := w.SaveSnapshot(raft.SnapshotMeta{Index: 7, Term: 1}, []byte("snapdata")); err != nil {
		t.Fatal(err)
	}
	// Post-compaction appends must survive a reopen too.
	if err := w.Append([]raft.Entry{entry(11, 2, "y")}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	_, meta, snap, entries, _ := w2.Load()
	if meta.Index != 7 || string(snap) != "snapdata" {
		t.Fatalf("snapshot %+v %q", meta, snap)
	}
	if len(entries) != 4 || entries[0].Index != 8 || entries[3].Index != 11 {
		t.Fatalf("entries %+v", entries)
	}
}

func TestTornTailTruncated(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]raft.Entry{entry(1, 1, "keep")}); err != nil {
		t.Fatal(err)
	}
	if err := w.SetHardState(raft.HardState{Term: 2, VotedFor: 0}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	// Simulate a crash mid-write: chop bytes off the tail.
	path := filepath.Join(dir, "wal.log")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data[:len(data)-3], 0o644); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	hs, _, _, entries, _ := w2.Load()
	if len(entries) != 1 || string(entries[0].Command) != "keep" {
		t.Fatalf("entries %+v", entries)
	}
	// The torn hard-state record must be gone entirely, not half-applied.
	if hs.Term != 0 {
		t.Fatalf("torn record applied: %+v", hs)
	}
	// And the WAL must accept new writes after recovery.
	if err := w2.Append([]raft.Entry{entry(2, 2, "after")}); err != nil {
		t.Fatal(err)
	}
}

func TestCorruptRecordStopsReplay(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]raft.Entry{entry(1, 1, "good")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]raft.Entry{entry(2, 1, "bad")}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	// Flip a byte inside the last record's payload: CRC must catch it.
	path := filepath.Join(dir, "wal.log")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-2] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	_, _, _, entries, _ := w2.Load()
	if len(entries) != 1 || string(entries[0].Command) != "good" {
		t.Fatalf("entries after corruption: %+v", entries)
	}
}
