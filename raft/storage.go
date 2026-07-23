package raft

import "sync"

// HardState is the durable per-node Raft state (Raft paper Figure 2,
// "Persistent state on all servers": currentTerm, votedFor; the log itself is
// persisted via Append/TruncateSuffix). It must be flushed to stable storage
// before responding to RPCs (§5.1, Figure 2 note "Updated on stable storage
// before responding to RPCs") — the WAL implementation fsyncs on every
// mutation for exactly this reason.
type HardState struct {
	Term     int
	VotedFor int // -1 if none
}

// SnapshotMeta identifies the log prefix a snapshot replaces (§7).
type SnapshotMeta struct {
	Index int
	Term  int
}

// Storage is the durability interface Raft writes through. Every method must
// be durable when it returns (the file-backed implementation fsyncs; the
// in-memory implementation used by the simulator survives node "restarts"
// within a test process, modeling a disk).
//
// Raft calls these while holding its own mutex, so implementations need not
// serialize against concurrent Raft calls, only against Load.
type Storage interface {
	// SetHardState durably records term and vote.
	SetHardState(hs HardState) error
	// Append durably appends entries after the current last entry.
	Append(entries []Entry) error
	// TruncateSuffix durably discards all entries with index >= from.
	TruncateSuffix(from int) error
	// SaveSnapshot durably stores the service snapshot and compacts the log
	// prefix up through meta.Index.
	SaveSnapshot(meta SnapshotMeta, data []byte) error
	// Load returns everything needed to restart: hard state, snapshot (nil
	// data if none), and the log entries after the snapshot.
	Load() (HardState, SnapshotMeta, []byte, []Entry, error)
	// LogSizeBytes approximates the size of the un-compacted log, used by the
	// service to decide when to snapshot.
	LogSizeBytes() int
}

// MemoryStorage is a Storage backed by process memory. The simulation harness
// gives each node one MemoryStorage that persists across simulated crashes,
// which models a durable disk without real I/O.
type MemoryStorage struct {
	mu       sync.Mutex
	hs       HardState
	snapMeta SnapshotMeta
	snapshot []byte
	entries  []Entry
	logBytes int

	// failWrites simulates a full/failing disk (nemesis "disk-full"): every
	// mutation returns ErrDiskFull while set.
	failWrites bool
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{hs: HardState{Term: 0, VotedFor: -1}}
}

// SetFailWrites toggles simulated disk-write failure.
func (m *MemoryStorage) SetFailWrites(fail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failWrites = fail
}

func (m *MemoryStorage) SetHardState(hs HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failWrites {
		return ErrDiskFull
	}
	m.hs = hs
	return nil
}

func (m *MemoryStorage) Append(entries []Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failWrites {
		return ErrDiskFull
	}
	m.entries = append(m.entries, entries...)
	for _, e := range entries {
		m.logBytes += len(e.Command) + 16
	}
	return nil
}

func (m *MemoryStorage) TruncateSuffix(from int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failWrites {
		return ErrDiskFull
	}
	keep := m.entries[:0]
	for _, e := range m.entries {
		if e.Index < from {
			keep = append(keep, e)
		} else {
			m.logBytes -= len(e.Command) + 16
		}
	}
	m.entries = keep
	return nil
}

func (m *MemoryStorage) SaveSnapshot(meta SnapshotMeta, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failWrites {
		return ErrDiskFull
	}
	if meta.Index <= m.snapMeta.Index {
		return nil
	}
	m.snapMeta = meta
	m.snapshot = data
	keep := make([]Entry, 0, len(m.entries))
	bytes := 0
	for _, e := range m.entries {
		if e.Index > meta.Index {
			keep = append(keep, e)
			bytes += len(e.Command) + 16
		}
	}
	m.entries = keep
	m.logBytes = bytes
	return nil
}

func (m *MemoryStorage) Load() (HardState, SnapshotMeta, []byte, []Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries := make([]Entry, len(m.entries))
	copy(entries, m.entries)
	var snap []byte
	if m.snapshot != nil {
		snap = append([]byte(nil), m.snapshot...)
	}
	return m.hs, m.snapMeta, snap, entries, nil
}

func (m *MemoryStorage) LogSizeBytes() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.logBytes
}
