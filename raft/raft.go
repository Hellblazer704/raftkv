// Package raft implements the Raft consensus algorithm from the paper
// "In Search of an Understandable Consensus Algorithm (Extended Version)"
// (Ongaro & Ousterhout, 2014), with no external consensus libraries.
//
// Code comments cite paper sections (§) so the implementation can be audited
// against the paper: leader election §5.2, log replication §5.3, election
// restriction §5.4.1, commit rules §5.4.2, persistence Figure 2, log
// compaction §7.
package raft

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ErrDiskFull is returned by Storage implementations that simulate (or hit) a
// full disk. Raft treats any storage error as fatal for the node — the same
// choice etcd makes — because continuing with un-persisted state can violate
// safety after a crash.
var ErrDiskFull = errors.New("raft: storage write failed (disk full)")

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// ApplyMsg is delivered on the apply channel: either a committed command or a
// snapshot the service must restore (after a restart or an InstallSnapshot).
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

// Config tunes timers. Zero values pick production-sane defaults; the
// simulation harness compresses them and injects per-node clock skew.
type Config struct {
	// ElectionTimeoutMin/Max bound the randomized election timeout (§5.2:
	// "election timeouts are chosen randomly from a fixed interval").
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	// HeartbeatInterval is how often an idle leader broadcasts AppendEntries.
	HeartbeatInterval time.Duration
	// ClockSkew multiplies this node's election timeouts, simulating a fast
	// (<1) or slow (>1) local clock. 0 means 1.0.
	ClockSkew float64
	// Seed makes this node's randomized timeouts reproducible. 0 means seed
	// from the global RNG.
	Seed int64
	// Logger receives structured debug logs. Nil disables logging.
	Logger *slog.Logger
}

func (c Config) withDefaults(me int) Config {
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 250 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 450 * time.Millisecond
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 70 * time.Millisecond
	}
	if c.ClockSkew == 0 {
		c.ClockSkew = 1.0
	}
	if c.Seed == 0 {
		c.Seed = rand.Int63()
	}
	if c.Logger == nil {
		c.Logger = slog.New(discardHandler{})
	}
	c.Logger = c.Logger.With("node", me)
	return c
}

// Raft is a single consensus participant. All exported methods are safe for
// concurrent use.
type Raft struct {
	mu        sync.Mutex
	me        int
	n         int // cluster size; peers are ids 0..n-1
	transport Transport
	storage   Storage
	applyCh   chan ApplyMsg
	cfg       Config
	rng       *rand.Rand
	logger    *slog.Logger

	// Persistent state (Figure 2) — mirrored in storage before any RPC reply.
	currentTerm int
	votedFor    int
	log         *raftLog
	snapshot    []byte // in-memory copy of the latest snapshot, for InstallSnapshot sends

	// Volatile state (Figure 2).
	state       State
	commitIndex int
	lastApplied int
	leaderID    int // last known leader, for client redirection

	// Volatile leader state (Figure 2), reinitialized after election.
	nextIndex  []int
	matchIndex []int

	// ackTime[i] is the last local time peer i acknowledged an AppendEntries
	// from this leadership term; the basis for the leader lease (used by
	// lease-based linearizable reads).
	ackTime   []time.Time
	leaseFrom []time.Time // per-peer send times of in-flight probes

	electionDeadline time.Time
	lastBroadcast    time.Time

	applyCond       *sync.Cond
	pendingSnapshot *ApplyMsg

	dead   atomic.Bool
	killCh chan struct{}
}

// Make creates and starts a Raft node. me is this node's id in [0, n).
// Committed entries and snapshots are delivered on applyCh in log order.
func Make(me, n int, transport Transport, storage Storage, applyCh chan ApplyMsg, cfg Config) *Raft {
	cfg = cfg.withDefaults(me)
	rf := &Raft{
		me:        me,
		n:         n,
		transport: transport,
		storage:   storage,
		applyCh:   applyCh,
		cfg:       cfg,
		rng:       rand.New(rand.NewSource(cfg.Seed)),
		logger:    cfg.Logger,
		votedFor:  -1,
		log:       newLog(),
		state:     Follower,
		leaderID:  -1,
		killCh:    make(chan struct{}),
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	// Recover from stable storage (crash restart, §5.1: servers restart from
	// persistent state).
	hs, snapMeta, snapData, entries, err := storage.Load()
	if err != nil {
		panic("raft: cannot load storage: " + err.Error())
	}
	rf.currentTerm = hs.Term
	rf.votedFor = hs.VotedFor
	if snapMeta.Index > 0 {
		rf.log = &raftLog{entries: []Entry{{Index: snapMeta.Index, Term: snapMeta.Term}}}
		rf.snapshot = snapData
		rf.commitIndex = snapMeta.Index
		rf.lastApplied = snapMeta.Index
		if len(snapData) > 0 {
			rf.pendingSnapshot = &ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapData,
				SnapshotIndex: snapMeta.Index,
				SnapshotTerm:  snapMeta.Term,
			}
		}
	}
	rf.log.append(entries...)

	rf.resetElectionTimer()
	go rf.ticker()
	go rf.applier()
	return rf
}

// Start proposes a command. If this node is the leader it appends the command
// to its log and begins replication, returning the entry's index and term;
// otherwise isLeader is false. Commitment is not guaranteed (§5.3) — the
// caller learns the outcome from the apply channel.
func (rf *Raft) Start(command []byte) (index, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || rf.killed() {
		return -1, rf.currentTerm, false
	}
	index = rf.log.lastIndex() + 1
	term = rf.currentTerm
	e := Entry{Index: index, Term: term, Command: command}
	if !rf.persistOK(rf.storage.Append([]Entry{e})) {
		return -1, term, false
	}
	rf.log.append(e)
	rf.matchIndex[rf.me] = index
	rf.logger.Debug("start", "index", index, "term", term)
	rf.broadcastLocked()
	return index, term, true
}

// GetState returns this node's current term and whether it believes it is the
// leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// LeaderHint returns the id of the last observed leader, or -1.
func (rf *Raft) LeaderHint() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		return rf.me
	}
	return rf.leaderID
}

// LogSizeBytes reports the approximate size of the un-compacted log so the
// service can decide when to snapshot.
func (rf *Raft) LogSizeBytes() int { return rf.storage.LogSizeBytes() }

// Kill shuts the node down. Safe to call more than once.
func (rf *Raft) Kill() {
	if rf.dead.CompareAndSwap(false, true) {
		close(rf.killCh)
	}
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool { return rf.dead.Load() }

// persistOK checks a storage write. On failure the node halts (storage errors
// are unrecoverable without operator intervention; see ErrDiskFull) and the
// caller must abandon the state transition it was making.
func (rf *Raft) persistOK(err error) bool {
	if err == nil {
		return true
	}
	rf.logger.Error("storage write failed; halting node", "err", err)
	if rf.dead.CompareAndSwap(false, true) {
		close(rf.killCh)
	}
	rf.applyCond.Broadcast()
	return false
}

func (rf *Raft) persistHardState() bool {
	return rf.persistOK(rf.storage.SetHardState(HardState{Term: rf.currentTerm, VotedFor: rf.votedFor}))
}

// resetElectionTimer randomizes the next election deadline (§5.2), scaled by
// this node's simulated clock skew.
func (rf *Raft) resetElectionTimer() {
	span := rf.cfg.ElectionTimeoutMax - rf.cfg.ElectionTimeoutMin
	d := rf.cfg.ElectionTimeoutMin + time.Duration(rf.rng.Int63n(int64(span)+1))
	d = time.Duration(float64(d) * rf.cfg.ClockSkew)
	rf.electionDeadline = time.Now().Add(d)
}

// stepDown transitions to follower for a newer term (§5.1: "If a server
// receives a request with a stale term number, it rejects the request... if
// its own term is smaller, it updates its term").
func (rf *Raft) stepDown(term int) bool {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = Follower
	return rf.persistHardState()
}

// ticker drives election timeouts and leader heartbeats.
func (rf *Raft) ticker() {
	const tick = 10 * time.Millisecond
	for !rf.killed() {
		time.Sleep(tick)
		rf.mu.Lock()
		switch rf.state {
		case Leader:
			if time.Since(rf.lastBroadcast) >= rf.cfg.HeartbeatInterval {
				rf.broadcastLocked()
			}
		default:
			if time.Now().After(rf.electionDeadline) {
				rf.startElectionLocked()
			}
		}
		rf.mu.Unlock()
	}
}

// applier delivers committed entries (and snapshots) to the service in order.
// It never holds rf.mu across a channel send, so the service is free to call
// back into Raft (e.g. Snapshot) from its apply loop.
func (rf *Raft) applier() {
	rf.mu.Lock()
	for !rf.killed() {
		if rf.pendingSnapshot != nil {
			msg := *rf.pendingSnapshot
			rf.pendingSnapshot = nil
			if rf.lastApplied < msg.SnapshotIndex {
				rf.lastApplied = msg.SnapshotIndex
			}
			rf.mu.Unlock()
			select {
			case rf.applyCh <- msg:
			case <-rf.killCh:
				return
			}
			rf.mu.Lock()
			continue
		}
		if rf.lastApplied < rf.commitIndex {
			if rf.lastApplied < rf.log.firstIndex() {
				// A snapshot overtook these entries; the pendingSnapshot
				// branch above (or the restart path) covers the gap.
				rf.lastApplied = rf.log.firstIndex()
				continue
			}
			rf.lastApplied++
			e := rf.log.entry(rf.lastApplied)
			msg := ApplyMsg{CommandValid: true, Command: e.Command, CommandIndex: e.Index, CommandTerm: e.Term}
			rf.mu.Unlock()
			select {
			case rf.applyCh <- msg:
			case <-rf.killCh:
				return
			}
			rf.mu.Lock()
			continue
		}
		rf.applyCond.Wait()
	}
	rf.mu.Unlock()
}

// discardHandler is a no-op slog.Handler (slog.DiscardHandler arrived in Go
// 1.24; we target 1.22).
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (discardHandler) WithAttrs([]slog.Attr) slog.Handler        { return discardHandler{} }
func (discardHandler) WithGroup(string) slog.Handler             { return discardHandler{} }
