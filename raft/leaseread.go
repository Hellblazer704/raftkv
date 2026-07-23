package raft

import (
	"sort"
	"time"
)

// Lease-based linearizable reads.
//
// A leader that has heard from a majority "recently enough" knows no other
// leader can have been elected (followers refuse to vote for
// ElectionTimeoutMin after hearing from a leader), so it may serve reads
// from local state without writing to the log — the optimization described
// in Raft §8 / Ongaro's thesis §6.4.
//
// Two safety conditions, both enforced here:
//
//  1. The lease is measured from the *send* time of each acknowledged
//     AppendEntries, the conservative end of the round trip, and lasts only
//     half of ElectionTimeoutMin — leaving margin for bounded clock-rate
//     skew between nodes (the simulator deliberately runs node clocks up to
//     ±15% off).
//  2. The leader's commitIndex must cover an entry of its own term.
//     A fresh leader inherits committed entries it cannot yet *prove* are
//     committed (its commitIndex may lag the old leader's); serving reads
//     before closing that gap would return stale data. Callers hitting
//     ErrLeaseNoQuorum should fall back to a through-the-log read, which
//     both returns correct data and commits a current-term entry.

// LeaseRead returns the commit index a lease-holding leader may serve reads
// at (the caller must wait until its state machine has applied through that
// index). ok is false if this node is not leader, holds no lease, or has not
// yet committed an entry in its current term.
func (rf *Raft) LeaseRead() (readIndex int, ok bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || rf.killed() {
		return 0, false
	}
	// Condition 2: current-term entry committed.
	if rf.log.term(rf.commitIndex) != rf.currentTerm {
		return 0, false
	}
	// Condition 1: majority lease. Collect per-peer last acknowledged send
	// times; with ourselves, the (n/2)-th freshest peer completes a majority.
	if rf.n == 1 {
		return rf.commitIndex, true
	}
	times := make([]time.Time, 0, rf.n-1)
	for i := 0; i < rf.n; i++ {
		if i != rf.me {
			times = append(times, rf.leaseFrom[i])
		}
	}
	sort.Slice(times, func(a, b int) bool { return times[a].After(times[b]) })
	quorumAck := times[rf.n/2-1]
	lease := time.Duration(float64(rf.cfg.ElectionTimeoutMin) / 2)
	if time.Since(quorumAck) >= lease {
		return 0, false
	}
	return rf.commitIndex, true
}

// AppliedIndex returns how far the service has been shown committed entries.
// Used by lease-read callers to wait for readIndex without polling Raft.
func (rf *Raft) AppliedIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplied
}

// Stats is a point-in-time view of a node's consensus state, for metrics
// and operator tooling.
type Stats struct {
	Term        int
	State       string
	LeaderHint  int
	CommitIndex int
	LastApplied int
	FirstIndex  int // snapshot boundary
	LastIndex   int
	LogBytes    int
}

// Stats returns current consensus statistics.
func (rf *Raft) Stats() Stats {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	leader := rf.leaderID
	if rf.state == Leader {
		leader = rf.me
	}
	return Stats{
		Term:        rf.currentTerm,
		State:       rf.state.String(),
		LeaderHint:  leader,
		CommitIndex: rf.commitIndex,
		LastApplied: rf.lastApplied,
		FirstIndex:  rf.log.firstIndex(),
		LastIndex:   rf.log.lastIndex(),
		LogBytes:    rf.storage.LogSizeBytes(),
	}
}
