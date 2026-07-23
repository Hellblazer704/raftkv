package raft

import "time"

// broadcastLocked sends AppendEntries (or InstallSnapshot for peers that have
// fallen behind the snapshot boundary) to every peer. Caller holds rf.mu.
func (rf *Raft) broadcastLocked() {
	rf.lastBroadcast = time.Now()
	for peer := 0; peer < rf.n; peer++ {
		if peer == rf.me {
			continue
		}
		go rf.replicateTo(peer, rf.currentTerm)
	}
}

// replicateTo sends one replication RPC to peer and processes the reply.
// term guards against the goroutine outliving the leadership that spawned it.
func (rf *Raft) replicateTo(peer, term int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != term || rf.killed() {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[peer] <= rf.log.firstIndex() {
		// Peer needs entries we've compacted away — ship the snapshot (§7:
		// "the leader must occasionally send snapshots to followers that lag
		// behind").
		rf.sendSnapshotLocked(peer, term) // releases rf.mu
		return
	}
	prev := rf.nextIndex[peer] - 1
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.log.term(prev),
		Entries:      rf.log.slice(prev + 1),
		LeaderCommit: rf.commitIndex,
	}
	sentAt := time.Now()
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	if !rf.transport.AppendEntries(peer, args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.stepDown(reply.Term)
		return
	}
	if rf.state != Leader || rf.currentTerm != term {
		return
	}
	// Any successful round trip in our term extends the lease basis for this
	// peer; the lease is measured from send time, the conservative bound.
	if reply.Success || reply.Term == term {
		if sentAt.After(rf.leaseFrom[peer]) {
			rf.leaseFrom[peer] = sentAt
			rf.ackTime[peer] = time.Now()
		}
	}
	if reply.Success {
		// Use args (not current state) — a stale reply must not move
		// matchIndex backward or double-count entries appended since.
		match := args.PrevLogIndex + len(args.Entries)
		if match > rf.matchIndex[peer] {
			rf.matchIndex[peer] = match
		}
		if match+1 > rf.nextIndex[peer] {
			rf.nextIndex[peer] = match + 1
		}
		rf.advanceCommitLocked()
		return
	}
	// Fast backtracking (§5.3 optimization): jump nextIndex using the
	// follower's conflict hints instead of decrementing one at a time.
	if reply.ConflictTerm == -1 {
		rf.nextIndex[peer] = reply.ConflictIndex
	} else if last := rf.log.lastIndexOfTerm(reply.ConflictTerm); last != -1 {
		// Leader has the conflict term: probe from its last entry of that term.
		rf.nextIndex[peer] = last + 1
	} else {
		// Leader never saw that term: skip the follower's whole run of it.
		rf.nextIndex[peer] = reply.ConflictIndex
	}
	if rf.nextIndex[peer] <= rf.log.firstIndex() {
		rf.nextIndex[peer] = rf.log.firstIndex() // snapshot path next round
	}
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
	// Retry immediately so a lagging follower converges in O(#terms) round
	// trips rather than waiting a heartbeat per step.
	go rf.replicateTo(peer, term)
}

// advanceCommitLocked applies the leader commit rule (§5.4.2 / Figure 2: "if
// there exists an N such that N > commitIndex, a majority of matchIndex[i] >=
// N, and log[N].term == currentTerm: set commitIndex = N"). Entries from
// earlier terms are never counted directly — they commit transitively when a
// current-term entry commits (Figure 8's lesson).
func (rf *Raft) advanceCommitLocked() {
	for n := rf.log.lastIndex(); n > rf.commitIndex && n > rf.log.firstIndex(); n-- {
		if rf.log.term(n) != rf.currentTerm {
			break // terms only decrease going down; nothing below qualifies
		}
		count := 0
		for _, m := range rf.matchIndex {
			if m >= n {
				count++
			}
		}
		if count > rf.n/2 {
			rf.commitIndex = n
			rf.logger.Debug("commit advanced", "commitIndex", n)
			rf.applyCond.Signal()
			break
		}
	}
}

// HandleAppendEntries implements the AppendEntries receiver (§5.3, Figure 2).
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	if rf.killed() {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		if !rf.stepDown(args.Term) {
			return
		}
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return // §5.1: reject stale leader
	}
	// A valid AppendEntries in our term means there is a current leader:
	// candidates step down (§5.2: "If the leader's term is at least as large
	// as the candidate's current term, then the candidate returns to
	// follower state").
	rf.state = Follower
	rf.leaderID = args.LeaderID
	rf.resetElectionTimer()

	prevIndex, prevTerm, entries := args.PrevLogIndex, args.PrevLogTerm, args.Entries

	// The prev point may predate our snapshot (a stale or duplicated RPC).
	// Everything at or before the snapshot boundary is committed, hence
	// guaranteed to match — advance the prev point past it.
	if prevIndex < rf.log.firstIndex() {
		skip := rf.log.firstIndex() - prevIndex
		if skip > len(entries) {
			// Entirely covered by our snapshot; nothing to do but report
			// success so the leader advances nextIndex.
			reply.Success = true
			return
		}
		entries = entries[skip:]
		prevIndex = rf.log.firstIndex()
		prevTerm = rf.log.term(prevIndex)
	}

	// Consistency check (§5.3: "reply false if log doesn't contain an entry
	// at prevLogIndex whose term matches prevLogTerm").
	if prevIndex > rf.log.lastIndex() {
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.log.lastIndex() + 1
		return
	}
	if rf.log.term(prevIndex) != prevTerm {
		reply.ConflictTerm = rf.log.term(prevIndex)
		reply.ConflictIndex = rf.log.firstIndexOfTerm(reply.ConflictTerm)
		if reply.ConflictIndex == -1 {
			reply.ConflictIndex = prevIndex
		}
		return
	}

	// Append new entries (§5.3: "If an existing entry conflicts with a new
	// one (same index but different terms), delete the existing entry and all
	// that follow it"). Crucially, entries that already match are left alone —
	// truncating on a duplicated/reordered old RPC would un-commit entries.
	for i, e := range entries {
		idx := prevIndex + 1 + i
		if idx <= rf.log.lastIndex() {
			if rf.log.term(idx) == e.Term {
				continue // already have it
			}
			if !rf.persistOK(rf.storage.TruncateSuffix(idx)) {
				return
			}
			rf.log.truncateSuffix(idx)
		}
		toAppend := entries[i:]
		if !rf.persistOK(rf.storage.Append(toAppend)) {
			return
		}
		rf.log.append(toAppend...)
		break
	}

	// Advance commit index (§5.3 / Figure 2: "set commitIndex =
	// min(leaderCommit, index of last new entry)").
	if args.LeaderCommit > rf.commitIndex {
		lastNew := prevIndex + len(entries)
		rf.commitIndex = min(args.LeaderCommit, lastNew)
		rf.applyCond.Signal()
	}
	reply.Success = true
}
