package raft

// Log compaction (Raft paper §7): the service snapshots its own state and
// tells Raft, which discards the covered log prefix; leaders ship the
// snapshot to followers that have fallen behind the boundary.

// Snapshot is called by the service when it has serialized its state up
// through (and including) index. Raft compacts its log and persists the
// snapshot (§7: "each server takes snapshots independently, covering just
// the committed entries in its log").
func (rf *Raft) Snapshot(index int, data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.log.firstIndex() || index > rf.log.lastIndex() {
		return // already compacted, or covering un-held entries (stale call)
	}
	term := rf.log.term(index)
	if !rf.persistOK(rf.storage.SaveSnapshot(SnapshotMeta{Index: index, Term: term}, data)) {
		return
	}
	rf.log.compact(index, term)
	rf.snapshot = data
	rf.logger.Debug("snapshot taken", "index", index, "term", term)
}

// sendSnapshotLocked ships the current snapshot to a lagging peer. Caller
// holds rf.mu; this releases it.
func (rf *Raft) sendSnapshotLocked(peer, term int) {
	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.log.firstIndex(),
		LastIncludedTerm:  rf.log.term(rf.log.firstIndex()),
		Data:              rf.snapshot,
	}
	rf.mu.Unlock()

	reply := &InstallSnapshotReply{}
	if !rf.transport.InstallSnapshot(peer, args, reply) {
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
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
	if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
}

// HandleInstallSnapshot implements the InstallSnapshot receiver (§7).
func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
		return
	}
	rf.state = Follower
	rf.leaderID = args.LeaderID
	rf.resetElectionTimer()

	// Stale snapshot: we've already committed past it. Applying it would
	// rewind the service's state machine.
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	// §7: "if the follower has an entry that matches the snapshot's last
	// included index and term, it retains the log entries following it";
	// otherwise the entire log is discarded. compact() implements both in
	// memory; storage must make the same call, so decide once here.
	keepSuffix := args.LastIncludedIndex <= rf.log.lastIndex() &&
		rf.log.term(args.LastIncludedIndex) == args.LastIncludedTerm
	if !rf.persistOK(rf.storage.SaveSnapshot(
		SnapshotMeta{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}, args.Data)) {
		return
	}
	if !keepSuffix {
		if !rf.persistOK(rf.storage.TruncateSuffix(args.LastIncludedIndex + 1)) {
			return
		}
	}
	rf.log.compact(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.snapshot = args.Data
	rf.commitIndex = args.LastIncludedIndex
	rf.pendingSnapshot = &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.applyCond.Signal()
	rf.logger.Debug("snapshot installed", "index", args.LastIncludedIndex)
}
