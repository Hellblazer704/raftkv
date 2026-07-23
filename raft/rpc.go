package raft

// RPC argument/reply types, following the Raft paper (Figure 2).
//
// Field names mirror the paper so the implementation can be cross-checked
// against it section by section.

// RequestVoteArgs — Raft paper §5.2 (leader election) and §5.4.1
// (election restriction: LastLogIndex/LastLogTerm).
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry (§5.4.1)
	LastLogTerm  int // term of candidate's last log entry (§5.4.1)
}

// RequestVoteReply — Raft paper §5.2.
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// AppendEntriesArgs — Raft paper §5.3 (log replication); empty Entries is a
// heartbeat (§5.2).
type AppendEntriesArgs struct {
	Term         int     // leader's term
	LeaderID     int     // so followers can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of PrevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat)
	LeaderCommit int     // leader's commitIndex
}

// AppendEntriesReply — Raft paper §5.3, plus the fast-backtracking
// optimization sketched at the end of §5.3 (ConflictIndex/ConflictTerm) so a
// lagging follower is caught up in O(#distinct terms) round trips instead of
// O(#entries).
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex/Term

	// Fast backtracking (§5.3, "if desired, the protocol can be optimized"):
	ConflictTerm  int // term of the conflicting entry, -1 if follower's log is too short
	ConflictIndex int // first index the follower stores for ConflictTerm, or len(log) if too short
}

// InstallSnapshotArgs — Raft paper §7 (log compaction). We always send the
// snapshot in a single chunk, so Offset/Done from the paper are omitted.
type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderID          int    // so followers can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through this index
	LastIncludedTerm  int    // term of LastIncludedIndex
	Data              []byte // raw snapshot bytes of the service state machine
}

// InstallSnapshotReply — Raft paper §7.
type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// Transport abstracts how RPCs reach a peer. The deterministic simulation
// harness (package sim) and the production net/rpc transport both implement
// it. A false return models a dropped/timed-out RPC; Raft never retries
// blindly — it relies on the next heartbeat/election tick.
type Transport interface {
	RequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool
	AppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool
	InstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool
}
