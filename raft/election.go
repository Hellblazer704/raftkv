package raft

import "time"

// startElectionLocked begins a new election (§5.2: "To begin an election, a
// follower increments its current term and transitions to candidate state.
// It then votes for itself and issues RequestVote RPCs in parallel").
// Caller holds rf.mu.
func (rf *Raft) startElectionLocked() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	if !rf.persistHardState() {
		return
	}
	rf.resetElectionTimer()
	term := rf.currentTerm
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  rf.me,
		LastLogIndex: rf.log.lastIndex(),
		LastLogTerm:  rf.log.lastTerm(),
	}
	rf.logger.Debug("election started", "term", term)

	votes := 1 // voted for self
	for peer := 0; peer < rf.n; peer++ {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			if !rf.transport.RequestVote(peer, args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term)
				return
			}
			// Stale reply from an earlier candidacy — ignore.
			if rf.state != Candidate || rf.currentTerm != term || !reply.VoteGranted {
				return
			}
			votes++
			if votes > rf.n/2 {
				rf.becomeLeaderLocked()
			}
		}(peer)
	}
}

// becomeLeaderLocked initializes leader state (Figure 2: "for each server,
// nextIndex initialized to leader last log index + 1, matchIndex to 0") and
// immediately asserts leadership with a heartbeat round (§5.2).
func (rf *Raft) becomeLeaderLocked() {
	rf.state = Leader
	rf.leaderID = rf.me
	rf.nextIndex = make([]int, rf.n)
	rf.matchIndex = make([]int, rf.n)
	rf.leaseFrom = make([]time.Time, rf.n)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.log.lastIndex() + 1
	}
	rf.matchIndex[rf.me] = rf.log.lastIndex()
	rf.logger.Info("became leader", "term", rf.currentTerm, "lastIndex", rf.log.lastIndex())
	rf.broadcastLocked()
}

// HandleRequestVote implements the RequestVote receiver (§5.2, Figure 2),
// including the election restriction (§5.4.1: "the voter denies its vote if
// its own log is more up-to-date than that of the candidate").
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
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
		// §5.1: reject stale-term requests.
		return
	}
	// §5.4.1 up-to-date check: compare last entry terms, then lengths.
	upToDate := args.LastLogTerm > rf.log.lastTerm() ||
		(args.LastLogTerm == rf.log.lastTerm() && args.LastLogIndex >= rf.log.lastIndex())
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		if !rf.persistHardState() {
			return
		}
		reply.VoteGranted = true
		// Granting a vote counts as hearing from a viable candidate — reset
		// the election timer so we don't immediately start a rival election.
		rf.resetElectionTimer()
		rf.logger.Debug("vote granted", "to", args.CandidateID, "term", args.Term)
	}
}
