package raft

// Entry is a single Raft log entry (Raft paper Figure 2: "each entry contains
// command for state machine, and term when entry was received by leader").
type Entry struct {
	Index   int
	Term    int
	Command []byte
}

// raftLog holds the suffix of the log that has not yet been compacted into a
// snapshot (§7). Physical slot 0 is a sentinel carrying the snapshot's
// lastIncludedIndex/lastIncludedTerm, which keeps every consistency check
// (PrevLogTerm at the snapshot boundary, election restriction, etc.) uniform
// with no special cases.
type raftLog struct {
	entries []Entry // entries[0] is the sentinel; entries[0].Command is nil
}

func newLog() *raftLog {
	return &raftLog{entries: []Entry{{Index: 0, Term: 0}}}
}

// firstIndex is the snapshot boundary: everything <= firstIndex is compacted.
func (l *raftLog) firstIndex() int { return l.entries[0].Index }

func (l *raftLog) lastIndex() int { return l.entries[len(l.entries)-1].Index }

func (l *raftLog) lastTerm() int { return l.entries[len(l.entries)-1].Term }

// term returns the term of the entry at index, which must satisfy
// firstIndex <= index <= lastIndex.
func (l *raftLog) term(index int) int {
	return l.entries[index-l.firstIndex()].Term
}

// entry returns the entry at index (firstIndex < index <= lastIndex).
func (l *raftLog) entry(index int) Entry {
	return l.entries[index-l.firstIndex()]
}

// slice returns a copy of entries in [from, lastIndex]. Copying matters: the
// caller hands these to RPCs that outlive the lock, and the underlying array
// is mutated by truncations.
func (l *raftLog) slice(from int) []Entry {
	src := l.entries[from-l.firstIndex():]
	out := make([]Entry, len(src))
	copy(out, src)
	return out
}

// append adds entries after the current last entry.
func (l *raftLog) append(entries ...Entry) {
	l.entries = append(l.entries, entries...)
}

// truncateSuffix drops all entries with index >= from (used when a follower
// finds a conflicting entry, §5.3: "delete the existing entry and all that
// follow it").
func (l *raftLog) truncateSuffix(from int) {
	l.entries = l.entries[:from-l.firstIndex()]
}

// compact discards entries up through index (which becomes the new sentinel).
// Called after the service snapshots its state (§7).
func (l *raftLog) compact(index, term int) {
	if index <= l.firstIndex() {
		return
	}
	var suffix []Entry
	if index <= l.lastIndex() && l.term(index) == term {
		suffix = l.slice(index + 1)
	}
	// Rebuild backing array so compacted entries can be garbage collected.
	fresh := make([]Entry, 0, len(suffix)+1)
	fresh = append(fresh, Entry{Index: index, Term: term})
	fresh = append(fresh, suffix...)
	l.entries = fresh
}

// firstIndexOfTerm returns the lowest index in the log whose entry has the
// given term, or -1 if no entry has that term. Used for fast backtracking.
func (l *raftLog) firstIndexOfTerm(term int) int {
	for i := 1; i < len(l.entries); i++ {
		if l.entries[i].Term == term {
			return l.entries[i].Index
		}
	}
	return -1
}

// lastIndexOfTerm returns the highest index whose entry has the given term,
// or -1 if none. Used by the leader's fast-backtracking response handling.
func (l *raftLog) lastIndexOfTerm(term int) int {
	for i := len(l.entries) - 1; i >= 1; i-- {
		if l.entries[i].Term == term {
			return l.entries[i].Index
		}
	}
	return -1
}
