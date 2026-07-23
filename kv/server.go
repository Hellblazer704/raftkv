package kv

import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hellblazer704/raftkv/raft"
)

// Server is one KV replica: a Raft node plus the applied state machine, the
// session table, and the waiter plumbing that parks RPC handlers until their
// command applies.
type Server struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	mu          sync.Mutex
	applied     *sync.Cond // broadcast when lastApplied advances (lease reads)
	data        map[string]string
	sessions    map[int64]session // per-client dedup + memoized CAS results
	waiters     map[int][]waiter
	lastApplied int

	snapshotThreshold int
	dead              atomic.Bool
	counters          counters
}

// session is one client's exactly-once state. CAS is the op whose *result*
// carries information, so it is memoized: a duplicate CAS delivery must
// return the original outcome, not re-evaluate against newer state.
type session struct {
	LastSeq    int64
	CasSuccess bool
	CasOld     string
}

// counters are cheap always-on operation counters, exported for metrics.
type counters struct {
	Gets, LeaseReads, Puts, Appends, Cas, WrongLeader, Timeouts atomic.Int64
}

// Counters is a point-in-time snapshot of the server's op counters.
type Counters struct {
	Gets, LeaseReads, Puts, Appends, Cas, WrongLeader, Timeouts int64
}

// Counters returns current op counts (for metrics/tests).
func (s *Server) Counters() Counters {
	return Counters{
		Gets:        s.counters.Gets.Load(),
		LeaseReads:  s.counters.LeaseReads.Load(),
		Puts:        s.counters.Puts.Load(),
		Appends:     s.counters.Appends.Load(),
		Cas:         s.counters.Cas.Load(),
		WrongLeader: s.counters.WrongLeader.Load(),
		Timeouts:    s.counters.Timeouts.Load(),
	}
}

type waiter struct {
	clientID int64
	seq      int64
	ch       chan waitResult
}

type waitResult struct {
	err     Err
	value   string
	casOK   bool
	casSeen bool // result carries CAS fields
}

// commitWait bounds how long an RPC handler waits for its log entry to
// apply before reporting ErrTimeout (ambiguous; the clerk retries with the
// same seq, which dedup makes safe).
const commitWait = 1200 * time.Millisecond

// NewServer boots a replica. snapshotThreshold is the un-compacted log size
// (bytes) that triggers a service snapshot; <=0 disables snapshotting.
func NewServer(me, n int, transport raft.Transport, storage raft.Storage, snapshotThreshold int, cfg raft.Config) *Server {
	s := &Server{
		me:                me,
		applyCh:           make(chan raft.ApplyMsg),
		data:              make(map[string]string),
		sessions:          make(map[int64]session),
		waiters:           make(map[int][]waiter),
		snapshotThreshold: snapshotThreshold,
	}
	s.applied = sync.NewCond(&s.mu)
	s.rf = raft.Make(me, n, transport, storage, s.applyCh, cfg)
	go s.applyLoop()
	return s
}

// Raft exposes the underlying node (tests, metrics, leader hints).
func (s *Server) Raft() *raft.Raft { return s.rf }

// Kill stops the replica; storage survives for a restart.
func (s *Server) Kill() {
	s.dead.Store(true)
	s.rf.Kill()
	s.mu.Lock()
	for idx, ws := range s.waiters {
		for _, w := range ws {
			w.ch <- waitResult{err: ErrWrongLeader}
		}
		delete(s.waiters, idx)
	}
	s.applied.Broadcast()
	s.mu.Unlock()
}

// PutAppend handles writes. Exactly-once: if this clerk's seq has already
// been applied, reply OK without touching the log.
func (s *Server) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	if args.Append {
		s.counters.Appends.Add(1)
	} else {
		s.counters.Puts.Add(1)
	}
	if s.dead.Load() {
		reply.Err = ErrWrongLeader
		return nil
	}
	if _, isLeader := s.rf.GetState(); !isLeader {
		s.counters.WrongLeader.Add(1)
		reply.Err = ErrWrongLeader
		return nil
	}
	s.mu.Lock()
	if args.Seq <= s.sessions[args.ClientID].LastSeq {
		s.mu.Unlock()
		reply.Err = OK // duplicate of an applied write
		return nil
	}
	s.mu.Unlock()

	kind := opPut
	if args.Append {
		kind = opAppend
	}
	res := s.propose(Op{Kind: kind, Key: args.Key, Value: args.Value, ClientID: args.ClientID, Seq: args.Seq})
	s.countErr(res.err)
	reply.Err = res.err
	return nil
}

// Cas handles compare-and-swap. Duplicates return the memoized original
// outcome — re-evaluating a retried CAS against newer state would report a
// wrong answer.
func (s *Server) Cas(args *CasArgs, reply *CasReply) error {
	s.counters.Cas.Add(1)
	if s.dead.Load() {
		reply.Err = ErrWrongLeader
		return nil
	}
	if _, isLeader := s.rf.GetState(); !isLeader {
		s.counters.WrongLeader.Add(1)
		reply.Err = ErrWrongLeader
		return nil
	}
	s.mu.Lock()
	if sess, ok := s.sessions[args.ClientID]; ok && args.Seq <= sess.LastSeq {
		s.mu.Unlock()
		if args.Seq == sess.LastSeq {
			reply.Err, reply.Success, reply.Old = OK, sess.CasSuccess, sess.CasOld
		} else {
			// Older than the clerk's live op; no correct clerk reads this.
			reply.Err = ErrWrongLeader
		}
		return nil
	}
	s.mu.Unlock()

	res := s.propose(Op{Kind: opCas, Key: args.Key, Value: args.Value, Expect: args.Expect, ClientID: args.ClientID, Seq: args.Seq})
	s.countErr(res.err)
	reply.Err = res.err
	if res.err == OK {
		reply.Success, reply.Old = res.casOK, res.value
	}
	return nil
}

func (s *Server) countErr(err Err) {
	switch err {
	case ErrWrongLeader:
		s.counters.WrongLeader.Add(1)
	case ErrTimeout:
		s.counters.Timeouts.Add(1)
	}
}

// Get handles reads: lease fast path when the leader holds a majority lease,
// through-the-log otherwise.
func (s *Server) Get(args *GetArgs, reply *GetReply) error {
	s.counters.Gets.Add(1)
	if s.dead.Load() {
		reply.Err = ErrWrongLeader
		return nil
	}
	if readIndex, ok := s.rf.LeaseRead(); ok {
		if value, served := s.leaseServe(readIndex, args.Key); served {
			s.counters.LeaseReads.Add(1)
			reply.Err, reply.Value = OK, value
			return nil
		}
		// Fall through to the log path (applier lagging or node dying).
	}
	res := s.propose(Op{Kind: opGet, Key: args.Key, ClientID: args.ClientID, Seq: args.Seq})
	s.countErr(res.err)
	reply.Err, reply.Value = res.err, res.value
	return nil
}

// leaseServe waits (bounded) for the state machine to cover readIndex, then
// reads locally. Serving state newer than readIndex is fine: the lease says
// no other leader exists right now, so the current applied state is current.
func (s *Server) leaseServe(readIndex int, key string) (string, bool) {
	deadline := time.Now().Add(commitWait)
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.lastApplied < readIndex && !s.dead.Load() && time.Now().Before(deadline) {
		s.applied.Wait() // woken by applyLoop broadcasts and Kill
	}
	if s.lastApplied < readIndex {
		return "", false
	}
	return s.data[key], true
}

// propose replicates op through Raft and waits for it to apply here.
func (s *Server) propose(op Op) waitResult {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&op); err != nil {
		panic("kv: op encode: " + err.Error())
	}
	index, _, isLeader := s.rf.Start(buf.Bytes())
	if !isLeader {
		return waitResult{err: ErrWrongLeader}
	}
	ch := make(chan waitResult, 1)
	s.mu.Lock()
	s.waiters[index] = append(s.waiters[index], waiter{clientID: op.ClientID, seq: op.Seq, ch: ch})
	s.mu.Unlock()

	select {
	case res := <-ch:
		return res
	case <-time.After(commitWait):
		return waitResult{err: ErrTimeout}
	}
}

func (s *Server) applyLoop() {
	for msg := range s.applyCh {
		switch {
		case msg.CommandValid:
			var op Op
			if err := gob.NewDecoder(bytes.NewReader(msg.Command)).Decode(&op); err != nil {
				panic("kv: undecodable command")
			}
			s.mu.Lock()
			res := waitResult{err: OK}
			switch op.Kind {
			case opGet:
				res.value = s.data[op.Key]
			case opPut, opAppend, opCas:
				// The dedup that makes clerk retries safe: a (client, seq)
				// applies at most once no matter how many log entries carry it.
				sess := s.sessions[op.ClientID]
				if op.Seq > sess.LastSeq {
					switch op.Kind {
					case opPut:
						s.data[op.Key] = op.Value
					case opAppend:
						s.data[op.Key] += op.Value
					case opCas:
						sess.CasOld = s.data[op.Key]
						sess.CasSuccess = sess.CasOld == op.Expect
						if sess.CasSuccess {
							s.data[op.Key] = op.Value
						}
					}
					sess.LastSeq = op.Seq
					s.sessions[op.ClientID] = sess
				}
				if op.Kind == opCas && op.Seq == s.sessions[op.ClientID].LastSeq {
					// Fresh or duplicate: the memoized outcome is the answer.
					res.casSeen = true
					res.casOK = s.sessions[op.ClientID].CasSuccess
					res.value = s.sessions[op.ClientID].CasOld
				}
			}
			s.lastApplied = msg.CommandIndex
			for _, w := range s.waiters[msg.CommandIndex] {
				if w.clientID == op.ClientID && w.seq == op.Seq {
					w.ch <- res
				} else {
					// A different command landed at this index; the waiter's
					// clerk retries with the same seq — safe under dedup.
					w.ch <- waitResult{err: ErrWrongLeader}
				}
			}
			delete(s.waiters, msg.CommandIndex)
			s.maybeSnapshotLocked(msg.CommandIndex)
			s.applied.Broadcast()
			s.mu.Unlock()

		case msg.SnapshotValid:
			s.mu.Lock()
			s.restoreSnapshotLocked(msg.Snapshot, msg.SnapshotIndex)
			for idx, ws := range s.waiters {
				if idx <= msg.SnapshotIndex {
					for _, w := range ws {
						w.ch <- waitResult{err: ErrWrongLeader}
					}
					delete(s.waiters, idx)
				}
			}
			s.applied.Broadcast()
			s.mu.Unlock()
		}
		if s.dead.Load() {
			return
		}
	}
}

// snapshotState is everything the service must carry across a snapshot:
// the data AND the session table — dropping the sessions would let old
// duplicates re-apply (or re-answer CAS wrongly) after a restart.
type snapshotState struct {
	KV          map[string]string
	Sessions    map[int64]session
	LastApplied int
}

func (s *Server) maybeSnapshotLocked(index int) {
	if s.snapshotThreshold <= 0 || s.rf.LogSizeBytes() < s.snapshotThreshold {
		return
	}
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(&snapshotState{KV: s.data, Sessions: s.sessions, LastApplied: index})
	if err != nil {
		panic("kv: snapshot encode: " + err.Error())
	}
	s.rf.Snapshot(index, buf.Bytes())
}

func (s *Server) restoreSnapshotLocked(snap []byte, index int) {
	var st snapshotState
	if err := gob.NewDecoder(bytes.NewReader(snap)).Decode(&st); err != nil {
		panic("kv: snapshot decode: " + err.Error())
	}
	s.data = st.KV
	s.sessions = st.Sessions
	if s.data == nil {
		s.data = make(map[string]string)
	}
	if s.sessions == nil {
		s.sessions = make(map[int64]session)
	}
	s.lastApplied = index
}
