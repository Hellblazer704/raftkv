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
	lastSeq     map[int64]int64 // session table: highest applied seq per client
	waiters     map[int][]waiter
	lastApplied int

	snapshotThreshold int
	dead              atomic.Bool
}

type waiter struct {
	clientID int64
	seq      int64
	ch       chan waitResult
}

type waitResult struct {
	err   Err
	value string
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
		lastSeq:           make(map[int64]int64),
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
	if s.dead.Load() {
		reply.Err = ErrWrongLeader
		return nil
	}
	if _, isLeader := s.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}
	s.mu.Lock()
	if args.Seq <= s.lastSeq[args.ClientID] {
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
	reply.Err = res.err
	return nil
}

// Get handles reads: lease fast path when the leader holds a majority lease,
// through-the-log otherwise.
func (s *Server) Get(args *GetArgs, reply *GetReply) error {
	if s.dead.Load() {
		reply.Err = ErrWrongLeader
		return nil
	}
	if readIndex, ok := s.rf.LeaseRead(); ok {
		if value, served := s.leaseServe(readIndex, args.Key); served {
			reply.Err, reply.Value = OK, value
			return nil
		}
		// Fall through to the log path (applier lagging or node dying).
	}
	res := s.propose(Op{Kind: opGet, Key: args.Key, ClientID: args.ClientID, Seq: args.Seq})
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
			var out string
			switch op.Kind {
			case opGet:
				out = s.data[op.Key]
			case opPut, opAppend:
				// The dedup that makes clerk retries safe: a (client, seq)
				// applies at most once no matter how many log entries carry it.
				if op.Seq > s.lastSeq[op.ClientID] {
					if op.Kind == opPut {
						s.data[op.Key] = op.Value
					} else {
						s.data[op.Key] += op.Value
					}
					s.lastSeq[op.ClientID] = op.Seq
				}
			}
			s.lastApplied = msg.CommandIndex
			for _, w := range s.waiters[msg.CommandIndex] {
				if w.clientID == op.ClientID && w.seq == op.Seq {
					w.ch <- waitResult{err: OK, value: out}
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
// duplicates re-apply after a restart.
type snapshotState struct {
	Data        map[string]string
	LastSeq     map[int64]int64
	LastApplied int
}

func (s *Server) maybeSnapshotLocked(index int) {
	if s.snapshotThreshold <= 0 || s.rf.LogSizeBytes() < s.snapshotThreshold {
		return
	}
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(&snapshotState{Data: s.data, LastSeq: s.lastSeq, LastApplied: index})
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
	s.data = st.Data
	s.lastSeq = st.LastSeq
	if s.data == nil {
		s.data = make(map[string]string)
	}
	if s.lastSeq == nil {
		s.lastSeq = make(map[int64]int64)
	}
	s.lastApplied = index
}
