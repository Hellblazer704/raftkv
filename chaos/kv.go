// Package chaos is a Jepsen-style test rig: it runs a minimal Raft-backed KV
// service on the simulated network, lets a nemesis inject faults from a
// seeded schedule, records every client operation with real-time bounds, and
// hands the history to the linearizability checker.
//
// The KV subject here is deliberately session-free, so its clients never
// retry a write: an ambiguous outcome (lost reply, commit timeout) is
// recorded as indeterminate rather than retried, because a blind retry could
// execute twice and no checker model would fit. Exactly-once retries are what
// the session layer (kv package, Phase 3) adds.
package chaos

import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hellblazer704/raftkv/linz"
	"github.com/Hellblazer704/raftkv/raft"
	"github.com/Hellblazer704/raftkv/sim"
)

// Command is the state-machine operation replicated through Raft.
type Command struct {
	Kind  linz.Kind
	Key   string
	Value string
	Nonce int64 // unique per proposal; identifies whose Start() committed
}

// OpArgs/OpReply are the client-facing RPC.
type OpArgs struct {
	Cmd Command
}

const (
	replyOK          = ""             // executed; Value holds the Get result
	replyWrongLeader = "wrong_leader" // definitely not executed; safe to retry elsewhere
	replyMaybe       = "maybe"        // may or may not commit later; NOT safe to retry
)

type OpReply struct {
	Err   string
	Value string
}

type waiter struct {
	nonce int64
	ch    chan OpReply
}

// Server is one KV replica: a Raft node plus the applied state machine.
type Server struct {
	sim.RaftService
	me      int
	applyCh chan raft.ApplyMsg

	mu          sync.Mutex
	data        map[string]string
	waiters     map[int][]waiter
	lastApplied int

	snapshotThreshold int
	dead              atomic.Bool
}

// NewServer boots a replica on the given network and storage.
func NewServer(me, n int, net *sim.Network, storage raft.Storage, snapshotThreshold int, cfg raft.Config) *Server {
	s := &Server{
		me:                me,
		applyCh:           make(chan raft.ApplyMsg),
		data:              make(map[string]string),
		waiters:           make(map[int][]waiter),
		snapshotThreshold: snapshotThreshold,
	}
	s.RF = raft.Make(me, n, &sim.RaftTransport{Net: net, From: me}, storage, s.applyCh, cfg)
	go s.applyLoop()
	return s
}

// Kill stops the replica (crash). Storage is left intact for a restart.
func (s *Server) Kill() {
	s.dead.Store(true)
	s.RF.Kill()
	s.failAllWaiters(replyMaybe)
}

func (s *Server) failAllWaiters(err string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for idx, ws := range s.waiters {
		for _, w := range ws {
			w.ch <- OpReply{Err: err}
		}
		delete(s.waiters, idx)
	}
}

// Dispatch implements sim.Service: Raft RPCs are delegated, KV.Op handled here.
func (s *Server) Dispatch(method string, args, reply any) {
	if method == "KV.Op" {
		s.handleOp(args.(*OpArgs), reply.(*OpReply))
		return
	}
	s.RaftService.Dispatch(method, args, reply)
}

func (s *Server) handleOp(args *OpArgs, reply *OpReply) {
	if s.dead.Load() {
		reply.Err = replyWrongLeader
		return
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&args.Cmd); err != nil {
		reply.Err = replyWrongLeader
		return
	}
	index, term, isLeader := s.RF.Start(buf.Bytes())
	if !isLeader {
		reply.Err = replyWrongLeader
		return
	}
	ch := make(chan OpReply, 1)
	s.mu.Lock()
	s.waiters[index] = append(s.waiters[index], waiter{nonce: args.Cmd.Nonce, ch: ch})
	s.mu.Unlock()

	select {
	case r := <-ch:
		*reply = r
	case <-time.After(1200 * time.Millisecond):
		// The entry may still commit after we give up — ambiguous.
		reply.Err = replyMaybe
	}
	_ = term
}

func (s *Server) applyLoop() {
	for msg := range s.applyCh {
		switch {
		case msg.CommandValid:
			var cmd Command
			if err := gob.NewDecoder(bytes.NewReader(msg.Command)).Decode(&cmd); err != nil {
				panic("chaos: undecodable command")
			}
			s.mu.Lock()
			var out string
			switch cmd.Kind {
			case linz.Put:
				s.data[cmd.Key] = cmd.Value
			case linz.Append:
				s.data[cmd.Key] += cmd.Value
			case linz.Get:
				out = s.data[cmd.Key]
			}
			s.lastApplied = msg.CommandIndex
			for _, w := range s.waiters[msg.CommandIndex] {
				if w.nonce == cmd.Nonce {
					w.ch <- OpReply{Err: replyOK, Value: out}
				} else {
					// A different entry landed at our index. That does NOT
					// prove our proposal died: it may have been replicated
					// before we lost leadership and committed at a *different*
					// index under the new leader. Only "maybe" is sound —
					// calling this wrong_leader once let clients retry and
					// double-execute writes (see BUGS.md).
					w.ch <- OpReply{Err: replyMaybe}
				}
			}
			delete(s.waiters, msg.CommandIndex)
			s.maybeSnapshotLocked(msg.CommandIndex)
			s.mu.Unlock()

		case msg.SnapshotValid:
			s.mu.Lock()
			s.restoreSnapshotLocked(msg.Snapshot, msg.SnapshotIndex)
			// Anything we were waiting on at or below the snapshot boundary
			// committed as *something*, but we can no longer match nonces.
			for idx, ws := range s.waiters {
				if idx <= msg.SnapshotIndex {
					for _, w := range ws {
						w.ch <- OpReply{Err: replyMaybe}
					}
					delete(s.waiters, idx)
				}
			}
			s.mu.Unlock()
		}
		if s.dead.Load() {
			return
		}
	}
}

type snapshotState struct {
	Data        map[string]string
	LastApplied int
}

func (s *Server) maybeSnapshotLocked(index int) {
	if s.snapshotThreshold <= 0 || s.RF.LogSizeBytes() < s.snapshotThreshold {
		return
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&snapshotState{Data: s.data, LastApplied: index}); err != nil {
		panic("chaos: snapshot encode")
	}
	s.RF.Snapshot(index, buf.Bytes())
}

func (s *Server) restoreSnapshotLocked(snap []byte, index int) {
	var st snapshotState
	if err := gob.NewDecoder(bytes.NewReader(snap)).Decode(&st); err != nil {
		panic("chaos: snapshot decode")
	}
	s.data = st.Data
	if s.data == nil {
		s.data = make(map[string]string)
	}
	s.lastApplied = index
}
