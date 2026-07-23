package shard

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hellblazer704/raftkv/raft"
)

// stateMachine is the deterministic core a replicator drives. Apply returns
// the tag identifying which proposal the command satisfies (waiters match on
// it) and the result to hand back.
//
// Apply, Snapshot and Restore are called with the replicator's mutex held;
// services that read state from RPC handlers do so via View.
type stateMachine interface {
	Apply(cmd []byte) (tag string, result any)
	Snapshot() []byte
	Restore(snap []byte)
}

// replicator owns the Raft node, the apply loop, snapshot triggering, and
// the waiter plumbing shared by the controller and the shard groups.
type replicator struct {
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	mu          sync.Mutex
	sm          stateMachine
	waiters     map[int][]rsmWaiter
	lastApplied int

	snapshotThreshold int
	dead              atomic.Bool
}

type rsmWaiter struct {
	tag string
	ch  chan rsmResult
}

type rsmResult struct {
	err    Err
	result any
}

const proposeWait = 1200 * time.Millisecond

func newReplicator(me, n int, transport raft.Transport, storage raft.Storage,
	snapshotThreshold int, cfg raft.Config, sm stateMachine) *replicator {
	r := &replicator{
		applyCh:           make(chan raft.ApplyMsg),
		sm:                sm,
		waiters:           make(map[int][]rsmWaiter),
		snapshotThreshold: snapshotThreshold,
	}
	r.rf = raft.Make(me, n, transport, storage, r.applyCh, cfg)
	go r.applyLoop()
	return r
}

func (r *replicator) Kill() {
	r.dead.Store(true)
	r.rf.Kill()
	r.mu.Lock()
	for idx, ws := range r.waiters {
		for _, w := range ws {
			w.ch <- rsmResult{err: ErrWrongLeader}
		}
		delete(r.waiters, idx)
	}
	r.mu.Unlock()
}

func (r *replicator) killed() bool { return r.dead.Load() }

// View runs f with the state machine's lock held (read paths in RPC
// handlers).
func (r *replicator) View(f func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f()
}

// propose replicates cmd and waits until the state machine applies a command
// carrying this tag at the chosen index.
func (r *replicator) propose(tag string, cmd []byte) (any, Err) {
	if r.killed() {
		return nil, ErrWrongLeader
	}
	index, _, isLeader := r.rf.Start(cmd)
	if !isLeader {
		return nil, ErrWrongLeader
	}
	ch := make(chan rsmResult, 1)
	r.mu.Lock()
	r.waiters[index] = append(r.waiters[index], rsmWaiter{tag: tag, ch: ch})
	r.mu.Unlock()

	select {
	case res := <-ch:
		return res.result, res.err
	case <-time.After(proposeWait):
		return nil, ErrTimeout
	}
}

func (r *replicator) applyLoop() {
	for msg := range r.applyCh {
		switch {
		case msg.CommandValid:
			r.mu.Lock()
			tag, result := r.sm.Apply(msg.Command)
			r.lastApplied = msg.CommandIndex
			for _, w := range r.waiters[msg.CommandIndex] {
				if w.tag == tag {
					w.ch <- rsmResult{err: OK, result: result}
				} else {
					w.ch <- rsmResult{err: ErrWrongLeader} // superseded entry
				}
			}
			delete(r.waiters, msg.CommandIndex)
			if r.snapshotThreshold > 0 && r.rf.LogSizeBytes() >= r.snapshotThreshold {
				r.rf.Snapshot(msg.CommandIndex, r.sm.Snapshot())
			}
			r.mu.Unlock()

		case msg.SnapshotValid:
			r.mu.Lock()
			r.sm.Restore(msg.Snapshot)
			r.lastApplied = msg.SnapshotIndex
			for idx, ws := range r.waiters {
				if idx <= msg.SnapshotIndex {
					for _, w := range ws {
						w.ch <- rsmResult{err: ErrWrongLeader}
					}
					delete(r.waiters, idx)
				}
			}
			r.mu.Unlock()
		}
		if r.killed() {
			return
		}
	}
}
