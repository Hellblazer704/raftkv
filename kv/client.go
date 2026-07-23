package kv

import (
	"sync"
	"time"
)

// Clerk is the client library. Safe for use from one goroutine at a time
// (like a database handle); create one clerk per worker.
//
// Exactly-once contract: writes carry (clientID, seq); the clerk retries any
// ambiguous failure with the SAME seq until a leader confirms it applied.
type Clerk struct {
	transport Transport
	n         int

	mu       sync.Mutex
	clientID int64
	seq      int64
	leader   int
}

func NewClerk(transport Transport, n int) *Clerk {
	return &Clerk{transport: transport, n: n, clientID: newClientID()}
}

// Get returns the value for key ("" if absent). Blocks until a quorum
// answers.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	ck.seq++
	args := &GetArgs{Key: key, ClientID: ck.clientID, Seq: ck.seq}
	ck.mu.Unlock()
	for attempt := 0; ; attempt++ {
		target := ck.pick(attempt)
		reply := &GetReply{}
		if ck.transport.Call(target, "KV.Get", args, reply) && reply.Err == OK {
			ck.setLeader(target)
			return reply.Value
		}
		ck.backoff(attempt)
	}
}

// Put sets key=value.
func (ck *Clerk) Put(key, value string) { ck.putAppend(key, value, false) }

// Append appends value to key.
func (ck *Clerk) Append(key, value string) { ck.putAppend(key, value, true) }

func (ck *Clerk) putAppend(key, value string, isAppend bool) {
	ck.mu.Lock()
	ck.seq++
	args := &PutAppendArgs{Key: key, Value: value, Append: isAppend, ClientID: ck.clientID, Seq: ck.seq}
	ck.mu.Unlock()
	for attempt := 0; ; attempt++ {
		target := ck.pick(attempt)
		reply := &PutAppendReply{}
		if ck.transport.Call(target, "KV.PutAppend", args, reply) && reply.Err == OK {
			ck.setLeader(target)
			return
		}
		// ErrWrongLeader, ErrTimeout, or no reply: rotate and retry with the
		// same seq. Dedup on the servers makes this safe.
		ck.backoff(attempt)
	}
}

// Cas atomically sets key to value iff its current value equals expect,
// returning (whether it swapped, the value observed). Retries are safe: a
// duplicate delivery returns the original attempt's memoized outcome.
func (ck *Clerk) Cas(key, expect, value string) (bool, string) {
	ck.mu.Lock()
	ck.seq++
	args := &CasArgs{Key: key, Expect: expect, Value: value, ClientID: ck.clientID, Seq: ck.seq}
	ck.mu.Unlock()
	for attempt := 0; ; attempt++ {
		target := ck.pick(attempt)
		reply := &CasReply{}
		if ck.transport.Call(target, "KV.Cas", args, reply) && reply.Err == OK {
			ck.setLeader(target)
			return reply.Success, reply.Old
		}
		ck.backoff(attempt)
	}
}

func (ck *Clerk) pick(attempt int) int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return (ck.leader + attempt) % ck.n
}

func (ck *Clerk) setLeader(target int) {
	ck.mu.Lock()
	ck.leader = target
	ck.mu.Unlock()
}

// backoff sleeps briefly once a full rotation has failed, so a leaderless
// cluster isn't hammered.
func (ck *Clerk) backoff(attempt int) {
	if (attempt+1)%ck.n == 0 {
		time.Sleep(60 * time.Millisecond)
	}
}
