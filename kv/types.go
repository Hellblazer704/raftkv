// Package kv is the client-facing key-value service on top of Raft, with
// exactly-once semantics: every clerk owns a random client ID and stamps each
// write with a monotonically increasing sequence number; replicas apply a
// (clientID, seq) at most once, so clerks retry ambiguous failures freely —
// the capability whose absence the chaos rig demonstrated (BUGS.md #2).
//
// Reads are linearizable two ways: through the log like writes, or — when
// the leader holds a majority lease — served from local state with no log
// write at all (see raft/leaseread.go).
package kv

import (
	"crypto/rand"
	"math/big"
)

type Err string

const (
	OK             Err = "OK"
	ErrWrongLeader Err = "ErrWrongLeader" // definitely not executed here
	ErrTimeout     Err = "ErrTimeout"     // possibly executed; retry with same seq
)

// PutAppendArgs is the write RPC. Seq must increase by 1 per clerk write;
// retries of the same write reuse the same Seq.
type PutAppendArgs struct {
	Key      string
	Value    string
	Append   bool
	ClientID int64
	Seq      int64
}

type PutAppendReply struct {
	Err Err
}

// GetArgs is the read RPC. ClientID/Seq identify the waiter on the log-read
// path; reads are not deduplicated (re-executing a read is harmless).
type GetArgs struct {
	Key      string
	ClientID int64
	Seq      int64
}

type GetReply struct {
	Err   Err
	Value string
}

// CasArgs is compare-and-swap: set Key to Value iff its current value equals
// Expect. The one op whose *result* matters for exactly-once: a retried CAS
// must see the original attempt's outcome, so sessions memoize it.
type CasArgs struct {
	Key      string
	Expect   string
	Value    string
	ClientID int64
	Seq      int64
}

type CasReply struct {
	Err     Err
	Success bool
	Old     string // value observed at apply time
}

// Op is the command replicated through Raft (gob-encoded).
type Op struct {
	Kind     opKind
	Key      string
	Value    string
	Expect   string // opCas only
	ClientID int64
	Seq      int64
}

type opKind uint8

const (
	opGet opKind = iota
	opPut
	opAppend
	opCas
)

// Transport carries clerk RPCs to server i. Implemented by the simulated
// network (tests/benchmarks) and by net/rpc (production, cmd/raftkvd).
type Transport interface {
	Call(server int, method string, args, reply any) bool
}

// newClientID draws a random 62-bit id; collision across clerks would merge
// their sessions, so take it from crypto/rand.
func newClientID() int64 {
	n, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		panic("kv: cannot generate client id: " + err.Error())
	}
	return n.Int64()
}
