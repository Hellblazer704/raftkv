// Package shard implements the sharded keyspace: a shard controller (its own
// Raft group) that versions shard→group assignments, and shard groups (each a
// Raft group) that serve slices of the keyspace and migrate shards between
// configs.
//
// Everything that changes state — client ops, config transitions, shard
// installs, shard deletions — goes through each group's Raft log, so every
// replica makes identical decisions. Session tables migrate *with* their
// shards: exactly-once must survive a client whose retry lands on the
// shard's new owner.
package shard

import (
	"hash/fnv"
)

// NShards is the fixed number of keyspace slices. Groups own whole slices;
// the controller balances slice counts across groups.
const NShards = 10

// Key2Shard maps a key to its shard.
func Key2Shard(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % NShards)
}

// Config is one versioned assignment of shards to groups. Config 0 is the
// zero value: no groups, every shard unassigned (gid 0).
type Config struct {
	Num    int              // monotonically increasing version
	Shards [NShards]int     // shard -> gid; 0 means unassigned
	Groups map[int][]string // gid -> server names
}

// Clone deep-copies a config (Groups is a map).
func (c Config) Clone() Config {
	out := c
	out.Groups = make(map[int][]string, len(c.Groups))
	for gid, servers := range c.Groups {
		out.Groups[gid] = append([]string(nil), servers...)
	}
	return out
}

type Err string

const (
	OK             Err = "OK"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
	ErrWrongGroup  Err = "ErrWrongGroup" // shard not owned/serving here at this config
	ErrNotReady    Err = "ErrNotReady"   // group hasn't reached the requested config yet
)

// ---- controller RPCs ----

type JoinArgs struct {
	Servers  map[int][]string // new gid -> server names
	ClientID int64
	Seq      int64
}

type LeaveArgs struct {
	GIDs     []int
	ClientID int64
	Seq      int64
}

type MoveArgs struct {
	Shard    int
	GID      int
	ClientID int64
	Seq      int64
}

type QueryArgs struct {
	Num int // -1 (or > latest) means latest
}

type CtrlerReply struct {
	Err    Err
	Config Config // Query only
}

// ---- group client RPCs ----

type GetArgs struct {
	Key      string
	ClientID int64
	Seq      int64
}

type GetReply struct {
	Err   Err
	Value string
}

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

// ---- group-to-group RPCs (migration) ----

// PullArgs asks the previous owner for a shard frozen at the transition into
// config Num.
type PullArgs struct {
	Num   int
	Shard int
}

type PullReply struct {
	Err   Err
	Shard ShardData
}

// InstalledArgs asks a shard's new owner whether it has installed the shard
// at config Num; a "yes" lets the old owner garbage-collect its copy.
type InstalledArgs struct {
	Num   int
	Shard int
}

type InstalledReply struct {
	Err       Err
	Installed bool
}

// ShardData is one shard's state: user data plus the session table that
// makes retried writes idempotent — it must travel with the shard.
type ShardData struct {
	Data    map[string]string
	LastSeq map[int64]int64
}

// NewShardData allocates an empty shard.
func NewShardData() ShardData {
	return ShardData{Data: make(map[string]string), LastSeq: make(map[int64]int64)}
}

// Clone deep-copies shard data (handed across group boundaries).
func (sd ShardData) Clone() ShardData {
	out := NewShardData()
	for k, v := range sd.Data {
		out.Data[k] = v
	}
	for c, s := range sd.LastSeq {
		out.LastSeq[c] = s
	}
	return out
}

// Transport carries RPCs to named servers. Group membership is expressed as
// server names in Config.Groups, so both the simulated network and a real
// address-based transport plug in.
type Transport interface {
	Call(server string, method string, args, reply any) bool
}
