package shard

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"
)

func newClientID() int64 {
	n, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		panic("shard: cannot generate client id: " + err.Error())
	}
	return n.Int64()
}

// CtrlerClerk talks to the shard controller group. Mutations are
// exactly-once (client session); Query is read-only.
type CtrlerClerk struct {
	transport Transport
	servers   []string

	mu       sync.Mutex
	clientID int64
	seq      int64
	leader   int
}

func NewCtrlerClerk(transport Transport, servers []string) *CtrlerClerk {
	return &CtrlerClerk{transport: transport, servers: servers, clientID: newClientID()}
}

func (ck *CtrlerClerk) call(method string, build func(seq int64) any) Config {
	ck.mu.Lock()
	ck.seq++
	args := build(ck.seq)
	ck.mu.Unlock()
	for attempt := 0; ; attempt++ {
		ck.mu.Lock()
		target := (ck.leader + attempt) % len(ck.servers)
		server := ck.servers[target]
		ck.mu.Unlock()
		reply := &CtrlerReply{}
		if ck.transport.Call(server, method, args, reply) && reply.Err == OK {
			ck.mu.Lock()
			ck.leader = target
			ck.mu.Unlock()
			return reply.Config
		}
		if (attempt+1)%len(ck.servers) == 0 {
			time.Sleep(60 * time.Millisecond)
		}
	}
}

// Join adds groups and waits for the new config to commit.
func (ck *CtrlerClerk) Join(servers map[int][]string) {
	ck.call("Ctrler.Join", func(seq int64) any {
		return &JoinArgs{Servers: servers, ClientID: ck.clientID, Seq: seq}
	})
}

// Leave removes groups.
func (ck *CtrlerClerk) Leave(gids []int) {
	ck.call("Ctrler.Leave", func(seq int64) any {
		return &LeaveArgs{GIDs: gids, ClientID: ck.clientID, Seq: seq}
	})
}

// Move pins a shard to a group.
func (ck *CtrlerClerk) Move(shard, gid int) {
	ck.call("Ctrler.Move", func(seq int64) any {
		return &MoveArgs{Shard: shard, GID: gid, ClientID: ck.clientID, Seq: seq}
	})
}

// Query returns config num (latest for -1). Unlike the mutators it reports
// failure (bounded retries), because group tickers call it on a timer and
// must not block forever while the controller is unreachable.
func (ck *CtrlerClerk) Query(num int) (Config, Err) {
	args := &QueryArgs{Num: num}
	for attempt := 0; attempt < 2*len(ck.servers); attempt++ {
		ck.mu.Lock()
		target := (ck.leader + attempt) % len(ck.servers)
		server := ck.servers[target]
		ck.mu.Unlock()
		reply := &CtrlerReply{}
		if ck.transport.Call(server, "Ctrler.Query", args, reply) && reply.Err == OK {
			ck.mu.Lock()
			ck.leader = target
			ck.mu.Unlock()
			return reply.Config, OK
		}
	}
	return Config{}, ErrTimeout
}

// Clerk is the sharded KV client: routes each key to its shard's group per
// the cached config, refreshing from the controller on ErrWrongGroup.
type Clerk struct {
	ctrler    *CtrlerClerk
	transport Transport

	mu       sync.Mutex
	config   Config
	clientID int64
	seq      int64
	leader   map[int]int // gid -> cached leader offset
}

func NewClerk(ctrler *CtrlerClerk, transport Transport) *Clerk {
	return &Clerk{
		ctrler:    ctrler,
		transport: transport,
		clientID:  newClientID(),
		leader:    make(map[int]int),
	}
}

// Get returns the value for key ("" if absent).
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	ck.seq++
	args := &GetArgs{Key: key, ClientID: ck.clientID, Seq: ck.seq}
	ck.mu.Unlock()
	var value string
	ck.run(key, func(server string) (bool, Err) {
		reply := &GetReply{}
		if !ck.transport.Call(server, "KV.Get", args, reply) {
			return false, ErrTimeout
		}
		value = reply.Value
		return true, reply.Err
	})
	return value
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
	ck.run(key, func(server string) (bool, Err) {
		reply := &PutAppendReply{}
		if !ck.transport.Call(server, "KV.PutAppend", args, reply) {
			return false, ErrTimeout
		}
		return true, reply.Err
	})
}

// run drives one op to completion: route to the owning group, rotate through
// its servers, refresh the config on ErrWrongGroup, retry forever (safe:
// writes are deduplicated server-side).
func (ck *Clerk) run(key string, do func(server string) (bool, Err)) {
	shard := Key2Shard(key)
	for {
		ck.mu.Lock()
		gid := ck.config.Shards[shard]
		servers := append([]string(nil), ck.config.Groups[gid]...)
		start := ck.leader[gid]
		ck.mu.Unlock()

		if gid != 0 && len(servers) > 0 {
			for off := 0; off < len(servers); off++ {
				idx := (start + off) % len(servers)
				delivered, err := do(servers[idx])
				if delivered && err == OK {
					ck.mu.Lock()
					ck.leader[gid] = idx
					ck.mu.Unlock()
					return
				}
				if delivered && err == ErrWrongGroup {
					break // config moved; refresh below
				}
				// ErrWrongLeader / ErrTimeout / no reply: try next server.
			}
		}
		if cfg, err := ck.ctrler.Query(-1); err == OK {
			ck.mu.Lock()
			ck.config = cfg
			ck.mu.Unlock()
		}
		time.Sleep(60 * time.Millisecond)
	}
}
