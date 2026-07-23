package shard

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Hellblazer704/raftkv/raft"
)

// Shard lifecycle within a group, per config transition:
//
//	Absent   — not ours (or already handed off and garbage-collected)
//	Serving  — ours; client ops apply
//	Pulling  — newly ours; fetching frozen state from the previous owner
//	Offering — no longer ours; frozen, held until the new owner confirms
//	           installation, then deleted (GC)
//
// A group only advances to config N+1 once nothing is Pulling or Offering,
// so migrations complete one transition at a time and Pull sources are
// always frozen exactly at the requested config boundary.
type shardStatus uint8

const (
	statusAbsent shardStatus = iota
	statusServing
	statusPulling
	statusOffering
)

// Group is one replica of one shard group.
type Group struct {
	gid int
	me  int
	rsm *replicator

	ctrler    *CtrlerClerk
	transport Transport

	// Replicated state (guarded by the replicator's mutex).
	config     Config
	prevConfig Config
	shards     map[int]*ShardData
	status     [NShards]shardStatus

	stopped atomic.Bool
	stop    chan struct{}
}

type groupCmdKind uint8

const (
	gcmdKV groupCmdKind = iota
	gcmdConfig
	gcmdInstall
	gcmdDelete
)

type groupCmd struct {
	Kind groupCmdKind

	// gcmdKV
	OpKind   opKind // get/put/append
	Key      string
	Value    string
	ClientID int64
	Seq      int64

	// gcmdConfig
	Cfg Config

	// gcmdInstall / gcmdDelete
	Num   int
	Shard int
	Data  ShardData // gcmdInstall
}

type opKind uint8

const (
	opGet opKind = iota
	opPut
	opAppend
)

type kvResult struct {
	err   Err
	value string
}

// NewGroup boots one replica of group gid. ctrlerServers are the controller
// endpoints (queried for configs); transport reaches both the controller and
// other groups' servers by name.
func NewGroup(gid, me, n int, transport raft.Transport, storage raft.Storage,
	snapshotThreshold int, cfg raft.Config, ctrler *CtrlerClerk, groupTransport Transport) *Group {
	g := &Group{
		gid:       gid,
		me:        me,
		ctrler:    ctrler,
		transport: groupTransport,
		config:    Config{Num: 0, Groups: map[int][]string{}},
		shards:    make(map[int]*ShardData),
		stop:      make(chan struct{}),
	}
	g.rsm = newReplicator(me, n, transport, storage, snapshotThreshold, cfg, g)
	go g.ticker()
	return g
}

// Raft exposes the underlying node.
func (g *Group) Raft() *raft.Raft { return g.rsm.rf }

// HoldsShard reports whether this replica still stores data for shard s
// (used by tests to verify garbage collection, and by metrics).
func (g *Group) HoldsShard(s int) bool {
	held := false
	g.rsm.View(func() { _, held = g.shards[s] })
	return held
}

// Kill stops the replica.
func (g *Group) Kill() {
	if g.stopped.CompareAndSwap(false, true) {
		close(g.stop)
	}
	g.rsm.Kill()
}

// ---- client RPCs ----

func (g *Group) Get(args *GetArgs, reply *GetReply) error {
	res := g.proposeKV(groupCmd{Kind: gcmdKV, OpKind: opGet, Key: args.Key, ClientID: args.ClientID, Seq: args.Seq})
	reply.Err, reply.Value = res.err, res.value
	return nil
}

func (g *Group) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kind := opPut
	if args.Append {
		kind = opAppend
	}
	res := g.proposeKV(groupCmd{Kind: gcmdKV, OpKind: kind, Key: args.Key, Value: args.Value, ClientID: args.ClientID, Seq: args.Seq})
	reply.Err = res.err
	return nil
}

func (g *Group) proposeKV(cmd groupCmd) kvResult {
	// Fast pre-check so misrouted clients bounce without a log write.
	shard := Key2Shard(cmd.Key)
	misrouted := false
	g.rsm.View(func() {
		misrouted = g.config.Shards[shard] != g.gid || g.status[shard] != statusServing
	})
	if misrouted {
		return kvResult{err: ErrWrongGroup}
	}
	result, err := g.propose(fmt.Sprintf("k.%d.%d", cmd.ClientID, cmd.Seq), cmd)
	if err != OK {
		return kvResult{err: err}
	}
	return result.(kvResult)
}

func (g *Group) propose(tag string, cmd groupCmd) (any, Err) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&cmd); err != nil {
		panic("shard: group cmd encode: " + err.Error())
	}
	return g.rsm.propose(tag, buf.Bytes())
}

// ---- group-to-group RPCs ----

// Pull serves a frozen shard to its new owner. Any replica may answer: once
// a replica has applied the transition into args.Num, the shard can receive
// no further writes, so its applied copy is the final copy.
func (g *Group) Pull(args *PullArgs, reply *PullReply) error {
	g.rsm.View(func() {
		if g.config.Num == args.Num && g.status[args.Shard] == statusOffering {
			reply.Err = OK
			reply.Shard = g.shards[args.Shard].Clone()
			return
		}
		reply.Err = ErrNotReady
	})
	return nil
}

// Installed tells a shard's previous owner whether we have installed it at
// config args.Num (green light for its garbage collection). Applied state is
// committed state, so any replica's "yes" is safe.
func (g *Group) Installed(args *InstalledArgs, reply *InstalledReply) error {
	g.rsm.View(func() {
		reply.Err = OK
		reply.Installed = g.config.Num > args.Num ||
			(g.config.Num == args.Num && g.status[args.Shard] == statusServing)
	})
	return nil
}

// ---- background driver (leader only) ----

func (g *Group) ticker() {
	for {
		select {
		case <-g.stop:
			return
		case <-time.After(80 * time.Millisecond):
		}
		if _, isLeader := g.rsm.rf.GetState(); !isLeader {
			continue
		}
		g.driveMigration()
		g.driveConfig()
	}
}

// driveMigration pulls shards we're acquiring and GCs shards we've lost.
func (g *Group) driveMigration() {
	type pullJob struct {
		num     int
		shard   int
		servers []string
	}
	type gcJob struct {
		num     int
		shard   int
		servers []string
	}
	var pulls []pullJob
	var gcs []gcJob
	g.rsm.View(func() {
		for s := 0; s < NShards; s++ {
			switch g.status[s] {
			case statusPulling:
				owner := g.prevConfig.Shards[s]
				pulls = append(pulls, pullJob{num: g.config.Num, shard: s, servers: g.prevConfig.Groups[owner]})
			case statusOffering:
				owner := g.config.Shards[s]
				gcs = append(gcs, gcJob{num: g.config.Num, shard: s, servers: g.config.Groups[owner]})
			}
		}
	})
	for _, job := range pulls {
		for _, server := range job.servers {
			args := &PullArgs{Num: job.num, Shard: job.shard}
			reply := &PullReply{}
			if g.transport.Call(server, "Shard.Pull", args, reply) && reply.Err == OK {
				g.propose(fmt.Sprintf("i.%d.%d", job.num, job.shard),
					groupCmd{Kind: gcmdInstall, Num: job.num, Shard: job.shard, Data: reply.Shard})
				break
			}
		}
	}
	for _, job := range gcs {
		for _, server := range job.servers {
			args := &InstalledArgs{Num: job.num, Shard: job.shard}
			reply := &InstalledReply{}
			if g.transport.Call(server, "Shard.Installed", args, reply) && reply.Err == OK {
				if reply.Installed {
					g.propose(fmt.Sprintf("d.%d.%d", job.num, job.shard),
						groupCmd{Kind: gcmdDelete, Num: job.num, Shard: job.shard})
				}
				break
			}
		}
	}
}

// driveConfig advances to the next config once the current transition has
// fully settled (nothing pulling or offering).
func (g *Group) driveConfig() {
	settled, current := true, 0
	g.rsm.View(func() {
		current = g.config.Num
		for s := 0; s < NShards; s++ {
			if g.status[s] == statusPulling || g.status[s] == statusOffering {
				settled = false
			}
		}
	})
	if !settled {
		return
	}
	next, err := g.ctrler.Query(current + 1)
	if err != OK || next.Num != current+1 {
		return
	}
	g.propose(fmt.Sprintf("c.%d", next.Num), groupCmd{Kind: gcmdConfig, Cfg: next})
}

// ---- state machine (called with the replicator lock held) ----

// Apply implements stateMachine.
func (g *Group) Apply(cmdBytes []byte) (string, any) {
	var cmd groupCmd
	if err := gob.NewDecoder(bytes.NewReader(cmdBytes)).Decode(&cmd); err != nil {
		panic("shard: undecodable group cmd")
	}
	switch cmd.Kind {
	case gcmdKV:
		return fmt.Sprintf("k.%d.%d", cmd.ClientID, cmd.Seq), g.applyKV(cmd)
	case gcmdConfig:
		g.applyConfig(cmd.Cfg)
		return fmt.Sprintf("c.%d", cmd.Cfg.Num), nil
	case gcmdInstall:
		if cmd.Num == g.config.Num && g.status[cmd.Shard] == statusPulling {
			sd := cmd.Data.Clone()
			g.shards[cmd.Shard] = &sd
			g.status[cmd.Shard] = statusServing
		}
		return fmt.Sprintf("i.%d.%d", cmd.Num, cmd.Shard), nil
	case gcmdDelete:
		if cmd.Num == g.config.Num && g.status[cmd.Shard] == statusOffering {
			delete(g.shards, cmd.Shard)
			g.status[cmd.Shard] = statusAbsent
		}
		return fmt.Sprintf("d.%d.%d", cmd.Num, cmd.Shard), nil
	}
	panic("shard: unknown group cmd kind")
}

func (g *Group) applyKV(cmd groupCmd) kvResult {
	shard := Key2Shard(cmd.Key)
	// Ownership is re-checked at apply time: the config may have changed
	// between propose and apply, and every replica must make the same call.
	if g.config.Shards[shard] != g.gid || g.status[shard] != statusServing {
		return kvResult{err: ErrWrongGroup}
	}
	sd := g.shards[shard]
	switch cmd.OpKind {
	case opGet:
		return kvResult{err: OK, value: sd.Data[cmd.Key]}
	case opPut, opAppend:
		if cmd.Seq > sd.LastSeq[cmd.ClientID] {
			if cmd.OpKind == opPut {
				sd.Data[cmd.Key] = cmd.Value
			} else {
				sd.Data[cmd.Key] += cmd.Value
			}
			sd.LastSeq[cmd.ClientID] = cmd.Seq
		}
		return kvResult{err: OK}
	}
	return kvResult{err: ErrWrongGroup}
}

func (g *Group) applyConfig(next Config) {
	if next.Num != g.config.Num+1 {
		return // stale or premature proposal
	}
	for s := 0; s < NShards; s++ {
		if g.status[s] == statusPulling || g.status[s] == statusOffering {
			return // previous transition not settled; proposer raced
		}
	}
	old := g.config
	g.prevConfig = old
	g.config = next.Clone()
	for s := 0; s < NShards; s++ {
		from, to := old.Shards[s], next.Shards[s]
		switch {
		case to == g.gid && from == g.gid:
			// keep serving
		case to == g.gid && from == 0:
			sd := NewShardData()
			g.shards[s] = &sd
			g.status[s] = statusServing
		case to == g.gid:
			g.status[s] = statusPulling
		case from == g.gid:
			g.status[s] = statusOffering
		}
	}
}

type groupSnapshot struct {
	Config     Config
	PrevConfig Config
	Shards     map[int]ShardData
	Status     [NShards]shardStatus
}

// Snapshot implements stateMachine.
func (g *Group) Snapshot() []byte {
	st := groupSnapshot{
		Config:     g.config.Clone(),
		PrevConfig: g.prevConfig.Clone(),
		Shards:     make(map[int]ShardData, len(g.shards)),
		Status:     g.status,
	}
	for s, sd := range g.shards {
		st.Shards[s] = sd.Clone()
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&st); err != nil {
		panic("shard: group snapshot encode: " + err.Error())
	}
	return buf.Bytes()
}

// Restore implements stateMachine.
func (g *Group) Restore(snap []byte) {
	var st groupSnapshot
	if err := gob.NewDecoder(bytes.NewReader(snap)).Decode(&st); err != nil {
		panic("shard: group snapshot decode: " + err.Error())
	}
	g.config = st.Config
	g.prevConfig = st.PrevConfig
	g.status = st.Status
	g.shards = make(map[int]*ShardData, len(st.Shards))
	for s, sd := range st.Shards {
		c := sd.Clone()
		g.shards[s] = &c
	}
}
