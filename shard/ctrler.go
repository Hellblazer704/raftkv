package shard

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sort"

	"github.com/Hellblazer704/raftkv/raft"
)

// Ctrler is the shard controller: a replicated service (its own Raft group)
// whose state is the history of configs. Join/Leave/Move append a new config;
// Query reads one. All mutations are deduplicated per client session, exactly
// like the KV layer.
type Ctrler struct {
	rsm *replicator

	// Guarded by rsm's mutex (Apply/Snapshot/Restore/View).
	configs []Config // configs[i].Num == i
	lastSeq map[int64]int64
}

type ctrlerCmdKind uint8

const (
	cmdJoin ctrlerCmdKind = iota
	cmdLeave
	cmdMove
	cmdQuery
)

type ctrlerCmd struct {
	Kind     ctrlerCmdKind
	Servers  map[int][]string // Join
	GIDs     []int            // Leave
	Shard    int              // Move
	GID      int              // Move
	Num      int              // Query
	ClientID int64
	Seq      int64
}

// NewCtrler boots one controller replica.
func NewCtrler(me, n int, transport raft.Transport, storage raft.Storage, snapshotThreshold int, cfg raft.Config) *Ctrler {
	c := &Ctrler{
		configs: []Config{{Num: 0, Groups: map[int][]string{}}},
		lastSeq: make(map[int64]int64),
	}
	c.rsm = newReplicator(me, n, transport, storage, snapshotThreshold, cfg, c)
	return c
}

// Raft exposes the underlying node.
func (c *Ctrler) Raft() *raft.Raft { return c.rsm.rf }

// Kill stops the replica.
func (c *Ctrler) Kill() { c.rsm.Kill() }

func ctrlerTag(clientID, seq int64) string { return fmt.Sprintf("%d.%d", clientID, seq) }

func (c *Ctrler) mutate(cmd ctrlerCmd) Err {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&cmd); err != nil {
		panic("shard: ctrler cmd encode: " + err.Error())
	}
	_, err := c.rsm.propose(ctrlerTag(cmd.ClientID, cmd.Seq), buf.Bytes())
	return err
}

// Join handles new groups arriving.
func (c *Ctrler) Join(args *JoinArgs, reply *CtrlerReply) error {
	reply.Err = c.mutate(ctrlerCmd{Kind: cmdJoin, Servers: args.Servers, ClientID: args.ClientID, Seq: args.Seq})
	return nil
}

// Leave handles groups departing.
func (c *Ctrler) Leave(args *LeaveArgs, reply *CtrlerReply) error {
	reply.Err = c.mutate(ctrlerCmd{Kind: cmdLeave, GIDs: args.GIDs, ClientID: args.ClientID, Seq: args.Seq})
	return nil
}

// Move pins one shard to a group (operator override; the next Join/Leave
// rebalance may move it again).
func (c *Ctrler) Move(args *MoveArgs, reply *CtrlerReply) error {
	reply.Err = c.mutate(ctrlerCmd{Kind: cmdMove, Shard: args.Shard, GID: args.GID, ClientID: args.ClientID, Seq: args.Seq})
	return nil
}

// Query returns config Num (latest if Num is -1 or out of range). Reads go
// through the log, so a Query result is never stale.
func (c *Ctrler) Query(args *QueryArgs, reply *CtrlerReply) error {
	var buf bytes.Buffer
	cmd := ctrlerCmd{Kind: cmdQuery, Num: args.Num}
	if err := gob.NewEncoder(&buf).Encode(&cmd); err != nil {
		panic("shard: ctrler cmd encode: " + err.Error())
	}
	result, err := c.rsm.propose(fmt.Sprintf("q.%d", args.Num), buf.Bytes())
	reply.Err = err
	if err == OK {
		reply.Config = result.(Config)
	}
	return nil
}

// Apply implements stateMachine (called with the replicator lock held).
func (c *Ctrler) Apply(cmdBytes []byte) (string, any) {
	var cmd ctrlerCmd
	if err := gob.NewDecoder(bytes.NewReader(cmdBytes)).Decode(&cmd); err != nil {
		panic("shard: undecodable ctrler cmd")
	}
	if cmd.Kind == cmdQuery {
		num := cmd.Num
		if num < 0 || num >= len(c.configs) {
			num = len(c.configs) - 1
		}
		return fmt.Sprintf("q.%d", cmd.Num), c.configs[num].Clone()
	}
	tag := ctrlerTag(cmd.ClientID, cmd.Seq)
	if cmd.Seq <= c.lastSeq[cmd.ClientID] {
		return tag, nil // duplicate
	}
	c.lastSeq[cmd.ClientID] = cmd.Seq

	next := c.configs[len(c.configs)-1].Clone()
	next.Num++
	switch cmd.Kind {
	case cmdJoin:
		for gid, servers := range cmd.Servers {
			next.Groups[gid] = append([]string(nil), servers...)
		}
		rebalance(&next)
	case cmdLeave:
		for _, gid := range cmd.GIDs {
			delete(next.Groups, gid)
			for s := range next.Shards {
				if next.Shards[s] == gid {
					next.Shards[s] = 0
				}
			}
		}
		rebalance(&next)
	case cmdMove:
		next.Shards[cmd.Shard] = cmd.GID
	}
	c.configs = append(c.configs, next)
	return tag, nil
}

// rebalance evens shard counts across groups while moving as few shards as
// possible. It must be fully deterministic — every replica runs it on the
// same input and must produce the same config — so all iteration is over
// sorted slices, never map order.
func rebalance(cfg *Config) {
	gids := make([]int, 0, len(cfg.Groups))
	for gid := range cfg.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	if len(gids) == 0 {
		cfg.Shards = [NShards]int{}
		return
	}

	// Free any shard owned by a departed group.
	owned := make(map[int][]int, len(gids)) // gid -> shards, ascending
	var free []int
	for s, gid := range cfg.Shards {
		if _, ok := cfg.Groups[gid]; ok {
			owned[gid] = append(owned[gid], s)
		} else {
			free = append(free, s)
		}
	}

	// Targets: base each, first `extra` (in sorted gid order) get one more.
	base, extra := NShards/len(gids), NShards%len(gids)
	target := func(i int) int {
		if i < extra {
			return base + 1
		}
		return base
	}

	// Overloaded groups surrender their highest-numbered shards.
	for i, gid := range gids {
		for len(owned[gid]) > target(i) {
			last := len(owned[gid]) - 1
			free = append(free, owned[gid][last])
			owned[gid] = owned[gid][:last]
		}
	}
	sort.Ints(free)
	// Underloaded groups (sorted order) take from the free pool.
	for i, gid := range gids {
		for len(owned[gid]) < target(i) {
			s := free[0]
			free = free[1:]
			owned[gid] = append(owned[gid], s)
			cfg.Shards[s] = gid
		}
	}
}

type ctrlerSnapshot struct {
	Configs []Config
	LastSeq map[int64]int64
}

// Snapshot implements stateMachine.
func (c *Ctrler) Snapshot() []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&ctrlerSnapshot{Configs: c.configs, LastSeq: c.lastSeq}); err != nil {
		panic("shard: ctrler snapshot encode: " + err.Error())
	}
	return buf.Bytes()
}

// Restore implements stateMachine.
func (c *Ctrler) Restore(snap []byte) {
	var st ctrlerSnapshot
	if err := gob.NewDecoder(bytes.NewReader(snap)).Decode(&st); err != nil {
		panic("shard: ctrler snapshot decode: " + err.Error())
	}
	c.configs = st.Configs
	c.lastSeq = st.LastSeq
}
