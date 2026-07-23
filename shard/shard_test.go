package shard_test

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Hellblazer704/raftkv/raft"
	"github.com/Hellblazer704/raftkv/shard"
	"github.com/Hellblazer704/raftkv/sim"
)

// world wires a controller group and several shard groups onto one simulated
// network. Endpoint ids: controller replicas first, then group replicas,
// then clients.
type world struct {
	t    *testing.T
	net  *sim.Network
	seed int64

	ctrlerServers []string
	nextID        int

	mu     sync.Mutex
	groups map[int]*groupSet
}

type groupSet struct {
	gid      int
	ids      []int
	names    []string
	storages []*raft.MemoryStorage
	servers  []*shard.Group
}

var worldClients atomic.Int64

func newWorld(t *testing.T, seed int64) *world {
	w := &world{
		t:      t,
		net:    sim.NewNetwork(seed),
		seed:   seed,
		groups: make(map[int]*groupSet),
	}
	// Controller: 3 replicas at endpoints 0..2.
	n := 3
	for i := 0; i < n; i++ {
		w.ctrlerServers = append(w.ctrlerServers, fmt.Sprint(i))
	}
	for i := 0; i < n; i++ {
		c := shard.NewCtrler(i, n, &sim.RaftTransport{Net: w.net, From: i}, raft.NewMemoryStorage(), 4096, w.raftConfig(int64(i)))
		w.net.Register(i, shard.CtrlerService{C: c})
	}
	w.nextID = n
	return w
}

func (w *world) raftConfig(salt int64) raft.Config {
	return raft.Config{
		ElectionTimeoutMin: 250 * time.Millisecond,
		ElectionTimeoutMax: 450 * time.Millisecond,
		HeartbeatInterval:  70 * time.Millisecond,
		Seed:               w.seed*613 + salt,
	}
}

// addGroup boots a 3-replica shard group and registers it with the
// controller via Join.
func (w *world) addGroup(gid int) *groupSet {
	const n = 3
	gs := &groupSet{gid: gid}
	for i := 0; i < n; i++ {
		id := w.nextID
		w.nextID++
		gs.ids = append(gs.ids, id)
		gs.names = append(gs.names, fmt.Sprint(id))
		gs.storages = append(gs.storages, raft.NewMemoryStorage())
	}
	for i := 0; i < n; i++ {
		w.bootReplica(gs, i)
	}
	w.mu.Lock()
	w.groups[gid] = gs
	w.mu.Unlock()
	return gs
}

func (w *world) bootReplica(gs *groupSet, i int) {
	id := gs.ids[i]
	ctrler := shard.NewCtrlerClerk(shard.SimNameTransport{Net: w.net, From: id}, w.ctrlerServers)
	g := shard.NewGroup(gs.gid, i, len(gs.ids),
		&sim.RaftTransport{Net: w.net, From: id, Peers: gs.ids}, gs.storages[i], 2048,
		w.raftConfig(int64(id)*31), ctrler, shard.SimNameTransport{Net: w.net, From: id})
	for len(gs.servers) <= i {
		gs.servers = append(gs.servers, nil)
	}
	gs.servers[i] = g
	w.net.Register(id, shard.GroupService{G: g})
}

func (w *world) crashReplica(gid, i int) {
	w.mu.Lock()
	gs := w.groups[gid]
	w.mu.Unlock()
	if gs.servers[i] == nil {
		return
	}
	w.net.Deregister(gs.ids[i])
	gs.servers[i].Kill()
	gs.servers[i] = nil
}

func (w *world) restartReplica(gid, i int) {
	w.mu.Lock()
	gs := w.groups[gid]
	w.mu.Unlock()
	if gs.servers[i] != nil {
		return
	}
	w.bootReplica(gs, i)
}

func (w *world) shutdown() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, gs := range w.groups {
		for i, g := range gs.servers {
			if g != nil {
				w.net.Deregister(gs.ids[i])
				g.Kill()
			}
		}
	}
}

func (w *world) ctrlerClerk() *shard.CtrlerClerk {
	id := 100000 + int(worldClients.Add(1))
	w.net.Register(id, nil)
	w.net.SetGroup(id, sim.GroupAny)
	return shard.NewCtrlerClerk(shard.SimNameTransport{Net: w.net, From: id}, w.ctrlerServers)
}

func (w *world) clerk() *shard.Clerk {
	id := 200000 + int(worldClients.Add(1))
	w.net.Register(id, nil)
	w.net.SetGroup(id, sim.GroupAny)
	return shard.NewClerk(w.ctrlerClerk(), shard.SimNameTransport{Net: w.net, From: id})
}

func (w *world) join(gids ...int) {
	ck := w.ctrlerClerk()
	servers := map[int][]string{}
	for _, gid := range gids {
		gs := w.addGroup(gid)
		servers[gid] = gs.names
	}
	ck.Join(servers)
}

// checkBalanced asserts the config distributes shards evenly over its groups.
func checkBalanced(t *testing.T, cfg shard.Config) {
	t.Helper()
	if len(cfg.Groups) == 0 {
		return
	}
	counts := map[int]int{}
	for s, gid := range cfg.Shards {
		if gid == 0 {
			t.Fatalf("config %d: shard %d unassigned with %d groups", cfg.Num, s, len(cfg.Groups))
		}
		if _, ok := cfg.Groups[gid]; !ok {
			t.Fatalf("config %d: shard %d owned by departed group %d", cfg.Num, s, gid)
		}
		counts[gid]++
	}
	minC, maxC := shard.NShards, 0
	for gid := range cfg.Groups {
		c := counts[gid]
		if c < minC {
			minC = c
		}
		if c > maxC {
			maxC = c
		}
	}
	if maxC-minC > 1 {
		t.Fatalf("config %d unbalanced: min %d max %d (%v)", cfg.Num, minC, maxC, counts)
	}
}

func TestCtrlerRebalance(t *testing.T) {
	w := newWorld(t, 301)
	defer w.shutdown()
	ck := w.ctrlerClerk()

	w.join(101)
	cfg, _ := ck.Query(-1)
	checkBalanced(t, cfg)
	if cfg.Num != 1 {
		t.Fatalf("config num %d, want 1", cfg.Num)
	}

	w.join(102)
	cfg2, _ := ck.Query(-1)
	checkBalanced(t, cfg2)
	// Minimal movement: only shards that had to move moved (5 for 10 shards
	// going 1 group -> 2 groups).
	moved := 0
	for s := range cfg.Shards {
		if cfg2.Shards[s] != cfg.Shards[s] {
			moved++
		}
	}
	if moved != shard.NShards/2 {
		t.Fatalf("join moved %d shards, want %d", moved, shard.NShards/2)
	}

	w.join(103)
	cfg3, _ := ck.Query(-1)
	checkBalanced(t, cfg3)

	ck.Leave([]int{102})
	cfg4, _ := ck.Query(-1)
	checkBalanced(t, cfg4)
	// Shards that stayed with surviving groups must not have moved.
	for s := range cfg3.Shards {
		if cfg3.Shards[s] != 102 && cfg4.Shards[s] != cfg3.Shards[s] {
			t.Fatalf("leave moved shard %d owned by surviving group %d", s, cfg3.Shards[s])
		}
	}

	ck.Move(3, 101)
	cfg5, _ := ck.Query(-1)
	if cfg5.Shards[3] != 101 {
		t.Fatalf("move ignored: shard 3 on %d", cfg5.Shards[3])
	}

	// Query by number returns history.
	old, _ := ck.Query(1)
	if old.Num != 1 {
		t.Fatalf("query(1) returned config %d", old.Num)
	}
}

func TestShardBasicAndMigration(t *testing.T) {
	w := newWorld(t, 302)
	defer w.shutdown()
	w.join(101)
	ck := w.clerk()

	// Seed data spread over all shards.
	for i := 0; i < 20; i++ {
		ck.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("v%d", i))
	}
	// Second group joins: half the shards migrate; every key must survive.
	w.join(102)
	for i := 0; i < 20; i++ {
		if v := ck.Get(fmt.Sprintf("key%d", i)); v != fmt.Sprintf("v%d", i) {
			t.Fatalf("after join: key%d=%q", i, v)
		}
	}
	// First group leaves: everything migrates to 102.
	w.ctrlerClerk().Leave([]int{101})
	for i := 0; i < 20; i++ {
		if v := ck.Get(fmt.Sprintf("key%d", i)); v != fmt.Sprintf("v%d", i) {
			t.Fatalf("after leave: key%d=%q", i, v)
		}
	}
}

func TestSessionsSurviveMigration(t *testing.T) {
	w := newWorld(t, 303)
	defer w.shutdown()
	w.join(101)
	ck := w.clerk()
	w.net.SetReliable(false) // force retries -> duplicates in flight

	var tokens []string
	appendSome := func(from, to int) {
		for j := from; j < to; j++ {
			tok := fmt.Sprintf("(%d)", j)
			tokens = append(tokens, tok)
			ck.Append("migrating-key", tok)
		}
	}
	appendSome(0, 12)
	w.net.SetReliable(true)
	w.join(102) // key's shard may move to 102, carrying the session table
	w.net.SetReliable(false)
	appendSome(12, 24)
	w.net.SetReliable(true)

	value := ck.Get("migrating-key")
	for _, tok := range tokens {
		if c := strings.Count(value, tok); c != 1 {
			t.Fatalf("token %s appears %d times; dedup state lost in migration\nvalue=%q", tok, c, value)
		}
	}
}

func TestMigrationWithCrashesAndRestarts(t *testing.T) {
	w := newWorld(t, 304)
	defer w.shutdown()
	w.join(101, 102)
	ck := w.clerk()

	for i := 0; i < 10; i++ {
		ck.Put(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	// Crash one replica in each group, join a third group mid-flight,
	// restart the crashed replicas, then verify.
	w.crashReplica(101, 0)
	w.crashReplica(102, 1)
	w.join(103)
	time.Sleep(500 * time.Millisecond)
	w.restartReplica(101, 0)
	w.restartReplica(102, 1)
	for i := 0; i < 10; i++ {
		if v := ck.Get(fmt.Sprintf("k%d", i)); v != fmt.Sprintf("v%d", i) {
			t.Fatalf("k%d=%q", i, v)
		}
	}
}

// TestConcurrentOpsDuringChurn keeps clients appending while groups join and
// leave repeatedly; every append must land exactly once, in session order.
func TestConcurrentOpsDuringChurn(t *testing.T) {
	w := newWorld(t, 305)
	defer w.shutdown()
	w.join(101)

	const clerks = 3
	const perClerk = 30
	perTokens := make([][]string, clerks)
	var wg sync.WaitGroup
	for c := 0; c < clerks; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			ck := w.clerk()
			key := fmt.Sprintf("churn%d", c)
			for j := 0; j < perClerk; j++ {
				tok := fmt.Sprintf("(%d.%d)", c, j)
				perTokens[c] = append(perTokens[c], tok)
				ck.Append(key, tok)
			}
		}(c)
	}

	// Config churn while the appends run.
	admin := w.ctrlerClerk()
	w.join(102)
	time.Sleep(300 * time.Millisecond)
	w.join(103)
	time.Sleep(300 * time.Millisecond)
	admin.Leave([]int{101})
	time.Sleep(300 * time.Millisecond)
	w.join(104)
	wg.Wait()

	ck := w.clerk()
	for c := 0; c < clerks; c++ {
		value := ck.Get(fmt.Sprintf("churn%d", c))
		pos := -1
		for _, tok := range perTokens[c] {
			if n := strings.Count(value, tok); n != 1 {
				t.Fatalf("clerk %d token %s count %d\nvalue=%q", c, tok, n, value)
			}
			at := strings.Index(value, tok)
			if at < pos {
				t.Fatalf("clerk %d token %s out of order\nvalue=%q", c, tok, value)
			}
			pos = at
		}
	}
}

// TestOldOwnerGarbageCollects: after a shard moves, the previous owner must
// eventually drop its copy.
func TestOldOwnerGarbageCollects(t *testing.T) {
	w := newWorld(t, 306)
	defer w.shutdown()
	w.join(101)
	ck := w.clerk()
	for i := 0; i < 20; i++ {
		ck.Put(fmt.Sprintf("gc%d", i), strings.Repeat("x", 100))
	}
	w.join(102)
	// Wait for migration + GC to settle.
	time.Sleep(2 * time.Second)

	// The old group's replicas must no longer hold shards owned by 102.
	cfg, _ := w.ctrlerClerk().Query(-1)
	w.mu.Lock()
	gs := w.groups[101]
	w.mu.Unlock()
	for s, gid := range cfg.Shards {
		if gid != 102 {
			continue
		}
		for i, g := range gs.servers {
			if g == nil {
				continue
			}
			if g.HoldsShard(s) {
				t.Fatalf("group 101 replica %d still holds migrated shard %d", i, s)
			}
		}
	}
}
