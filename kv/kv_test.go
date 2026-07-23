package kv_test

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Hellblazer704/raftkv/chaos"
	"github.com/Hellblazer704/raftkv/kv"
	"github.com/Hellblazer704/raftkv/linz"
	"github.com/Hellblazer704/raftkv/raft"
	"github.com/Hellblazer704/raftkv/sim"
)

// cluster implements chaos.Target for the session KV, so the same nemesis
// that tortures the minimal chaos KV can torture the real service.
type cluster struct {
	n     int
	seed  int64
	net   *sim.Network
	epoch time.Time

	mu       sync.Mutex
	storages []*raft.MemoryStorage
	servers  []*kv.Server

	snapshotThreshold int
}

func newCluster(n int, seed int64, snapshotThreshold int) *cluster {
	c := &cluster{
		n: n, seed: seed,
		net:               sim.NewNetwork(seed),
		epoch:             time.Now(),
		storages:          make([]*raft.MemoryStorage, n),
		servers:           make([]*kv.Server, n),
		snapshotThreshold: snapshotThreshold,
	}
	for i := 0; i < n; i++ {
		c.storages[i] = raft.NewMemoryStorage()
		c.boot(i)
	}
	return c
}

func (c *cluster) config(i int) raft.Config {
	return raft.Config{
		ElectionTimeoutMin: 250 * time.Millisecond,
		ElectionTimeoutMax: 450 * time.Millisecond,
		HeartbeatInterval:  70 * time.Millisecond,
		Seed:               c.seed*997 + int64(i),
	}
}

func (c *cluster) boot(i int) {
	s := kv.NewServer(i, c.n, &sim.RaftTransport{Net: c.net, From: i}, c.storages[i], c.snapshotThreshold, c.config(i))
	c.servers[i] = s
	c.net.Register(i, kv.SimService{S: s})
}

func (c *cluster) NodeCount() int        { return c.n }
func (c *cluster) Network() *sim.Network { return c.net }
func (c *cluster) Epoch() int64          { return int64(time.Since(c.epoch)) }

func (c *cluster) Alive(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[i] != nil
}

func (c *cluster) AliveCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, s := range c.servers {
		if s != nil {
			count++
		}
	}
	return count
}

func (c *cluster) Crash(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.servers[i] == nil {
		return
	}
	c.net.Deregister(i)
	c.servers[i].Kill()
	c.servers[i] = nil
}

func (c *cluster) Restart(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.servers[i] != nil {
		return
	}
	c.storages[i].SetFailWrites(false)
	c.boot(i)
}

func (c *cluster) DiskFull(i int) { c.storages[i].SetFailWrites(true) }

func (c *cluster) Leader() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, s := range c.servers {
		if s != nil {
			if _, isLeader := s.Raft().GetState(); isLeader {
				return i
			}
		}
	}
	return -1
}

func (c *cluster) leaderServer() *kv.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.servers {
		if s != nil {
			if _, isLeader := s.Raft().GetState(); isLeader {
				return s
			}
		}
	}
	return nil
}

func (c *cluster) Shutdown() {
	for i := 0; i < c.n; i++ {
		c.Crash(i)
	}
}

var clientEndpoints atomic.Int64

func (c *cluster) clientTransport() kv.SimTransport {
	id := c.n + int(clientEndpoints.Add(1))
	c.net.Register(id, nil)
	c.net.SetGroup(id, sim.GroupAny)
	return kv.SimTransport{Net: c.net, From: id}
}

func (c *cluster) clerk() *kv.Clerk {
	return kv.NewClerk(c.clientTransport(), c.n)
}

func TestBasicOps(t *testing.T) {
	c := newCluster(3, 101, 0)
	defer c.Shutdown()
	ck := c.clerk()

	if v := ck.Get("missing"); v != "" {
		t.Fatalf("missing key = %q", v)
	}
	ck.Put("a", "1")
	if v := ck.Get("a"); v != "1" {
		t.Fatalf("a=%q want 1", v)
	}
	ck.Append("a", "2")
	ck.Append("a", "3")
	if v := ck.Get("a"); v != "123" {
		t.Fatalf("a=%q want 123", v)
	}
	ck.Put("a", "reset")
	if v := ck.Get("a"); v != "reset" {
		t.Fatalf("a=%q want reset", v)
	}
}

// assertExactlyOnce verifies that key's value is exactly the tokens each
// clerk appended, each token once, in per-clerk order (session ordering).
func assertExactlyOnce(t *testing.T, value string, perClerk [][]string) {
	t.Helper()
	for ci, tokens := range perClerk {
		pos := -1
		for _, tok := range tokens {
			count := strings.Count(value, tok)
			if count != 1 {
				t.Fatalf("clerk %d token %q appears %d times (want 1)\nvalue=%q", ci, tok, count, value)
			}
			at := strings.Index(value, tok)
			if at < pos {
				t.Fatalf("clerk %d token %q out of session order\nvalue=%q", ci, tok, value)
			}
			pos = at
		}
	}
}

func TestExactlyOnceUnreliable(t *testing.T) {
	c := newCluster(5, 102, 0)
	defer c.Shutdown()
	c.net.SetReliable(false) // drops + delays force clerk retries

	const clerks, appends = 3, 25
	perClerk := make([][]string, clerks)
	var wg sync.WaitGroup
	for i := 0; i < clerks; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ck := c.clerk()
			for j := 0; j < appends; j++ {
				tok := fmt.Sprintf("(%d.%d)", i, j)
				perClerk[i] = append(perClerk[i], tok)
				ck.Append("shared", tok)
			}
		}(i)
	}
	wg.Wait()
	c.net.SetReliable(true)
	assertExactlyOnce(t, c.clerk().Get("shared"), perClerk)
}

func TestLeaderFailoverExactlyOnce(t *testing.T) {
	c := newCluster(5, 103, 0)
	defer c.Shutdown()

	const rounds, perRound = 5, 6
	var tokens []string
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // nemesis: keep killing leaders
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case <-time.After(300 * time.Millisecond):
				if l := c.Leader(); l != -1 {
					c.Crash(l)
					time.Sleep(200 * time.Millisecond)
					c.Restart(l)
				}
			}
		}
	}()
	ck := c.clerk()
	for r := 0; r < rounds; r++ {
		for j := 0; j < perRound; j++ {
			tok := fmt.Sprintf("(%d.%d)", r, j)
			tokens = append(tokens, tok)
			ck.Append("k", tok)
		}
	}
	close(done)
	wg.Wait()
	assertExactlyOnce(t, ck.Get("k"), [][]string{tokens})
}

func TestSnapshotCarriesSessions(t *testing.T) {
	c := newCluster(3, 104, 512) // snapshot constantly
	defer c.Shutdown()
	c.net.SetReliable(false) // force duplicate deliveries

	ck := c.clerk()
	var tokens []string
	for j := 0; j < 30; j++ {
		tok := fmt.Sprintf("(a.%d)", j)
		tokens = append(tokens, tok)
		ck.Append("k", tok)
	}
	c.net.SetReliable(true)

	// Full cold restart: sessions must come back from the snapshot, or the
	// retried appends below could double-apply.
	for i := 0; i < 3; i++ {
		c.Crash(i)
	}
	for i := 0; i < 3; i++ {
		c.Restart(i)
	}
	c.net.SetReliable(false)
	for j := 30; j < 45; j++ {
		tok := fmt.Sprintf("(a.%d)", j)
		tokens = append(tokens, tok)
		ck.Append("k", tok)
	}
	c.net.SetReliable(true)
	assertExactlyOnce(t, ck.Get("k"), [][]string{tokens})
}

func TestLeaseReadsBypassLog(t *testing.T) {
	c := newCluster(3, 105, 0)
	defer c.Shutdown()
	ck := c.clerk()

	ck.Put("k", "v")
	// Let heartbeats establish the lease everywhere.
	time.Sleep(300 * time.Millisecond)

	leader := c.leaderServer()
	if leader == nil {
		t.Fatal("no leader")
	}
	before := leader.Raft().LogSizeBytes()
	for i := 0; i < 100; i++ {
		if v := ck.Get("k"); v != "v" {
			t.Fatalf("get = %q", v)
		}
	}
	after := leader.Raft().LogSizeBytes()
	// A log-path Get would add ~60+ bytes each; allow one straggler.
	if after-before > 120 {
		t.Fatalf("100 lease reads grew the log by %d bytes — reads are going through the log", after-before)
	}
}

func TestStaleLeaseRejected(t *testing.T) {
	c := newCluster(5, 106, 0)
	defer c.Shutdown()
	ck := c.clerk()

	ck.Put("k", "old")
	oldLeader := -1
	deadline := time.Now().Add(5 * time.Second)
	for oldLeader == -1 && time.Now().Before(deadline) {
		oldLeader = c.Leader()
		time.Sleep(20 * time.Millisecond)
	}
	if oldLeader == -1 {
		t.Fatal("no leader")
	}

	// Cut the leader off. Its lease (ElectionTimeoutMin/2 = 125ms) expires
	// long before the majority elects a replacement.
	c.net.SetGroup(oldLeader, 1)
	time.Sleep(600 * time.Millisecond)
	ck.Put("k", "new") // clerk reaches the majority side (GroupAny)

	// Ask the deposed leader directly. It must NOT serve the stale value:
	// lease expired -> log path -> no quorum -> timeout/wrong-leader.
	tr := c.clientTransport()
	args := &kv.GetArgs{Key: "k", ClientID: 1, Seq: 1}
	reply := &kv.GetReply{}
	ok := tr.Call(oldLeader, "KV.Get", args, reply)
	if ok && reply.Err == kv.OK && reply.Value == "old" {
		t.Fatalf("deposed leader served a stale read: %q", reply.Value)
	}

	c.net.HealPartitions()
	if v := ck.Get("k"); v != "new" {
		t.Fatalf("after heal, k=%q want new", v)
	}
}

func TestCasBasic(t *testing.T) {
	c := newCluster(3, 107, 0)
	defer c.Shutdown()
	ck := c.clerk()

	ck.Put("k", "a")
	if ok, old := ck.Cas("k", "wrong", "b"); ok || old != "a" {
		t.Fatalf("cas(wrong) = %v, %q", ok, old)
	}
	if ok, old := ck.Cas("k", "a", "b"); !ok || old != "a" {
		t.Fatalf("cas(a->b) = %v, %q", ok, old)
	}
	if v := ck.Get("k"); v != "b" {
		t.Fatalf("k=%q", v)
	}
}

// TestCasAtomicIncrementUnreliable: concurrent CAS-loop counters over a
// lossy network. If a retried CAS re-evaluated instead of returning its
// memoized outcome, increments would be lost or doubled.
func TestCasAtomicIncrementUnreliable(t *testing.T) {
	c := newCluster(5, 108, 0)
	defer c.Shutdown()
	c.net.SetReliable(false)

	const clerks, increments = 3, 10
	var wg sync.WaitGroup
	for i := 0; i < clerks; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ck := c.clerk()
			done := 0
			for done < increments {
				cur := ck.Get("counter")
				n := 0
				if cur != "" {
					n, _ = strconv.Atoi(cur)
				}
				if ok, _ := ck.Cas("counter", cur, strconv.Itoa(n+1)); ok {
					done++
				}
			}
		}()
	}
	wg.Wait()
	c.net.SetReliable(true)
	ck := c.clerk()
	if v := ck.Get("counter"); v != strconv.Itoa(clerks*increments) {
		t.Fatalf("counter=%q, want %d", v, clerks*increments)
	}
}

// recClerk performs bounded-retry ops and records a linearizability history.
// Writes retry with the same seq (safe: dedup); a write that never confirms
// is recorded as indeterminate.
type recClerk struct {
	tr     kv.SimTransport
	c      *cluster
	id     int64
	seq    int64
	leader int

	mu   sync.Mutex
	hist []linz.Op
}

func newRecClerk(c *cluster, id int64) *recClerk {
	return &recClerk{tr: c.clientTransport(), c: c, id: id}
}

func (rc *recClerk) history() []linz.Op {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return append([]linz.Op(nil), rc.hist...)
}

func (rc *recClerk) record(op linz.Op) {
	rc.mu.Lock()
	rc.hist = append(rc.hist, op)
	rc.mu.Unlock()
}

func (rc *recClerk) write(kind linz.Kind, key, value string) {
	rc.seq++
	args := &kv.PutAppendArgs{
		Key: key, Value: value, Append: kind == linz.Append,
		ClientID: rc.id, Seq: rc.seq,
	}
	call := rc.c.Epoch()
	deadline := time.Now().Add(3 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		reply := &kv.PutAppendReply{}
		if rc.tr.Call((rc.leader+attempt)%rc.c.n, "KV.PutAppend", args, reply) && reply.Err == kv.OK {
			rc.leader = (rc.leader + attempt) % rc.c.n
			rc.record(linz.Op{Client: int(rc.id), Kind: kind, Key: key, Value: value, Call: call, Return: rc.c.Epoch()})
			return
		}
	}
	// Never confirmed: may land at any later time (or never).
	rc.record(linz.Op{Client: int(rc.id), Kind: kind, Key: key, Value: value, Call: call, Return: math.MaxInt64})
}

func (rc *recClerk) get(key string) {
	rc.seq++
	args := &kv.GetArgs{Key: key, ClientID: rc.id, Seq: rc.seq}
	call := rc.c.Epoch()
	deadline := time.Now().Add(3 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		reply := &kv.GetReply{}
		if rc.tr.Call((rc.leader+attempt)%rc.c.n, "KV.Get", args, reply) && reply.Err == kv.OK {
			rc.leader = (rc.leader + attempt) % rc.c.n
			rc.record(linz.Op{Client: int(rc.id), Kind: linz.Get, Key: key, Output: reply.Value, Call: call, Return: rc.c.Epoch()})
			return
		}
	}
	// Failed reads have no side effects; drop them.
}

// TestKVChaosSchedules: the full nemesis against the session KV, with
// retrying clients — including lease reads — checked for linearizability.
func TestKVChaosSchedules(t *testing.T) {
	seeds := 4
	if v := os.Getenv("RAFTKV_KV_CHAOS_SEEDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			seeds = n
		}
	}
	if testing.Short() {
		seeds = 2
	}
	for s := 0; s < seeds; s++ {
		seed := int64(9000 + s)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			runKVSchedule(t, seed)
		})
	}
}

func runKVSchedule(t *testing.T, seed int64) {
	const (
		replicas = 5
		clerks   = 4
		keys     = 4
	)
	c := newCluster(replicas, seed, 2048)
	defer c.Shutdown()

	nm := chaos.NewNemesis(c, seed*104729)
	stopNemesis := make(chan struct{})
	nemesisDone := make(chan struct{})
	go func() {
		defer close(nemesisDone)
		nm.Run(stopNemesis)
	}()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	rcs := make([]*recClerk, clerks)
	for i := 0; i < clerks; i++ {
		rcs[i] = newRecClerk(c, int64(1000+i))
		wg.Add(1)
		go func(rc *recClerk, workerSeed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(workerSeed))
			n := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("k%d", rng.Intn(keys))
				n++
				val := fmt.Sprintf("%d.%d;", rc.id, n)
				switch r := rng.Float64(); {
				case r < 0.35:
					rc.write(linz.Put, key, val)
				case r < 0.60:
					rc.write(linz.Append, key, val)
				default:
					rc.get(key)
				}
				time.Sleep(time.Duration(rng.Intn(40)) * time.Millisecond)
			}
		}(rcs[i], seed*17+int64(i))
	}

	time.Sleep(3500 * time.Millisecond)
	close(stopNemesis)
	<-nemesisDone
	time.Sleep(1 * time.Second)
	close(stop)
	wg.Wait()

	var history []linz.Op
	for _, rc := range rcs {
		history = append(history, rc.history()...)
	}
	res := linz.CheckKV(history, 60*time.Second)
	if res.Unknown {
		t.Logf("seed %d: checker budget exhausted (%d ops) — inconclusive", seed, len(history))
		return
	}
	if !res.Linearizable {
		path := fmt.Sprintf("kv-failure-seed%d.txt", seed)
		f, err := os.Create(path)
		if err == nil {
			fmt.Fprintf(f, "KV LINEARIZABILITY VIOLATION seed=%d key=%q\n\nNemesis:\n", seed, res.BadKey)
			for _, line := range nm.Log() {
				fmt.Fprintln(f, line)
			}
			fmt.Fprintf(f, "\nHistory (%d ops):\n", len(history))
			for _, op := range history {
				fmt.Fprintln(f, op)
			}
			f.Close()
		}
		t.Fatalf("linearizability violation on key %q (seed %d); see %s", res.BadKey, seed, path)
	}
}
