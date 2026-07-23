package raft_test

// Cluster harness for torturing Raft on the simulated network. Modeled on
// the MIT 6.5840 test config: every applied command is cross-checked against
// what every other node applied at the same index, so any safety violation
// (divergent commits, out-of-order or duplicate applies) fails the test at
// the moment it happens.

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Hellblazer704/raftkv/raft"
	"github.com/Hellblazer704/raftkv/sim"
)

type cluster struct {
	t        *testing.T
	n        int
	seed     int64
	net      *sim.Network
	storages []*raft.MemoryStorage

	mu       sync.Mutex
	rafts    []*raft.Raft
	gen      []int         // incarnation counter per node; guards stale apply loops
	logs     []map[int]int // per-node applied commands by index
	applied  []int         // per-node highest applied index
	maxIndex int
	errMsg   string

	snapshotEvery int // if >0, service snapshots every N applied entries
}

// testSnapshot is the "service state" the harness snapshots: everything it
// has applied so far.
type testSnapshot struct {
	LastIndex int
	Cmds      map[int]int
}

func newCluster(t *testing.T, n int, seed int64, reliable bool) *cluster {
	c := &cluster{
		t:        t,
		n:        n,
		seed:     seed,
		net:      sim.NewNetwork(seed),
		storages: make([]*raft.MemoryStorage, n),
		rafts:    make([]*raft.Raft, n),
		gen:      make([]int, n),
		logs:     make([]map[int]int, n),
		applied:  make([]int, n),
	}
	c.net.SetReliable(reliable)
	for i := 0; i < n; i++ {
		c.storages[i] = raft.NewMemoryStorage()
	}
	for i := 0; i < n; i++ {
		c.start(i)
	}
	return c
}

func (c *cluster) config() raft.Config {
	return raft.Config{
		ElectionTimeoutMin: 250 * time.Millisecond,
		ElectionTimeoutMax: 450 * time.Millisecond,
		HeartbeatInterval:  70 * time.Millisecond,
	}
}

// start boots (or reboots) node i from its persistent storage.
func (c *cluster) start(i int) {
	c.mu.Lock()
	c.gen[i]++
	myGen := c.gen[i]
	c.logs[i] = make(map[int]int)
	c.applied[i] = 0
	c.mu.Unlock()

	applyCh := make(chan raft.ApplyMsg)
	cfg := c.config()
	cfg.Seed = c.seed*31 + int64(i) + int64(myGen)*1000
	rf := raft.Make(i, c.n, &sim.RaftTransport{Net: c.net, From: i}, c.storages[i], applyCh, cfg)

	c.mu.Lock()
	c.rafts[i] = rf
	c.mu.Unlock()

	go c.applyLoop(i, myGen, rf, applyCh)
	c.net.Register(i, &sim.RaftService{RF: rf})
}

func (c *cluster) applyLoop(i, myGen int, rf *raft.Raft, applyCh chan raft.ApplyMsg) {
	for msg := range applyCh {
		c.mu.Lock()
		if c.gen[i] != myGen {
			c.mu.Unlock()
			return
		}
		switch {
		case msg.SnapshotValid:
			var snap testSnapshot
			if err := gob.NewDecoder(bytes.NewReader(msg.Snapshot)).Decode(&snap); err != nil {
				c.errMsg = fmt.Sprintf("node %d: undecodable snapshot: %v", i, err)
			} else {
				c.logs[i] = make(map[int]int)
				for k, v := range snap.Cmds {
					c.logs[i][k] = v
				}
				c.applied[i] = snap.LastIndex
			}
		case msg.CommandValid:
			cmd, err := strconv.Atoi(string(msg.Command))
			if err != nil {
				c.errMsg = fmt.Sprintf("node %d: non-integer command at %d", i, msg.CommandIndex)
			}
			if msg.CommandIndex != c.applied[i]+1 {
				c.errMsg = fmt.Sprintf("node %d: applied index %d after %d (gap or duplicate)",
					i, msg.CommandIndex, c.applied[i])
			}
			for j := 0; j < c.n; j++ {
				if v, ok := c.logs[j][msg.CommandIndex]; ok && v != cmd {
					c.errMsg = fmt.Sprintf("commit inconsistency at index %d: node %d has %d, node %d applying %d",
						msg.CommandIndex, j, v, i, cmd)
				}
			}
			c.logs[i][msg.CommandIndex] = cmd
			c.applied[i] = msg.CommandIndex
			if msg.CommandIndex > c.maxIndex {
				c.maxIndex = msg.CommandIndex
			}
			if c.snapshotEvery > 0 && msg.CommandIndex%c.snapshotEvery == 0 {
				snap := testSnapshot{LastIndex: msg.CommandIndex, Cmds: make(map[int]int, len(c.logs[i]))}
				for k, v := range c.logs[i] {
					snap.Cmds[k] = v
				}
				var buf bytes.Buffer
				if err := gob.NewEncoder(&buf).Encode(&snap); err != nil {
					c.errMsg = fmt.Sprintf("node %d: snapshot encode: %v", i, err)
				}
				index := msg.CommandIndex
				c.mu.Unlock()
				rf.Snapshot(index, buf.Bytes())
				c.mu.Lock()
			}
		}
		c.mu.Unlock()
	}
}

func (c *cluster) checkErr() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.errMsg != "" {
		c.t.Fatal(c.errMsg)
	}
}

func (c *cluster) cleanup() {
	c.mu.Lock()
	rafts := append([]*raft.Raft(nil), c.rafts...)
	c.mu.Unlock()
	for _, rf := range rafts {
		if rf != nil {
			rf.Kill()
		}
	}
	c.checkErr()
}

func (c *cluster) raft(i int) *raft.Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rafts[i]
}

// disconnect isolates node i from the network without killing it.
func (c *cluster) disconnect(i int) { c.net.SetEnabled(i, false) }

func (c *cluster) connect(i int) { c.net.SetEnabled(i, true) }

// crash kills node i; its storage survives for a later restart.
func (c *cluster) crash(i int) {
	c.net.Deregister(i)
	c.mu.Lock()
	rf := c.rafts[i]
	c.rafts[i] = nil
	c.gen[i]++ // invalidate the incarnation's apply loop
	c.mu.Unlock()
	if rf != nil {
		rf.Kill()
	}
}

// restart reboots node i from its persisted state.
func (c *cluster) restart(i int) {
	c.crash(i)
	c.start(i)
}

// checkOneLeader waits for exactly one leader among connected nodes.
func (c *cluster) checkOneLeader() int {
	for iter := 0; iter < 10; iter++ {
		time.Sleep(time.Duration(450+iter*50) * time.Millisecond)
		leadersByTerm := make(map[int][]int)
		for i := 0; i < c.n; i++ {
			if rf := c.raft(i); rf != nil {
				if term, isLeader := rf.GetState(); isLeader {
					leadersByTerm[term] = append(leadersByTerm[term], i)
				}
			}
		}
		lastTerm, leader := -1, -1
		for term, ids := range leadersByTerm {
			if len(ids) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ids), ids)
			}
			if term > lastTerm {
				lastTerm, leader = term, ids[0]
			}
		}
		if leader != -1 {
			return leader
		}
	}
	c.t.Fatal("no leader elected")
	return -1
}

// checkNoLeader asserts no connected node claims leadership.
func (c *cluster) checkNoLeader() {
	for i := 0; i < c.n; i++ {
		if rf := c.raft(i); rf != nil && c.isConnected(i) {
			if _, isLeader := rf.GetState(); isLeader {
				c.t.Fatalf("node %d claims leadership without a quorum", i)
			}
		}
	}
}

func (c *cluster) isConnected(i int) bool {
	// The harness tracks connectivity implicitly; tests only call
	// checkNoLeader after disconnecting a known set, so query the network.
	return c.net.Enabled(i)
}

// checkTerms asserts all connected nodes agree on the term, returning it.
func (c *cluster) checkTerms() int {
	term := -1
	for i := 0; i < c.n; i++ {
		if rf := c.raft(i); rf != nil && c.isConnected(i) {
			t, _ := rf.GetState()
			if term == -1 {
				term = t
			} else if term != t {
				c.t.Fatalf("nodes disagree on term: %d vs %d", term, t)
			}
		}
	}
	return term
}

// nCommitted counts how many nodes have applied index, and returns the value.
func (c *cluster) nCommitted(index int) (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	count, value := 0, -1
	for i := 0; i < c.n; i++ {
		if v, ok := c.logs[i][index]; ok {
			if count > 0 && v != value {
				c.t.Fatalf("committed values differ at index %d: %d vs %d", index, value, v)
			}
			count++
			value = v
		}
	}
	return count, value
}

// one submits cmd through whichever node is leader and waits until at least
// `expected` nodes have applied it. With retry, it re-submits after leader
// changes (so the command may commit more than once under faults — callers
// that care use unique commands).
func (c *cluster) one(cmd int, expected int, retry bool) int {
	command := []byte(strconv.Itoa(cmd))
	deadline := time.Now().Add(15 * time.Second)
	next := 0
	for time.Now().Before(deadline) {
		index := -1
		for range make([]struct{}, c.n) {
			next = (next + 1) % c.n
			if rf := c.raft(next); rf != nil {
				if i, _, isLeader := rf.Start(command); isLeader {
					index = i
					break
				}
			}
		}
		if index == -1 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		commitDeadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(commitDeadline) {
			count, value := c.nCommitted(index)
			if count >= expected && value == cmd {
				c.checkErr()
				return index
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !retry {
			c.t.Fatalf("command %d did not commit at index %d", cmd, index)
		}
	}
	c.t.Fatalf("command %d never committed", cmd)
	return -1
}

// wait blocks until at least n nodes have applied index (with a generous
// timeout that scales while leaders churn), returning the value.
func (c *cluster) wait(index, n int, startTerm int) int {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		count, _ := c.nCommitted(index)
		if count >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > -1 {
			for i := 0; i < c.n; i++ {
				if rf := c.raft(i); rf != nil {
					if t, _ := rf.GetState(); t > startTerm {
						return -1 // term moved on; caller re-submits
					}
				}
			}
		}
	}
	count, value := c.nCommitted(index)
	if count < n {
		c.t.Fatalf("only %d of %d nodes committed index %d", count, n, index)
	}
	return value
}
