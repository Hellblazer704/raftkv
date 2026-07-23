package chaos

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hellblazer704/raftkv/linz"
	"github.com/Hellblazer704/raftkv/raft"
	"github.com/Hellblazer704/raftkv/sim"
)

// Cluster is a KV service on the simulated network plus the levers the
// nemesis pulls: crash/restart, partitions, disk faults, unreliable delivery.
type Cluster struct {
	N     int
	Net   *sim.Network
	Seed  int64
	epoch time.Time

	mu       sync.Mutex
	storages []*raft.MemoryStorage
	servers  []*Server
	gen      []int

	snapshotThreshold int
}

// NewCluster boots n replicas. snapshotThreshold (bytes of un-compacted log)
// keeps snapshot/InstallSnapshot paths hot during chaos runs.
func NewCluster(n int, seed int64, snapshotThreshold int) *Cluster {
	c := &Cluster{
		N:                 n,
		Net:               sim.NewNetwork(seed),
		Seed:              seed,
		epoch:             time.Now(),
		storages:          make([]*raft.MemoryStorage, n),
		servers:           make([]*Server, n),
		gen:               make([]int, n),
		snapshotThreshold: snapshotThreshold,
	}
	for i := 0; i < n; i++ {
		c.storages[i] = raft.NewMemoryStorage()
		c.startServer(i)
	}
	return c
}

func (c *Cluster) config(i int) raft.Config {
	return raft.Config{
		ElectionTimeoutMin: 250 * time.Millisecond,
		ElectionTimeoutMax: 450 * time.Millisecond,
		HeartbeatInterval:  70 * time.Millisecond,
		Seed:               c.Seed*131 + int64(i)*7 + int64(c.gen[i])*10007,
		// Mild clock skew per node: timers on node i run up to ±15% off.
		ClockSkew: 1.0 + 0.15*float64(i%3-1),
	}
}

func (c *Cluster) startServer(i int) {
	c.gen[i]++
	s := NewServer(i, c.N, c.Net, c.storages[i], c.snapshotThreshold, c.config(i))
	c.servers[i] = s
	c.Net.Register(i, s)
}

// Crash kills replica i, keeping its storage for a later Restart.
func (c *Cluster) Crash(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.servers[i] == nil {
		return
	}
	c.Net.Deregister(i)
	c.servers[i].Kill()
	c.servers[i] = nil
}

// Restart reboots replica i from its persisted storage.
func (c *Cluster) Restart(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.servers[i] != nil {
		return
	}
	c.storages[i].SetFailWrites(false)
	c.startServer(i)
}

// Alive reports whether replica i is running.
func (c *Cluster) Alive(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[i] != nil
}

// AliveCount returns how many replicas are running.
func (c *Cluster) AliveCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, s := range c.servers {
		if s != nil {
			n++
		}
	}
	return n
}

// DiskFull makes replica i's next storage write fail; Raft treats that as
// fatal and halts the node (as etcd does), so the replica needs DiskRepair +
// Restart to rejoin.
func (c *Cluster) DiskFull(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.storages[i].SetFailWrites(true)
}

// Leader returns the current leader's id, or -1.
func (c *Cluster) Leader() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, s := range c.servers {
		if s != nil {
			if _, isLeader := s.RF.GetState(); isLeader {
				return i
			}
		}
	}
	return -1
}

// Shutdown kills everything.
func (c *Cluster) Shutdown() {
	for i := 0; i < c.N; i++ {
		c.Crash(i)
	}
}

// now returns nanoseconds since the cluster epoch (history timestamps).
func (c *Cluster) now() int64 { return int64(time.Since(c.epoch)) }

var clientIDs atomic.Int64

// Client issues ops against the cluster and records a linearizability
// history. Each client owns a network endpoint (GroupAny: clients see across
// partitions, they just can't make servers agree).
type Client struct {
	c      *Cluster
	id     int
	seq    int64
	leader int

	mu      sync.Mutex
	history []linz.Op
}

// NewClient attaches a fresh client endpoint to the network.
func (c *Cluster) NewClient() *Client {
	id := c.N + int(clientIDs.Add(1))
	c.Net.Register(id, nil)
	c.Net.SetGroup(id, sim.GroupAny)
	return &Client{c: c, id: id}
}

// History returns the ops recorded so far.
func (cl *Client) History() []linz.Op {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return append([]linz.Op(nil), cl.history...)
}

func (cl *Client) nonce() int64 {
	cl.seq++
	return int64(cl.id)<<32 | cl.seq
}

// Put writes key=value. Ambiguous outcomes are recorded as indeterminate and
// never retried (see package comment).
func (cl *Client) Put(key, value string) bool {
	return cl.write(linz.Put, key, value)
}

// Append appends value to key.
func (cl *Client) Append(key, value string) bool {
	return cl.write(linz.Append, key, value)
}

func (cl *Client) write(kind linz.Kind, key, value string) bool {
	cmd := Command{Kind: kind, Key: key, Value: value, Nonce: cl.nonce()}
	call := cl.c.now()
	err := cl.attempt(cmd)
	ret := cl.c.now()
	op := linz.Op{Client: cl.id, Kind: kind, Key: key, Value: value, Call: call, Return: ret}
	switch err {
	case replyOK:
		cl.record(op)
		return true
	case replyMaybe:
		op.Return = math.MaxInt64
		cl.record(op)
		return false
	default: // definitely not executed
		return false
	}
}

// Get reads key. Failed reads are discarded (no side effects to model).
func (cl *Client) Get(key string) (string, bool) {
	cmd := Command{Kind: linz.Get, Key: key, Nonce: cl.nonce()}
	call := cl.c.now()
	args := &OpArgs{Cmd: cmd}
	for attempt := 0; attempt < 2*cl.c.N; attempt++ {
		target := (cl.leader + attempt) % cl.c.N
		reply := &OpReply{}
		if !cl.c.Net.Call(cl.id, target, "KV.Op", args, reply, 64) {
			continue
		}
		if reply.Err == replyOK {
			cl.leader = target
			cl.record(linz.Op{
				Client: cl.id, Kind: linz.Get, Key: key, Output: reply.Value,
				Call: call, Return: cl.c.now(),
			})
			return reply.Value, true
		}
	}
	return "", false
}

// attempt sends the command until it gets a definite answer or an ambiguous
// failure. Distinct servers may be tried only after *definite* non-execution
// (wrong_leader); any ambiguous outcome stops the attempt, because sending
// the same command to a second server could execute it twice.
func (cl *Client) attempt(cmd Command) string {
	args := &OpArgs{Cmd: cmd}
	for attempt := 0; attempt < 2*cl.c.N; attempt++ {
		target := (cl.leader + attempt) % cl.c.N
		reply := &OpReply{}
		if !cl.c.Net.Call(cl.id, target, "KV.Op", args, reply, 64+len(cmd.Value)) {
			// The request may have reached a leader and executed with the
			// reply lost — ambiguous, so this op is done. But rotate the
			// cached leader so the *next* op doesn't hammer a dead node
			// forever (a liveness bug the chaos suite caught; see BUGS.md).
			cl.leader = (target + 1) % cl.c.N
			return replyMaybe
		}
		switch reply.Err {
		case replyOK:
			cl.leader = target
			return replyOK
		case replyWrongLeader:
			continue
		default:
			return replyMaybe
		}
	}
	return replyMaybe
}

func (cl *Client) record(op linz.Op) {
	cl.mu.Lock()
	cl.history = append(cl.history, op)
	cl.mu.Unlock()
}
