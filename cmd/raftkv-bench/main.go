// raftkv-bench drives YCSB-style workloads against an in-process raftkv
// cluster and prints markdown result rows (BENCHMARKS.md is assembled from
// its output).
//
// The cluster runs on the in-memory simulated network in reliable mode, so
// results measure consensus-path cost (Raft bookkeeping, apply pipeline,
// fsync policy), not wire latency. -storage wal puts a real fsync'ing WAL
// under every replica, which is where the honest write ceiling appears.
//
// Modes:
//
//	-mode ycsb     -workload a|b|c|d|f  throughput + latency percentiles
//	-mode failover                      leader-kill -> first-success times
//	-mode recovery                      snapshot catch-up after a cold restart
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/Hellblazer704/raftkv/kv"
	"github.com/Hellblazer704/raftkv/raft"
	"github.com/Hellblazer704/raftkv/raft/wal"
	"github.com/Hellblazer704/raftkv/sim"
)

var (
	mode      = flag.String("mode", "ycsb", "ycsb | failover | recovery")
	workload  = flag.String("workload", "a", "YCSB workload: a, b, c, d, f")
	nodes     = flag.Int("nodes", 3, "cluster size")
	clients   = flag.Int("clients", 32, "concurrent clerks")
	duration  = flag.Duration("duration", 8*time.Second, "measurement window")
	records   = flag.Int("records", 1000, "preloaded keys")
	valueSize = flag.Int("valuesize", 100, "value bytes")
	storage   = flag.String("storage", "mem", "mem | wal (wal = real fsync per mutation)")
	rounds    = flag.Int("rounds", 20, "failover rounds")
	seed      = flag.Int64("seed", 1, "workload seed")
	snapAt    = flag.Int("snapthreshold", 1<<18, "un-compacted log bytes that trigger a snapshot")
)

type cluster struct {
	n        int
	net      *sim.Network
	servers  []*kv.Server
	storages []raft.Storage
	walDirs  []string
}

func newCluster(n int) *cluster {
	c := &cluster{n: n, net: sim.NewNetwork(*seed)}
	for i := 0; i < n; i++ {
		var store raft.Storage = raft.NewMemoryStorage()
		if *storage == "wal" {
			dir, err := os.MkdirTemp("", "raftkv-bench-*")
			if err != nil {
				panic(err)
			}
			c.walDirs = append(c.walDirs, dir)
			w, err := wal.Open(filepath.Join(dir, fmt.Sprint(i)))
			if err != nil {
				panic(err)
			}
			store = w
		}
		c.storages = append(c.storages, store)
		s := kv.NewServer(i, n, &sim.RaftTransport{Net: c.net, From: i}, store, *snapAt, c.raftConfig(int64(i)))
		c.servers = append(c.servers, s)
		c.net.Register(i, kv.SimService{S: s})
	}
	return c
}

func (c *cluster) raftConfig(salt int64) raft.Config {
	return raft.Config{
		ElectionTimeoutMin: 250 * time.Millisecond,
		ElectionTimeoutMax: 450 * time.Millisecond,
		HeartbeatInterval:  70 * time.Millisecond,
		Seed:               *seed*97 + salt,
	}
}

func (c *cluster) shutdown() {
	for _, s := range c.servers {
		s.Kill()
	}
	for _, d := range c.walDirs {
		os.RemoveAll(d)
	}
}

var clientIDs int

func (c *cluster) clerk() *kv.Clerk {
	clientIDs++
	id := c.n + clientIDs
	c.net.Register(id, nil)
	c.net.SetGroup(id, sim.GroupAny)
	return kv.NewClerk(kv.SimTransport{Net: c.net, From: id}, c.n)
}

func (c *cluster) leader() *kv.Server {
	for _, s := range c.servers {
		if _, isLeader := s.Raft().GetState(); isLeader {
			return s
		}
	}
	return nil
}

func (c *cluster) waitLeader() {
	for c.leader() == nil {
		time.Sleep(20 * time.Millisecond)
	}
}

func main() {
	flag.Parse()
	switch *mode {
	case "ycsb":
		runYCSB()
	case "failover":
		runFailover()
	case "recovery":
		runRecovery()
	default:
		fmt.Fprintln(os.Stderr, "unknown mode", *mode)
		os.Exit(2)
	}
}

// opMix returns (readFrac, rmwFrac, insertFrac) for a workload; the
// remainder is updates. Workload E (scans) is omitted: raftkv has no range
// scans, and faking them would fake the numbers.
func opMix(w string) (read, rmw, insert float64) {
	switch w {
	case "a":
		return 0.50, 0, 0
	case "b":
		return 0.95, 0, 0
	case "c":
		return 1.0, 0, 0
	case "d":
		return 0.95, 0, 0.05
	case "f":
		return 0.50, 0.50, 0
	}
	panic("unknown workload " + w)
}

func runYCSB() {
	c := newCluster(*nodes)
	defer c.shutdown()
	c.waitLeader()

	// Preload.
	value := string(make([]byte, *valueSize))
	pre := c.clerk()
	for i := 0; i < *records; i++ {
		pre.Put(key(i), value)
	}

	readFrac, rmwFrac, insertFrac := opMix(*workload)
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		latencies []time.Duration
		ops       int
		inserted  = *records
	)
	stop := time.Now().Add(*duration)
	for w := 0; w < *clients; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ck := c.clerk()
			rng := rand.New(rand.NewSource(*seed*1000 + int64(w)))
			zipf := rand.NewZipf(rng, 1.1, 1, uint64(*records-1))
			var local []time.Duration
			n := 0
			for time.Now().Before(stop) {
				var k string
				if *workload == "d" {
					// Read-latest: recent inserts are hot.
					mu.Lock()
					hi := inserted
					mu.Unlock()
					lo := hi - 100
					if lo < 0 {
						lo = 0
					}
					k = key(lo + rng.Intn(hi-lo))
				} else {
					k = key(int(zipf.Uint64()))
				}
				start := time.Now()
				switch r := rng.Float64(); {
				case r < readFrac:
					ck.Get(k)
				case r < readFrac+rmwFrac:
					v := ck.Get(k)
					ck.Put(k, v[:min(len(v), *valueSize)])
				case r < readFrac+rmwFrac+insertFrac:
					mu.Lock()
					inserted++
					ki := inserted
					mu.Unlock()
					ck.Put(key(ki), value)
				default:
					ck.Put(k, value)
				}
				local = append(local, time.Since(start))
				n++
			}
			mu.Lock()
			latencies = append(latencies, local...)
			ops += n
			mu.Unlock()
		}(w)
	}
	wg.Wait()

	sort.Slice(latencies, func(a, b int) bool { return latencies[a] < latencies[b] })
	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[int(float64(len(latencies)-1)*p)]
	}
	fmt.Printf("| %s | %d | %s | %d | %.0f | %.2f | %.2f | %.2f |\n",
		*workload, *nodes, *storage, *clients,
		float64(ops) / duration.Seconds(),
		pct(0.50).Seconds()*1000, pct(0.95).Seconds()*1000, pct(0.99).Seconds()*1000)
}

func runFailover() {
	c := newCluster(*nodes)
	defer c.shutdown()
	c.waitLeader()

	ck := c.clerk()
	ck.Put("failover", "x")
	var times []time.Duration
	for round := 0; round < *rounds; round++ {
		c.waitLeader()
		time.Sleep(300 * time.Millisecond) // settle
		var victim int
		for i, s := range c.servers {
			if _, isLeader := s.Raft().GetState(); isLeader {
				victim = i
			}
		}
		start := time.Now()
		c.net.Deregister(victim)
		c.servers[victim].Kill()
		ck.Put("failover", fmt.Sprint(round)) // blocks until the new leader serves it
		times = append(times, time.Since(start))

		// Restart the dead replica from its own storage — a node that came
		// back with amnesia could double-vote in a term it already voted in.
		s := kv.NewServer(victim, c.n, &sim.RaftTransport{Net: c.net, From: victim},
			c.storages[victim], *snapAt, c.raftConfig(5000+int64(round)))
		c.servers[victim] = s
		c.net.Register(victim, kv.SimService{S: s})
		time.Sleep(200 * time.Millisecond)
	}
	sort.Slice(times, func(a, b int) bool { return times[a] < times[b] })
	pct := func(p float64) time.Duration { return times[int(float64(len(times)-1)*p)] }
	fmt.Printf("| %d | %d | %.0f | %.0f | %.0f | %.0f |\n",
		*nodes, *rounds,
		pct(0).Seconds()*1000, pct(0.50).Seconds()*1000, pct(0.95).Seconds()*1000, pct(1).Seconds()*1000)
}

func runRecovery() {
	c := newCluster(*nodes)
	defer c.shutdown()
	c.waitLeader()

	// Build enough state that recovery must go through a snapshot.
	value := string(make([]byte, *valueSize))
	ck := c.clerk()
	for i := 0; i < *records; i++ {
		ck.Put(key(i), value)
	}
	victim := 0
	if _, isLeader := c.servers[0].Raft().GetState(); isLeader {
		victim = 1
	}
	c.net.Deregister(victim)
	c.servers[victim].Kill()
	// More writes while the victim is down, forcing it behind the snapshot.
	for i := 0; i < *records; i++ {
		ck.Put(key(i), value+"x")
	}
	leader := c.leader()
	targetIndex := leader.Raft().AppliedIndex()

	start := time.Now()
	// Cold restart on the victim's own storage: it recovers its local state,
	// then catches up via InstallSnapshot + log entries from the leader.
	s := kv.NewServer(victim, c.n, &sim.RaftTransport{Net: c.net, From: victim},
		c.storages[victim], *snapAt, c.raftConfig(31337))
	c.servers[victim] = s
	c.net.Register(victim, kv.SimService{S: s})
	for s.Raft().AppliedIndex() < targetIndex {
		time.Sleep(5 * time.Millisecond)
	}
	elapsed := time.Since(start)
	fmt.Printf("| %d | %d | %dB | %.0f ms |\n", *nodes, 2*(*records), *valueSize, elapsed.Seconds()*1000)
}

func key(i int) string { return fmt.Sprintf("user%08d", i) }
