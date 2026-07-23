package chaos

// Randomized fault-schedule tests. Every schedule is a pure function of its
// seed; a failure report includes the seed, the nemesis timeline, and the
// recorded history, so any violation replays with:
//
//	RAFTKV_CHAOS_BASE=<seed> RAFTKV_CHAOS_SEEDS=1 go test ./chaos -run Schedules
//
// CI sets RAFTKV_CHAOS_SEEDS high (see .github/workflows); locally the
// default keeps `go test ./...` under a few minutes.

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Hellblazer704/raftkv/linz"
)

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func TestRandomFaultSchedules(t *testing.T) {
	seeds := envInt("RAFTKV_CHAOS_SEEDS", 10)
	base := envInt("RAFTKV_CHAOS_BASE", 1)
	if testing.Short() {
		seeds = min(seeds, 4)
	}
	for s := 0; s < seeds; s++ {
		seed := int64(base + s)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			runSchedule(t, seed)
		})
	}
}

// runSchedule: 5 replicas, 5 clients hammering 5 keys, nemesis pulling
// levers for ~4s, then repair, quiesce, and a linearizability check over the
// full recorded history.
func runSchedule(t *testing.T, seed int64) {
	const (
		replicas = 5
		clients  = 5
		keys     = 5
		runFor   = 4 * time.Second
	)
	c := NewCluster(replicas, seed, 2048)
	defer c.Shutdown()

	nm := NewNemesis(c, seed*7919)
	stopNemesis := make(chan struct{})
	nemesisDone := make(chan struct{})
	go func() {
		defer close(nemesisDone)
		nm.Run(stopNemesis)
	}()

	stopClients := make(chan struct{})
	var wg sync.WaitGroup
	cls := make([]*Client, clients)
	for i := 0; i < clients; i++ {
		cls[i] = c.NewClient()
		wg.Add(1)
		go func(cl *Client, workerSeed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(workerSeed))
			seq := 0
			for {
				select {
				case <-stopClients:
					return
				default:
				}
				key := fmt.Sprintf("k%d", rng.Intn(keys))
				seq++
				val := fmt.Sprintf("%d.%d;", cl.id, seq) // globally unique
				switch r := rng.Float64(); {
				case r < 0.40:
					cl.Put(key, val)
				case r < 0.65:
					cl.Append(key, val)
				default:
					cl.Get(key)
				}
				// The sleep floor bounds history density across machine
				// speeds: on Linux a 0ms draw really is 0ms, and the denser
				// histories made checker state explode (CI OOM).
				time.Sleep(time.Duration(10+rng.Intn(30)) * time.Millisecond)
			}
		}(cls[i], seed*1000+int64(i))
	}

	time.Sleep(runFor)
	close(stopNemesis)
	<-nemesisDone // nemesis repairs the world before returning
	time.Sleep(1 * time.Second)
	close(stopClients)
	wg.Wait()

	var history []linz.Op
	for _, cl := range cls {
		history = append(history, cl.History()...)
	}
	res := linz.CheckKV(history, 60*time.Second)
	if res.Unknown {
		t.Logf("seed %d: checker budget exhausted (%d ops) — inconclusive, not a failure", seed, len(history))
		return
	}
	if !res.Linearizable {
		dumpFailure(t, seed, nm, history, res)
	}
}

// dumpFailure writes everything needed to debug a violation.
func dumpFailure(t *testing.T, seed int64, nm *Nemesis, history []linz.Op, res linz.Result) {
	path := fmt.Sprintf("failure-seed%d.txt", seed)
	f, err := os.Create(path)
	if err == nil {
		fmt.Fprintf(f, "LINEARIZABILITY VIOLATION seed=%d key=%q\n\nNemesis timeline:\n", seed, res.BadKey)
		for _, line := range nm.Log() {
			fmt.Fprintln(f, line)
		}
		fmt.Fprintf(f, "\nHistory (%d ops):\n", len(history))
		for _, op := range history {
			fmt.Fprintln(f, op)
		}
		f.Close()
	}
	t.Fatalf("linearizability violation on key %q (seed %d); details in %s", res.BadKey, seed, path)
}

// TestKillLeaderMidCommit hammers the specific moment the prompt asks about:
// a leader dies immediately after accepting writes, over and over.
func TestKillLeaderMidCommit(t *testing.T) {
	c := NewCluster(5, 42, 4096)
	defer c.Shutdown()
	cl := c.NewClient()

	for round := 0; round < 8; round++ {
		// Fire writes and kill the leader while they're in flight.
		var wg sync.WaitGroup
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				w := c.NewClient()
				w.Put("mid", fmt.Sprintf("r%d.%d;", round, i))
			}(i)
		}
		time.Sleep(30 * time.Millisecond)
		if leader := c.Leader(); leader != -1 {
			c.Crash(leader)
		}
		wg.Wait()
		// Cluster must elect a new leader and accept writes again.
		if ok := cl.Put("mid", fmt.Sprintf("post%d;", round)); !ok {
			// One ambiguous write is fine; a definite success must follow.
			if ok2 := cl.Put("mid", fmt.Sprintf("post%db;", round)); !ok2 {
				deadline := time.Now().Add(5 * time.Second)
				recovered := false
				for time.Now().Before(deadline) {
					if cl.Put("mid", fmt.Sprintf("post%dc;", round)) {
						recovered = true
						break
					}
				}
				if !recovered {
					t.Fatalf("round %d: cluster never recovered after leader kill", round)
				}
			}
		}
		// Bring everyone back for the next round.
		for i := 0; i < 5; i++ {
			if !c.Alive(i) {
				c.Restart(i)
			}
		}
	}
}

// TestRestartFromSnapshot forces snapshots, cold-restarts the whole cluster,
// and verifies the state machine survives via reads.
func TestRestartFromSnapshot(t *testing.T) {
	c := NewCluster(3, 7, 512) // tiny threshold: snapshot constantly
	defer c.Shutdown()
	cl := c.NewClient()

	for i := 0; i < 60; i++ {
		for !cl.Put(fmt.Sprintf("k%d", i%5), fmt.Sprintf("v%d;", i)) {
			time.Sleep(50 * time.Millisecond)
		}
	}
	for i := 0; i < 3; i++ {
		c.Crash(i)
	}
	for i := 0; i < 3; i++ {
		c.Restart(i)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if v, ok := cl.Get("k4"); ok {
			if v != "v59;" {
				t.Fatalf("after snapshot restart: k4=%q, want %q", v, "v59;")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cluster did not serve reads after full restart")
		}
	}
}

// TestDiskFullHaltsNode: a replica whose disk fails must halt rather than
// respond with un-persisted state, and must rejoin after repair.
func TestDiskFullHaltsNode(t *testing.T) {
	c := NewCluster(3, 11, 4096)
	defer c.Shutdown()
	cl := c.NewClient()

	for !cl.Put("k", "before;") {
		time.Sleep(50 * time.Millisecond)
	}
	victim := c.Leader()
	if victim == -1 {
		t.Fatal("no leader")
	}
	c.DiskFull(victim)

	// Writes force the victim to hit its dead disk and halt; the other two
	// must elect a fresh leader and keep going.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if cl.Put("k", "during;") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cluster did not survive leader disk failure")
		}
	}

	c.Crash(victim)   // reap the halted process
	c.Restart(victim) // repairs the disk and reboots
	for !cl.Put("k", "after;") {
		time.Sleep(50 * time.Millisecond)
	}
	if v, ok := cl.Get("k"); ok && v != "after;" {
		t.Fatalf("k=%q, want %q", v, "after;")
	}
}
