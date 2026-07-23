package chaos

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Nemesis injects faults from a seeded schedule: leader kills, random
// crashes, minority/majority partitions, unreliable delivery, reply
// reordering, and disk-full storage faults. Every decision comes from the
// seed, so a failing schedule replays exactly.
type Nemesis struct {
	c   *Cluster
	rng *rand.Rand

	mu  sync.Mutex
	log []string
}

func NewNemesis(c *Cluster, seed int64) *Nemesis {
	return &Nemesis{c: c, rng: rand.New(rand.NewSource(seed))}
}

// Log returns the fault timeline, for failure reports.
func (nm *Nemesis) Log() []string {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	return append([]string(nil), nm.log...)
}

func (nm *Nemesis) note(format string, args ...any) {
	nm.mu.Lock()
	nm.log = append(nm.log, fmt.Sprintf("%8.3fs %s",
		time.Duration(nm.c.now()).Seconds(), fmt.Sprintf(format, args...)))
	nm.mu.Unlock()
}

// Run injects faults until stop closes, then repairs everything: partitions
// healed, network reliable, all nodes restarted.
func (nm *Nemesis) Run(stop <-chan struct{}) {
	partitioned := false
	unreliable := false
	for {
		delay := time.Duration(150+nm.rng.Intn(450)) * time.Millisecond
		select {
		case <-stop:
			nm.repair()
			return
		case <-time.After(delay):
		}

		switch nm.rng.Intn(10) {
		case 0, 1: // kill the leader (mid-commit, as far as clients know)
			if leader := nm.c.Leader(); leader != -1 && nm.mayKill() {
				nm.note("crash leader %d", leader)
				nm.c.Crash(leader)
			}
		case 2: // kill a random replica
			victim := nm.rng.Intn(nm.c.N)
			if nm.c.Alive(victim) && nm.mayKill() {
				nm.note("crash node %d", victim)
				nm.c.Crash(victim)
			}
		case 3, 4: // restart everything that is down
			for i := 0; i < nm.c.N; i++ {
				if !nm.c.Alive(i) {
					nm.note("restart node %d", i)
					nm.c.Restart(i)
				}
			}
		case 5: // partition: isolate a minority, usually including the leader
			minority := nm.pickMinority()
			for _, id := range minority {
				nm.c.Net.SetGroup(id, 1)
			}
			partitioned = true
			nm.note("partition minority %v", minority)
		case 6: // heal partitions
			if partitioned {
				nm.c.Net.HealPartitions()
				partitioned = false
				nm.note("heal partitions")
			}
		case 7: // toggle lossy/delaying delivery
			unreliable = !unreliable
			nm.c.Net.SetReliable(!unreliable)
			nm.note("unreliable=%v", unreliable)
		case 8: // reply reordering
			nm.c.Net.SetLongReordering(true)
			nm.note("long reordering on")
		case 9: // disk-full: next write kills the node etcd-style
			victim := nm.rng.Intn(nm.c.N)
			if nm.c.Alive(victim) && nm.mayKill() {
				nm.note("disk full on node %d", victim)
				nm.c.DiskFull(victim)
				// The node halts itself on its next write; mark it crashed so
				// the restart action repairs and reboots it.
				go func(v int) {
					time.Sleep(300 * time.Millisecond)
					nm.c.Crash(v)
				}(victim)
			}
		}
	}
}

// mayKill limits deliberate kills so the cluster usually keeps a quorum —
// but 10% of the time it lets the quorum die, to test full-outage recovery.
func (nm *Nemesis) mayKill() bool {
	if nm.rng.Float64() < 0.10 {
		return true
	}
	return nm.c.AliveCount() > nm.c.N/2+1
}

// pickMinority selects up to N/2 replicas, biased toward including the
// current leader (the interesting case: a deposed leader serving stale state).
func (nm *Nemesis) pickMinority() []int {
	size := 1 + nm.rng.Intn(nm.c.N/2)
	chosen := map[int]bool{}
	if leader := nm.c.Leader(); leader != -1 && nm.rng.Float64() < 0.7 {
		chosen[leader] = true
	}
	for len(chosen) < size {
		chosen[nm.rng.Intn(nm.c.N)] = true
	}
	out := make([]int, 0, len(chosen))
	for id := range chosen {
		out = append(out, id)
	}
	return out
}

// repair returns the world to a healthy state so the final reads can settle.
func (nm *Nemesis) repair() {
	nm.c.Net.HealPartitions()
	nm.c.Net.SetReliable(true)
	nm.c.Net.SetLongReordering(false)
	for i := 0; i < nm.c.N; i++ {
		if !nm.c.Alive(i) {
			nm.c.Restart(i)
		}
	}
	nm.note("repair: healed, reliable, all restarted")
}
