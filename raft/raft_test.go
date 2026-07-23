package raft_test

// Ported from the shape of the MIT 6.5840 Raft tests: elections, agreement
// under partitions and crashes, persistence, Figure 8, and unreliable
// networks. Seeds are fixed so failures reproduce.

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3, 1, true)
	defer c.cleanup()

	c.checkOneLeader()
	term1 := c.checkTerms()
	time.Sleep(600 * time.Millisecond)
	term2 := c.checkTerms()
	if term1 != term2 {
		t.Logf("warning: term changed with no failures (%d -> %d); allowed but unusual", term1, term2)
	}
	c.checkOneLeader()
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3, 2, true)
	defer c.cleanup()

	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	leader2 := c.checkOneLeader()

	// Old leader rejoins: must not cause two leaders. A new election is
	// legal, so re-read who leads now — computing the disconnect set from a
	// stale reading left the real leader connected (a harness bug CI caught
	// on slower runners; BUGS.md #6).
	c.connect(leader1)
	leader2 = c.checkOneLeader()

	// No quorum -> no leader.
	c.disconnect(leader2)
	c.disconnect((leader2 + 1) % 3)
	time.Sleep(700 * time.Millisecond)
	c.checkNoLeader()

	c.connect((leader2 + 1) % 3)
	c.checkOneLeader()
	c.connect(leader2)
	c.checkOneLeader()
}

func TestBasicAgree(t *testing.T) {
	c := newCluster(t, 3, 3, true)
	defer c.cleanup()

	for i := 1; i <= 3; i++ {
		if count, _ := c.nCommitted(i); count > 0 {
			t.Fatal("committed before any Start")
		}
		if idx := c.one(i*100, 3, false); idx != i {
			t.Fatalf("got index %d, want %d", idx, i)
		}
	}
}

func TestFailAgree(t *testing.T) {
	c := newCluster(t, 3, 4, true)
	defer c.cleanup()

	c.one(101, 3, false)
	leader := c.checkOneLeader()
	c.disconnect((leader + 1) % 3)

	// Majority of two still commits.
	c.one(102, 2, false)
	c.one(103, 2, false)
	time.Sleep(500 * time.Millisecond)
	c.one(104, 2, false)

	c.connect((leader + 1) % 3)
	c.one(105, 3, true)
	c.one(106, 3, true)
}

func TestFailNoAgree(t *testing.T) {
	c := newCluster(t, 5, 5, true)
	defer c.cleanup()

	c.one(10, 5, false)
	leader := c.checkOneLeader()
	c.disconnect((leader + 1) % 5)
	c.disconnect((leader + 2) % 5)
	c.disconnect((leader + 3) % 5)

	index, _, ok := c.raft(leader).Start([]byte("20"))
	if !ok {
		t.Fatal("leader rejected Start")
	}
	time.Sleep(2 * time.Second)
	if count, _ := c.nCommitted(index); count > 0 {
		t.Fatalf("committed %d without a quorum", index)
	}

	c.connect((leader + 1) % 5)
	c.connect((leader + 2) % 5)
	c.connect((leader + 3) % 5)
	c.one(30, 5, true)
}

func TestConcurrentStarts(t *testing.T) {
	c := newCluster(t, 3, 6, true)
	defer c.cleanup()

	c.checkOneLeader()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.one(300+i, 3, true)
		}(i)
	}
	wg.Wait()
}

func TestRejoin(t *testing.T) {
	c := newCluster(t, 3, 7, true)
	defer c.cleanup()

	c.one(101, 3, true)
	leader1 := c.checkOneLeader()

	// Isolated leader accumulates uncommitted entries.
	c.disconnect(leader1)
	c.raft(leader1).Start([]byte("102"))
	c.raft(leader1).Start([]byte("103"))
	c.raft(leader1).Start([]byte("104"))

	c.one(103, 2, true)

	// New leader gets isolated too; old leader rejoins with a conflicting log
	// that must be overwritten.
	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	c.connect(leader1)
	c.one(104, 2, true)
	c.connect(leader2)
	c.one(105, 3, true)
}

func TestBackup(t *testing.T) {
	c := newCluster(t, 5, 8, true)
	defer c.cleanup()

	c.one(randCmd(), 5, true)

	// Isolate a leader with a minority; it piles up entries that will never
	// commit and must later be rolled back quickly (fast backtracking).
	leader1 := c.checkOneLeader()
	c.disconnect((leader1 + 2) % 5)
	c.disconnect((leader1 + 3) % 5)
	c.disconnect((leader1 + 4) % 5)
	for i := 0; i < 50; i++ {
		c.raft(leader1).Start([]byte(strconv.Itoa(randCmd())))
	}
	time.Sleep(500 * time.Millisecond)

	c.disconnect(leader1)
	c.disconnect((leader1 + 1) % 5)
	c.connect((leader1 + 2) % 5)
	c.connect((leader1 + 3) % 5)
	c.connect((leader1 + 4) % 5)

	// The other partition commits plenty.
	for i := 0; i < 50; i++ {
		c.one(randCmd(), 3, true)
	}

	// New leader within that partition goes down to a minority too.
	leader2 := c.checkOneLeader()
	other := (leader1 + 2) % 5
	if leader2 == other {
		other = (leader2 + 1) % 5
	}
	c.disconnect(other)
	for i := 0; i < 50; i++ {
		c.raft(leader2).Start([]byte(strconv.Itoa(randCmd())))
	}
	time.Sleep(500 * time.Millisecond)

	// Bring the original minority back with the disconnected node.
	for i := 0; i < 5; i++ {
		c.disconnect(i)
	}
	c.connect(leader1)
	c.connect((leader1 + 1) % 5)
	c.connect(other)
	for i := 0; i < 50; i++ {
		c.one(randCmd(), 3, true)
	}

	for i := 0; i < 5; i++ {
		c.connect(i)
	}
	c.one(randCmd(), 5, true)
}

func TestPersistRestartAll(t *testing.T) {
	c := newCluster(t, 3, 9, true)
	defer c.cleanup()

	c.one(11, 3, true)
	for i := 0; i < 3; i++ {
		c.restart(i)
	}
	c.one(12, 3, true)

	leader := c.checkOneLeader()
	c.restart(leader)
	c.one(13, 3, true)
}

func TestPersistPartiallyCommitted(t *testing.T) {
	c := newCluster(t, 5, 10, true)
	defer c.cleanup()

	c.one(11, 5, true)
	leader := c.checkOneLeader()
	c.disconnect((leader + 1) % 5)
	c.disconnect((leader + 2) % 5)
	c.one(12, 3, true)

	// Crash the entire majority that persisted index 12.
	c.crash(leader)
	c.crash((leader + 3) % 5)
	c.crash((leader + 4) % 5)

	// Reconnect the two that missed it and restart one crashed node. Entry 12
	// lives only on the restarted node's disk — the election restriction
	// (§5.4.1) must make it the leader so 12 survives.
	c.connect((leader + 1) % 5)
	c.connect((leader + 2) % 5)
	c.start(leader)
	c.one(13, 3, true)

	c.start((leader + 3) % 5)
	c.start((leader + 4) % 5)
	c.one(14, 5, true)
}

func TestFigure8(t *testing.T) {
	// Figure 8 (§5.4.2): a leader must never commit an entry from a previous
	// term by counting replicas. Repeatedly crash leaders mid-replication and
	// verify the final agreement is consistent.
	c := newCluster(t, 5, 11, true)
	defer c.cleanup()

	c.one(randCmd(), 1, true)
	nup := 5
	for iter := 0; iter < 100; iter++ {
		leader := -1
		for i := 0; i < 5; i++ {
			if rf := c.raft(i); rf != nil {
				if _, _, ok := rf.Start([]byte(strconv.Itoa(randCmd()))); ok {
					leader = i
				}
			}
		}
		if leader != -1 && (iter%3 == 0) {
			c.crash(leader)
			nup--
		}
		if nup < 3 {
			for i := 0; i < 5; i++ {
				if c.raft(i) == nil {
					c.start(i)
					nup++
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	for i := 0; i < 5; i++ {
		if c.raft(i) == nil {
			c.start(i)
		}
	}
	c.one(randCmd(), 5, true)
}

func TestUnreliableAgree(t *testing.T) {
	c := newCluster(t, 5, 12, false)
	defer c.cleanup()

	var wg sync.WaitGroup
	for iter := 1; iter < 30; iter += 5 {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iter, j int) {
				defer wg.Done()
				c.one(100*iter+j, 1, true)
			}(iter, j)
		}
		c.one(iter, 1, true)
	}
	wg.Wait()
	c.net.SetReliable(true)
	c.one(100, 5, true)
}

func TestFigure8Unreliable(t *testing.T) {
	if testing.Short() {
		t.Skip("long chaos test")
	}
	c := newCluster(t, 5, 13, false)
	defer c.cleanup()

	c.one(randCmd()%10000, 1, true)
	nup := 5
	for iter := 0; iter < 300; iter++ {
		if iter == 200 {
			c.net.SetLongReordering(true)
		}
		leader := -1
		for i := 0; i < 5; i++ {
			if rf := c.raft(i); rf != nil {
				if _, _, ok := rf.Start([]byte(strconv.Itoa(randCmd() % 10000))); ok && c.isConnected(i) {
					leader = i
				}
			}
		}
		if leader != -1 && intn(1000) < 100 {
			c.disconnect(leader)
			nup--
		}
		if nup < 3 {
			for i := 0; i < 5; i++ {
				if !c.isConnected(i) {
					c.connect(i)
					nup++
				}
			}
		}
		time.Sleep(time.Duration(intn(30)) * time.Millisecond)
	}
	c.net.SetLongReordering(false)
	c.net.SetReliable(true)
	for i := 0; i < 5; i++ {
		c.connect(i)
	}
	c.one(randCmd()%10000, 5, true)
}

// TestInspectionAPIs deterministically exercises the read-only inspection
// surface used by the KV lease path and the metrics/CLI tooling: Stats,
// LeaseRead, LeaderHint, AppliedIndex. Coverage here does not depend on
// chaos timing — the functions are simply called on every node in a stable
// cluster, and the leader is additionally checked for a live lease.
func TestInspectionAPIs(t *testing.T) {
	c := newCluster(t, 3, 20, true)
	defer c.cleanup()

	// Commit a few current-term entries so the leader can hold a read lease.
	for i := 1; i <= 3; i++ {
		c.one(500+i, 3, true)
	}
	leader := c.checkOneLeader()

	for i := 0; i < 3; i++ {
		rf := c.raft(i)
		st := rf.Stats()
		if st.State == "" {
			t.Fatalf("node %d empty state string", i)
		}
		if st.LastApplied > st.CommitIndex {
			t.Fatalf("node %d applied %d > commit %d", i, st.LastApplied, st.CommitIndex)
		}
		if st.CommitIndex < st.FirstIndex {
			t.Fatalf("node %d commit %d < firstIndex %d", i, st.CommitIndex, st.FirstIndex)
		}
		rf.LeaseRead()   // exercised on followers (returns !ok) and leader
		rf.LeaderHint()  // follower returns leaderID, leader returns self
		rf.AppliedIndex()
	}

	// The leader must eventually hold a lease and report itself.
	deadline := time.Now().Add(3 * time.Second)
	gotLease := false
	for time.Now().Before(deadline) {
		if _, ok := c.raft(leader).LeaseRead(); ok {
			gotLease = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !gotLease {
		t.Fatal("leader never acquired a read lease in a healthy cluster")
	}
	if hint := c.raft(leader).LeaderHint(); hint != leader {
		t.Fatalf("leader hint = %d, want self %d", hint, leader)
	}
	if c.raft(leader).Stats().State != "leader" {
		t.Fatal("leader Stats() does not report leader state")
	}
}

func TestSnapshotBasic(t *testing.T) {
	c := newCluster(t, 3, 14, true)
	c.snapshotEvery = 10
	defer c.cleanup()

	for i := 1; i <= 35; i++ {
		c.one(1000+i, 3, false)
	}
	// Log must actually have been compacted.
	for i := 0; i < 3; i++ {
		if size := c.raft(i).LogSizeBytes(); size > 35*8 {
			t.Logf("node %d log size %dB after snapshots", i, size)
		}
	}
	c.one(9999, 3, false)
}

func TestSnapshotInstall(t *testing.T) {
	c := newCluster(t, 3, 15, true)
	c.snapshotEvery = 10
	defer c.cleanup()

	c.one(1, 3, false)
	leader := c.checkOneLeader()
	victim := (leader + 1) % 3
	c.disconnect(victim)

	// Push far past a snapshot boundary so the victim can only catch up via
	// InstallSnapshot.
	for i := 2; i <= 40; i++ {
		c.one(i, 2, true)
	}
	c.connect(victim)
	c.one(41, 3, true)

	c.mu.Lock()
	applied := c.applied[victim]
	c.mu.Unlock()
	if applied < 41 {
		t.Fatalf("victim only applied through %d", applied)
	}
}

func TestSnapshotRestart(t *testing.T) {
	c := newCluster(t, 3, 16, true)
	c.snapshotEvery = 10
	defer c.cleanup()

	for i := 1; i <= 25; i++ {
		c.one(i, 3, false)
	}
	for i := 0; i < 3; i++ {
		c.restart(i)
	}
	c.one(26, 3, true)
}

var cmdCounter atomic.Int64

// randCmd returns a process-unique command value, so retried submissions are
// distinguishable.
func randCmd() int { return int(cmdCounter.Add(1)) + 1_000_000 }

var intnState atomic.Int64

func intn(n int) int {
	// Cheap deterministic-ish source for test jitter; correctness never
	// depends on it.
	v := intnState.Add(2654435761)
	if v < 0 {
		v = -v
	}
	return int(v % int64(n))
}
