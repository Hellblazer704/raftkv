# raftkv

A distributed, fault-tolerant key-value store in Go, built on a from-scratch
implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf).
No external consensus libraries — the `raft` package follows the paper section
by section, and code comments cite the sections they implement.

## Layout

| Package | What it is |
|---|---|
| `raft/` | Raft consensus: leader election (§5.2), log replication (§5.3), safety (§5.4), log compaction (§7) |
| `raft/wal/` | File-backed write-ahead log: fsync on every mutation, CRC-checked records, torn-tail recovery |
| `sim/` | Seed-locked simulated network: partitions, message drops, delivery delays, reply reordering, crash/restart |
| `linz/` | Linearizability checker (Wing & Gong / Lowe DFS with memoization, per-key compositional, indeterminate-op support) |
| `chaos/` | Jepsen-style rig: KV subject + recording clients + seeded nemesis (leader kills, partitions, disk-full, reordering) |

## Phase 1 — Raft core

* **Leader election** with randomized timeouts and the §5.4.1 election
  restriction (a candidate needs an up-to-date log to win votes).
* **Log replication** with the §5.3 fast-backtracking optimization: a lagging
  or conflicting follower converges in O(#distinct terms) round trips.
* **Commit safety**: leaders only count replicas for entries of their own term
  (§5.4.2, the Figure 8 rule); older entries commit transitively.
* **Persistence**: term, vote, and log reach stable storage before any RPC
  response. The `wal` package appends CRC-checked records and fsyncs each one;
  recovery accepts any valid prefix, so a torn tail from a mid-write crash is
  detected and truncated rather than half-applied.
* **Snapshotting** (§7): the service snapshots its own state; Raft compacts
  the log and ships `InstallSnapshot` to followers that fall behind the
  boundary. Snapshot-then-WAL-rewrite ordering keeps a crash between the two
  recoverable.
* **Simulation harness**: all message drops, delays, reorderings and
  partitions come from one seeded RNG, so a failing schedule replays by seed.
  A dropped reply still executes the request on the receiver — the failure
  mode that actually bites real RPC systems.

The test suite is modeled on MIT 6.5840's: every applied command is
cross-checked against every other node's log at the same index, so divergent
commits, gaps, or duplicate applies fail the test the moment they happen.
Scenarios include partitioned leaders with uncommitted tails, crash-restart of
a committed majority, Figure 8, and 300 rounds of leader churn on an
unreliable, reordering network.

```
go test ./raft/...        # full suite
go test -race -short ./raft/...
```

## Phase 2 — Chaos suite

* **Linearizability checker** (`linz`): Wing & Gong / Lowe DFS with
  memoization on (linearized-set, state), checked per key — operations on
  distinct keys commute, so linearizability is compositional and one
  exponential search becomes many small ones. Timed-out client operations are
  modeled as indeterminate (they may take effect at any later point, or
  linearize harmlessly after the final observation).
* **Nemesis** (`chaos`): a seeded scheduler that kills leaders mid-commit,
  crashes random replicas, partitions minorities (biased to trap the leader),
  flips the network into lossy/reordering modes, and fails disks (a replica
  whose write fails halts, etcd-style, and must be repaired and restarted).
  Every decision derives from the seed: a failing schedule replays exactly,
  and a violation dumps the seed, fault timeline, and full history.
* **CI runs 1000 randomized fault schedules** per push (4 shards × 250
  seeds); any linearizability violation fails the build and uploads the
  repro. A separate job enforces ≥85% statement coverage over the consensus
  packages, under the race detector for the functional suites.
* **[BUGS.md](BUGS.md)** logs every real bug the rig has caught so far — in
  the store, the client, *and* the checker itself — with root causes and the
  design lessons each one forced.

```
RAFTKV_CHAOS_SEEDS=50 go test ./chaos -run TestRandomFaultSchedules -parallel 8
RAFTKV_CHAOS_BASE=<seed> RAFTKV_CHAOS_SEEDS=1 go test ./chaos -run Schedules   # replay one
```

## Status

- [x] Phase 1 — Raft core (election, replication, persistence, snapshots, sim harness)
- [x] Phase 2 — Chaos suite: linearizability checker + nemesis schedules
- [ ] Phase 3 — KV service: exactly-once sessions, sharding, lease reads
- [ ] Phase 4 — Benchmarks: YCSB workloads, failover distribution
- [ ] Phase 5 — Observability: Prometheus/Grafana, CLI, Docker cluster
