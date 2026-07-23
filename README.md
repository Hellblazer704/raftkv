# raftkv

A distributed, fault-tolerant key-value store in Go, built on a from-scratch
implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf)
— no external consensus libraries. The `raft` package follows the paper
section by section (code comments cite the sections they implement), and the
whole stack above it is verified by a Jepsen-style chaos suite with a
from-scratch linearizability checker: **CI runs 1000 randomized fault
schedules per push, and any linearizability violation fails the build.**

```
clients ──► kv.Clerk ──────────────┐            exactly-once sessions
            shard.Clerk ───────────┤            (clientID + seq dedup)
                                   ▼
   ┌─────────────────────────────────────────────────────┐
   │  kv.Server / shard.Group      state machine + apply │
   │  ── lease reads ── sessions ── snapshots ── CAS ──  │
   ├─────────────────────────────────────────────────────┤
   │  raft            elections · replication · safety   │
   │                  compaction · lease basis            │
   ├──────────────────────┬──────────────────────────────┤
   │  raft/wal            │  transports                   │
   │  fsync'd CRC WAL     │  sim (chaos)  ·  net/rpc      │
   └──────────────────────┴──────────────────────────────┘
```

## Packages

| Package | What it is |
|---|---|
| `raft/` | Raft consensus: leader election (§5.2), log replication + fast backtracking (§5.3), safety (§5.4), compaction (§7), lease reads (§8) |
| `raft/wal/` | File-backed write-ahead log: fsync on every mutation, CRC-checked records, torn-tail recovery |
| `sim/` | Seed-locked simulated network: partitions, drops, delays, reply reordering, crash/restart, multi-cluster peer mapping |
| `linz/` | Linearizability checker (Wing & Gong / Lowe DFS, memoized, per-key compositional, indeterminate-op support) |
| `chaos/` | Jepsen-style rig: seeded nemesis (leader kills, partitions, disk-full, reordering) + recording clients |
| `kv/` | Session KV service: exactly-once writes, memoized CAS, lease-read fast path, snapshot-carried sessions |
| `shard/` | Sharded keyspace: controller (configs via Raft) + groups with pull-based migration, per-shard sessions, GC |
| `rpcnet/` | Production transport: net/rpc over TCP with pooling, timeouts, reconnect |
| `metrics/` | Prometheus collectors for consensus + service state |
| `cmd/raftkvd` | Server daemon: WAL on disk, TCP RPC, /metrics, structured JSON logs |
| `cmd/raftkv-cli` | Client/operator CLI: get/put/append/cas, `txn incr`, `watch`, `status` |
| `cmd/raftkv-bench` | YCSB-style workloads, failover distribution, snapshot recovery |

## Quick start

```sh
# 5-node cluster + Prometheus + Grafana (pre-provisioned dashboard)
docker compose up --build -d
go run ./cmd/raftkv-cli -servers localhost:7001,localhost:7002,localhost:7003,localhost:7004,localhost:7005 put hello world
go run ./cmd/raftkv-cli -servers localhost:7001,... get hello
go run ./cmd/raftkv-cli -servers localhost:7001,... status
# Grafana at http://localhost:3000, Prometheus at http://localhost:9090

# or natively:
make build
./bin/raftkvd -id 0 -peers localhost:7001,localhost:7002,localhost:7003 -listen :7001 -data data/n0 -metrics :9101 &
# ... nodes 1, 2 ...
./bin/raftkv-cli -servers localhost:7001,localhost:7002,localhost:7003 watch hello
```

Tests:

```sh
make test          # everything
make race          # race detector, short mode
make chaos         # 50 seeded fault schedules (CI runs 1000)
make cover         # consensus coverage (CI gates at >=85%)
RAFTKV_CHAOS_BASE=<seed> RAFTKV_CHAOS_SEEDS=1 go test ./chaos -run Schedules   # replay a failure
```

## Phase 1 — Raft core

* **Leader election** with randomized timeouts and the §5.4.1 election
  restriction; **log replication** with §5.3 fast backtracking, so a lagging
  follower converges in O(#distinct terms) round trips.
* **Commit safety**: leaders count replicas only for entries of their own
  term (§5.4.2 — the Figure 8 rule); older entries commit transitively.
* **Persistence**: term, vote, and log reach stable storage before any RPC
  response. The WAL appends CRC-checked records and fsyncs each one;
  recovery accepts any valid prefix, so a torn tail from a mid-write crash
  is truncated, never half-applied. Storage errors halt the node
  (etcd-style) rather than risk responding with un-persisted state.
* **Snapshotting** (§7) with InstallSnapshot for lagging followers;
  snapshot-then-WAL-rewrite ordering keeps a crash between the two
  recoverable.
* **Simulation harness**: every drop, delay, reorder, and partition comes
  from one seeded RNG — a failing schedule replays by seed. A dropped reply
  still executes on the receiver, the failure mode that actually bites RPC
  systems.

The Raft test suite is modeled on MIT 6.5840's: every applied command is
cross-checked against every peer at the same index, so divergent commits,
gaps, or duplicate applies fail at the moment they happen. Scenarios include
Figure 8, crash-restart of an entire committed majority, and 300 rounds of
leader churn on an unreliable reordering network.

## Phase 2 — Chaos suite

* **Linearizability checker** (`linz`): Wing & Gong / Lowe DFS memoized on
  (linearized-set, state), checked per key (P-compositionality). Timed-out
  operations are modeled as indeterminate — they may take effect at any
  later point or never.
* **Nemesis** (`chaos`): seeded schedules of leader kills mid-commit, random
  crashes, leader-trapping minority partitions, lossy/reordering delivery,
  and disk-full faults (a replica whose write fails halts and must be
  repaired). Violations dump seed + fault timeline + full history.
* **[BUGS.md](BUGS.md)**: every real bug the rig has caught — in the store,
  the client, and the checker itself — with root causes and lessons. Two
  favorites: a "safe to retry" reply that wasn't (double execution — the
  canonical argument for sessions), and checker false positives from
  zero-duration ops under Windows' coarse clock.

## Phase 3 — KV service layer

* **Exactly-once sessions**: writes carry (clientID, seq); replicas apply
  each pair at most once, so clerks retry ambiguous failures freely. The
  session table rides inside snapshots and migrates with shards. **CAS**
  additionally memoizes its result — a retried CAS must see its original
  outcome, not a re-evaluation.
* **Lease-based reads**: a leader that heard from a majority within half an
  election timeout serves Gets locally — no log write, no fsync. The lease
  is measured from RPC *send* time with margin for clock-rate skew, and a
  fresh leader must commit a current-term entry first.
  `TestStaleLeaseRejected` pins the failure mode; benchmarks show 1.1M
  reads/s against an 82k ops/s write path.
* **Sharding**: a controller (its own Raft group) versions shard→group
  assignments with deterministic minimal-movement rebalancing; groups
  advance one config at a time, pulling frozen shards from previous owners,
  installing them through the log, and garbage-collecting handed-off copies
  after confirmation. Exactly-once survives a retry landing on a shard's
  new owner.

## Phase 4 — Benchmarks

See [BENCHMARKS.md](BENCHMARKS.md) for full tables and method. Headlines:

* Reads ride the lease (1.1M ops/s, zero log growth); writes ride the log
  (82k ops/s at 3 nodes, in-memory storage).
* **Adding replicas subtracts write throughput** (82k → 60k going 3 → 7
  nodes): the single-leader bottleneck is architectural. Multi-raft — which
  the shard layer implements — is the fix.
* **The fsync ceiling**: with a real WAL, writes plateau at ~3k ops/s while
  latency grows linearly with client count — the signature of one fsync per
  mutation. Group commit is the first-order fix; the analysis ranks the
  options.
* Failover: p50 ≈ one election-timeout draw (~250–310 ms), worst case one
  split vote more. Snapshot recovery is flat in history length — that's the
  point of snapshots.

## Phase 5 — Production surface

* `raftkvd`: TCP RPC daemon with WAL persistence, Prometheus `/metrics`,
  structured JSON logs; `docker compose up` boots a 5-node cluster with
  Prometheus and a pre-provisioned Grafana dashboard (leader, term, commit
  index, ops by type, lease-vs-log reads, apply lag, snapshot pressure).
* `raftkv-cli`: `get/put/append/cas`, `txn incr` (atomic RMW via CAS retry
  loop), `watch`, and `status` (per-node consensus state). raftkv's
  transactional primitive is single-key CAS; multi-key transactions would
  need a lock service or multi-shard commit and are out of scope — see the
  CLI's doc comment.
* CI: race-detector suite, ≥85% consensus coverage gate, and 4×250 chaos
  schedules per push with violation-repro artifacts.

## Honest limitations

* Single-key linearizable ops; no multi-key transactions, no range scans.
* Lease reads are leader-only (no follower reads yet).
* The WAL fsyncs per mutation — no group commit yet (see BENCHMARKS.md).
* Membership is static per process lifetime (the shard layer moves data
  between groups; Raft-level joint consensus is not implemented).
* Benchmarks run on the in-process network; shapes are durable, absolute
  numbers are not (full caveats in BENCHMARKS.md).

## Status

- [x] Phase 1 — Raft core (election, replication, persistence, snapshots, sim harness)
- [x] Phase 2 — Chaos suite: linearizability checker + nemesis schedules
- [x] Phase 3 — KV service: exactly-once sessions, sharding, lease reads
- [x] Phase 4 — Benchmarks: YCSB workloads, failover distribution
- [x] Phase 5 — Observability: Prometheus/Grafana, CLI, Docker cluster
