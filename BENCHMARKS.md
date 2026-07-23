# BENCHMARKS.md

Measured with `cmd/raftkv-bench` (all numbers reproducible: seeds fixed,
commands below each table).

**Environment — read this before the numbers.** Single Windows 11 laptop
(x86-64), cluster in one process on the in-memory simulated network in
reliable mode, so there is **no wire latency**: these numbers isolate the
cost of the consensus path itself (Raft bookkeeping, the apply pipeline,
fsync policy), which is the part this project implements. `-storage wal`
puts a real fsync'ing WAL under every replica on the laptop's SSD;
`-storage mem` uses in-memory storage. Latency percentiles quantize at
~0.5 ms (Windows timer granularity); "0.00" means "below half a
millisecond", not zero.

## YCSB-style workloads

32 clients, 8 s window, 1000 preloaded keys, 100 B values, zipfian keys.
Workload E (scans) is omitted: raftkv has no range scans, and faking them
with point reads would fake the numbers.

| workload | nodes | storage | clients | ops/s | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|---|---|
| a (50r/50w) | 3 | mem | 32 | 82,200 | 0.00 | 1.42 | 2.00 |
| a | 5 | mem | 32 | 63,370 | 0.00 | 1.51 | 2.51 |
| a | 7 | mem | 32 | 60,148 | 0.00 | 1.51 | 2.51 |
| b (95r/5w) | 3 | mem | 32 | 626,152 | 0.00 | 0.00 | 0.52 |
| b | 5 | mem | 32 | 518,255 | 0.00 | 0.00 | 1.00 |
| b | 7 | mem | 32 | 459,212 | 0.00 | 0.00 | 1.00 |
| c (100r) | 3 | mem | 32 | 1,120,870 | 0.00 | 0.00 | 1.01 |
| c | 5 | mem | 32 | 1,135,061 | 0.00 | 0.00 | 1.01 |
| c | 7 | mem | 32 | 1,113,616 | 0.00 | 0.00 | 1.00 |
| d (95r/5i) | 3 | mem | 32 | 301,072 | 0.00 | 0.00 | 0.62 |
| d | 5 | mem | 32 | 268,430 | 0.00 | 0.00 | 1.00 |
| d | 7 | mem | 32 | 222,535 | 0.00 | 0.00 | 1.00 |
| f (50r/50rmw) | 3 | mem | 32 | 74,551 | 0.00 | 1.29 | 2.00 |
| f | 5 | mem | 32 | 61,405 | 0.00 | 1.51 | 2.10 |
| f | 7 | mem | 32 | 53,035 | 0.00 | 1.08 | 2.00 |

```
throughput by workload (3 nodes, mem), log-ish scale
c  |=============================================| 1.12M   pure lease reads
b  |=========================                    |  626k   95% reads
d  |============                                 |  301k   read-latest
a  |===                                          |   82k   half writes
f  |===                                          |   75k   half RMW
```

`raftkv-bench -mode ycsb -workload a -nodes 3 -clients 32 -duration 8s -storage mem`

### What the shape says

* **Reads ride the lease; writes ride the log.** Workload C never touches
  the log (1.1M ops/s of local map reads behind a lease check); workload A
  is gated by replication. The gap *is* the value of lease reads.
* **Adding nodes does not add write throughput — it subtracts.** 82k → 63k →
  60k ops/s going 3 → 5 → 7 nodes on workload A. Every write still funnels
  through one leader, which now replicates to more followers and waits on
  the same majority math. Replicas buy fault tolerance and read capacity
  (if you add follower reads), never write capacity. This is the
  **single-leader bottleneck**, and it is architectural, not a bug.
* **Reads are also leader-bound here** (workload C flat across cluster
  sizes): lease reads are served only by the leader in this implementation.
  Follower reads with read-index leases would spread that load.

## The fsync ceiling (workload A, real WAL)

| workload | nodes | storage | clients | ops/s | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|---|---|
| a | 3 | wal | 32 | 3,445 | 9.02 | 14.48 | 17.75 |
| a | 5 | wal | 32 | 2,954 | 10.24 | 16.67 | 22.76 |
| a | 7 | wal | 32 | 2,601 | 12.03 | 19.53 | 24.60 |

Client scaling, 3 nodes:

| storage | clients | ops/s | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|
| wal | 1 | 2,388 | 0.00 | 1.50 | 2.00 |
| wal | 8 | 3,313 | 2.00 | 5.55 | 10.60 |
| wal | 32 | 2,953 | 9.29 | 21.18 | 28.33 |
| wal | 128 | 3,224 | 36.53 | 63.81 | 107.53 |
| mem | 1 | 7,700 | 0.00 | 0.00 | 0.58 |
| mem | 8 | 33,281 | 0.00 | 0.00 | 1.00 |
| mem | 32 | 83,138 | 0.00 | 1.28 | 2.00 |
| mem | 128 | 65,171 | 2.00 | 3.51 | 4.52 |

```
workload a, 3 nodes: mem vs wal
mem |==========================================| 82k ops/s
wal |==                                        | 3.4k ops/s   <- fsync
```

The honest write ceiling: with a real WAL, throughput **plateaus around
3k ops/s no matter how many clients pile on** — latency just grows
linearly (p50 goes 2 → 9 → 37 ms from 8 → 32 → 128 clients) while
throughput stays flat. That is the signature of a serialized fsync: this
WAL syncs **every mutation individually** while holding Raft's mutex, so
the disk's flush rate bounds the whole system.

What would move the ceiling, in order of leverage:

1. **Group commit** — batch all entries accepted during one fsync into the
   next fsync. Standard technique (etcd, Postgres); would turn ~3k
   syncs/s into ~3k *batches*/s and likely recover most of the 24×
   gap to mem at high client counts.
2. **Pipelined replication** — don't hold the log lock across the disk
   write; followers can ack in parallel with the leader's own fsync.
3. **Multi-raft** — the shard layer (`shard/`) already runs G independent
   Raft groups. Since each group has its own leader and its own WAL, write
   throughput scales ~linearly in G until the disk saturates. That's the
   architectural fix for the single-leader bottleneck; the per-group
   ceiling stays wherever fsync policy puts it.

## Leader failover time

Kill the leader, measure until a client write commits under a new leader.
20 rounds per cluster size. Election timeout is 250–450 ms randomized.

| nodes | rounds | min ms | p50 ms | p95 ms | max ms |
|---|---|---|---|---|---|
| 3 | 20 | 248 | 311 | 375 | 560 |
| 5 | 20 | 246 | 250 | 373 | 374 |
| 7 | 20 | 246 | 249 | 312 | 373 |

`raftkv-bench -mode failover -nodes 5 -rounds 20`

The distribution is exactly the election-timeout draw: no failover
resolved faster than the 250 ms minimum (correct — nobody may start an
election earlier), the typical case lands within one timeout draw, and the
3-node max (560 ms) is one split vote followed by a second draw. Larger
clusters recover slightly more predictably: more nodes drawing timeouts
means the fastest draw is statistically earlier.

## Snapshot recovery

Crash a follower cold (fresh disk), write past its horizon so it can only
recover via InstallSnapshot, restart it, and measure until it has applied
everything.

| nodes | entries behind | value size | catch-up |
|---|---|---|---|
| 3 | 4,000 | 256 B | 171 ms |
| 3 | 16,000 | 256 B | 166 ms |

`raftkv-bench -mode recovery -nodes 3 -records 8000 -valuesize 256`

Catch-up time is flat in the number of entries behind — that's the point
of snapshots. The follower receives one state snapshot (~2–4 MB here)
instead of replaying 4k–16k entries; cost scales with *state size*, not
*history length*.

## Caveats, all of them

* One machine, in-process networking: no real RTTs, no TCP, no cross-AZ
  anything. On a real 3-node LAN cluster, add ~1 RTT to every write and
  read-lease renewal; the *shapes* above (flat write scaling, fsync
  plateau, timeout-bounded failover) are the durable findings, not the
  absolute numbers.
* Client and server share CPUs, so high-client mem rows are partly
  scheduler-bound (see mem @128 dipping below @32).
* Windows timer granularity flattens sub-0.5 ms percentiles to 0.
* Workload E omitted (no scans). D approximates "read latest" with a
  100-key recency window.
