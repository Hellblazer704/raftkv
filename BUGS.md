# BUGS.md — found by the chaos suite (and how)

A running log of real bugs caught during development, in the order they were
found. Kept deliberately honest: half the value of a Jepsen-style rig is that
it finds bugs in *itself* — a checker that cries wolf teaches you as much
about the model as a store that loses writes.

---

## 1. Follower/storage divergence on conflicting InstallSnapshot

**Found:** Phase 1, code review before the first snapshot test run.

**Symptom (would have been):** a follower restarting after an
InstallSnapshot could resurrect log entries that conflicted with the
snapshot's history.

**Root cause:** §7 of the paper says a follower receiving a snapshot keeps
its log suffix only if the entry at `lastIncludedIndex` matches the
snapshot's term. The in-memory log implemented that rule (`compact`), but the
`Storage` layer's `SaveSnapshot` unconditionally kept every entry with
`index > lastIncludedIndex`. When the retained-suffix check failed in memory,
storage still kept the conflicting suffix — so a crash+restart reloaded
entries the running node had (correctly) discarded.

**Fix:** decide `keepSuffix` once in `HandleInstallSnapshot` and mirror the
decision into storage with an explicit `TruncateSuffix(lastIncludedIndex+1)`
(`raft/snapshot.go`).

**Lesson:** any state that exists twice (memory + disk) needs a single
decision point. Divergence bugs hide until a crash lands in exactly the
wrong window.

---

## 2. "wrong leader" reply after log truncation → double execution

**Found:** Phase 2, first chaos runs; every schedule reported linearizability
violations (lost updates and resurrected values on the same key).

**Symptom:** client writes executed twice. Histories showed a value written
once by a client appearing in a read, disappearing, then reappearing.

**Root cause:** when a different entry committed at the log index a waiter
was parked on, the KV server replied `wrong_leader` — "definitely not
executed, safe to retry elsewhere." That's false. The deposed leader may have
replicated the entry before losing leadership; the new leader can commit that
same entry at a *different* index. The client, told "not executed," retried —
and the write applied twice.

**Fix:** an entry displaced at its expected index is `maybe`, never
`wrong_leader` (`chaos/kv.go`). The only sound "definitely not executed"
signals are: the server wasn't leader at `Start()` time, or it was already
dead.

**Lesson:** this is *the* canonical argument for client sessions. Without
dedup (client ID + sequence number), a client can never safely retry a
timed-out write. Phase 3's session layer exists because of exactly this bug.

---

## 3. Checker false positives from zero-duration operations

**Found:** Phase 2, while investigating bug 2 — fixing it did not clear the
violations, and *every* seed still failed on the first key checked, which
pointed at something systematic rather than six independent consensus bugs.

**Symptom:** 100% of chaos schedules reported violations, always on the
alphabetically first key.

**Root cause:** on Windows the monotonic clock ticks at ~0.5ms, and the
in-process simulated network can commit an entry in microseconds — so many
operations record identical call and return timestamps. The checker's event
comparator sorted returns before calls at equal times (intended to sequence
*different* ops that touch at a boundary). For a zero-duration op this placed
the op's **return before its own call**, corrupting the event list; the DFS
hit the orphaned return first and reported "no linearization" immediately.

**Fix:** calls sort before returns at equal timestamps (`linz/checker.go`).
This is also the only sound choice for *different* ops under a coarse clock:
equal timestamps mean the true order is unknowable, and treating the ops as
concurrent is the choice that cannot manufacture a false violation
(it only ever admits more legal linearizations).

**Lesson:** a verification tool has failure modes of its own. "Every seed
fails identically" is a signature of a broken oracle, not a broken subject.

---

## 4. Client leader cache never rotates on silent failures

**Found:** Phase 2, `TestKillLeaderMidCommit` — the cluster "never recovered"
after a leader kill, even though a new leader had been elected within 500ms.

**Symptom:** after a leader crash, every subsequent write from a client
failed as indeterminate, forever.

**Root cause:** a write attempt that fails without a reply must stop (the
request may have executed; retrying elsewhere risks double execution — bug 2).
But the client also kept its cached leader pointing at the dead node, so
every *future* write started — and ended — with the same silent failure.
The op-level rule (never retry an ambiguous write) was wrongly inherited by
the session-level routing state.

**Fix:** on a silent failure, the current op still gives up, but the cached
leader advances to the next replica so subsequent ops probe a live node
(`chaos/cluster.go`).

**Lesson:** safety rules about *an operation* must not freeze *the client*.
Liveness bugs like this don't show up in safety histories — only an explicit
"does the system make progress again" assertion caught it.

---

## 5. Two Raft clusters cross-talking through shared peer indices

**Found:** Phase 3, first sharded run — `TestShardBasicAndMigration` hung
forever; the goroutine dump showed clients looping on `ErrWrongGroup` while
the shard group never advanced past config 0.

**Symptom:** shard groups never elected leaders. Worse was possible: the
controller cluster was receiving the groups' RequestVote and AppendEntries
RPCs.

**Root cause:** `sim.RaftTransport` equated Raft peer index with network
endpoint id. Every earlier test had exactly one cluster at endpoints 0..n-1,
so the identity mapping silently worked. The sharded world puts the
controller at endpoints 0–2 and each group's replicas at higher ids — but
each group's Raft still addressed peers 0..2, i.e. the *controller's*
endpoints. Two independent consensus clusters were exchanging consensus
traffic; cross-cluster AppendEntries can truncate the wrong cluster's log,
which is a silent split-brain factory.

**Fix:** the transport takes an explicit peer-index → endpoint mapping
(`sim/raft.go`), and multi-cluster worlds must supply it.

**Lesson:** "it worked in every previous test" is exactly what an implicit
identity mapping looks like right up until the topology changes. Anything
that names a peer should name it in one namespace, once.

---

## 6. Disconnect set computed from a stale leader reading

**Found:** first CI run on GitHub's runners — `TestReElection` failed under
`-race` with "node claims leadership without a quorum", after hundreds of
clean local runs.

**Symptom:** the no-quorum assertion found a connected leader.

**Root cause:** the test captured `leader2`, reconnected the *old* leader,
then disconnected `leader2` and one other node. On slow runners the rejoin
plausibly triggers a fresh election that a different node wins — legal
behavior — so the test disconnected the wrong pair and left the actual
leader connected and (correctly, from Raft's perspective) still leading.
A harness bug: the assertion blamed the implementation for the test's own
stale read.

**Fix:** re-read the leader after the rejoin settles (`raft/raft_test.go`).

**Lesson:** tests that act on "who is leader" must act on a *current*
answer; leadership is a moving target by design. Different hardware timing
is itself a nemesis.

---

## 7. Sixteen linearizability checkers OOM a 16 GB runner

**Found:** same first CI run — all chaos shards died with SIGTERM ~90s in,
right when the first wave of 16 parallel schedules finishes and hands its
histories to the checker.

**Symptom:** exit 143 with no test output; jobs killed by the runner.

**Root cause:** two compounding effects. On Linux, `Sleep(rand(40ms))`
actually sleeps ~0 on small draws (Windows floors at ~15ms), so client
histories were several times denser than any local run — many more
*concurrent indeterminate writes*, which is the exact shape that makes the
WGL search space explode. And the checker's memoization cache had no size
bound, so 16 concurrent checkers each growing a multi-GB cache took the
runner down before the per-check time budget could fire.

**Fix:** a floor on client pacing (bounds history density across machine
speeds), a hard cap on the checker cache that returns "Unknown /
inconclusive" instead of growing (linz/checker.go), and chaos parallelism
of 8 in CI.

**Lesson:** a verifier needs a memory budget as much as a time budget —
"gives up honestly" must be an explicit state. And test workloads calibrated
by wall-clock sleeps are calibrated to one machine's clock.
