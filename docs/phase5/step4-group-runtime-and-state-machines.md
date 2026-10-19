# Step 4 - GroupRuntime, the state machines, and idempotent apply

## What we are building
* **`GroupRuntime`**: wraps ONE `RaftNode` and gives it a thread, a mailbox and timers. Many groups share a small worker pool and one scheduler.
* **`PartitionStateMachine`**: applies committed `PRODUCE_BATCH` entries to the partition's **data log** (the Phase 1 `Log`) - idempotently.
* **`MetadataStateMachine`**: applies `CREATE_TOPIC` entries of the META group.
* **`LocalTransport`**: an in-process "network" for tests, with blockable links, drops and delays.

## Why it is designed this way
**A serial mailbox, not a lock.** `RaftNode` is deliberately not thread-safe. Everything that touches it - an incoming message, a timer tick, a client `propose` - becomes a task in the group's queue; at most one worker drains the queue at a time (an atomic "scheduled" flag decides who). It is the Node.js idea: one logical thread per group, no shared mutable state, but the *threads* are borrowed from a shared pool so 100 partitions do not need 100 threads. A runtime assertion checks that every call into the node happens on the thread that currently owns the mailbox.

**Timers come from the node itself.** After each batch of tasks the runtime reads `node.nextTimerMs()` and arms ONE scheduled wake-up for that moment. When it fires it enqueues a `tick`. (If the scheduler wakes a millisecond early the tick does nothing and we must re-arm - a bug waiting to happen that the runtime guards against by forgetting the armed time before every tick.)

**Propose-and-wait.** `propose` runs in the mailbox: if the node is not the leader the future fails with `NotLeader(hint)`; otherwise the entry gets `(index, term)` and the future is parked in a map keyed by index. When the **state machine applies that index** and the applied entry's **term equals the registered term**, the future completes with the state machine's result (for a produce: the base offset). If the entry at that index has a *different term* (our entry was overwritten by a new leader) or we lose leadership first, every pending future fails with `NotLeader`. This is the "classic ambiguity": a failed future does NOT mean the record is lost - the new leader may still commit it - so retries are at-least-once. In a single-node cluster the entry commits and applies *inside* `propose`; the runtime remembers results of entries applied during the call so the future is not lost.

**Two logs per replica, on purpose.**
* the **Raft log** (`FileRaftStorage`): durable, fsynced before any reply; the source of truth - the newsroom's signed minutes;
* the **data log** (`Log`): written only by the state machine when an entry is **committed and applied** - the published newspaper.

Consumers read the data log, so they can never see uncommitted data; the high watermark is just the end of the data log; zero-copy fetch works exactly as before. Cost: every record is written twice (and the Raft log keeps growing, no compaction). The alternative real systems use is to unify them (the Raft log *is* the partition log, a commit index tells readers how far they may read); it saves the double write but entangles Raft truncation with the storage engine. We chose clarity.

**Idempotent apply - the crash-consistency heart.** The data log is not fsynced per record (policy `never`) - the Raft log can rebuild it. After a restart the node replays committed entries **from index 1**, so `runningOffset` (the number of records contained in the entries applied so far) restarts at 0 while the data log may already contain some, all, or *part* of a batch. Worked example - batches A (3 records), B (4 records), C (2 records); the process dies while writing B:

| data log after the crash (and Phase 1 recovery cut the torn record) | holds | `nextOffset` |
|---|---|---|
| A fully, B's first 2 records | 5 records | 5 |

Replay: apply A: `running=0`, `0+3 <= 5` -> already there, skip; `running=3`. Apply B: `3+4=7 > 5` and `3 < 5` -> partly there: append only the missing records (positions `5-3=2` onward, i.e. B's last 2); `running=7`. Apply C: `running(7) == nextOffset(7)` -> append both; `running=9`. The data log now holds exactly 9 records, offsets 0..8, identical to every other replica. Because every replica applies the same entries in the same order, **offsets are identical on all brokers**. If `running > nextOffset` the data log lost more than a torn tail while we were not looking - we fail loudly instead of guessing.

**Timestamp inside the command.** The leader stamps the batch; all replicas store the same timestamp, so retention (which compares timestamps with "now") makes the same decision everywhere.

## Alternatives I rejected and why
* `synchronized` around the node: callers would block each other, and the inner callbacks (apply, send) would run under the lock.
* One thread per group: wasteful for hundreds of partitions.
* Applying with "append whatever is not there" using record timestamps or keys: ambiguous; counting records by position is exact.
* Fsyncing the data log on every apply: doubles the fsync cost for no safety gain.

## What goes wrong if you get it wrong
* Completing a future when the entry is *committed* but before it is applied: the client could be acknowledged and then not see its record.
* Completing a future without checking the term: an acknowledged write that was actually overwritten.
* Non-idempotent apply: after a crash the replay duplicates records on that replica only; replicas diverge silently and offsets stop agreeing.
* Letting a timer be armed twice or lost: either busy-ticking or a node that never starts an election.

## Tradeoffs and honest limits
`propose` futures live only in memory: a broker crash loses them (clients retry: at-least-once). Replicas may differ slightly in `logStartOffset` because each runs retention with its own clock. The Raft log is never compacted in this phase. Tests use real threads with short Raft timings (election 100-200 ms) so they take real, but small, time.

## How it is tested
Real threads and `LocalTransport`: three replicas elect a leader; a proposal is applied on all and returns the same base offset; a restarted follower catches up; leader isolation fails pending futures with `NotLeader`; a new leader serves later proposals; no acknowledged proposal is lost; overload and timeout; idempotent apply with (a) a complete data log, (b) a data log cut in the middle of a batch, (c) a deleted data log; all replicas' data logs end up byte-identical; the mailbox never runs two tasks of a group at once.
