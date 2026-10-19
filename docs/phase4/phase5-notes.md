# Phase 5 notes: wrapping the single-threaded core in a real runtime (plan only)

This is a plan, not code. The core (`RaftNode`) is a deterministic state machine that something else drives. A real runtime has four jobs: drive it, deliver messages, persist state, and apply committed entries.

## 1. One event loop per Raft group
* One thread (or one Netty event-loop task queue) per Raft group owns its `RaftNode`. Everything that touches the node - incoming messages, timer ticks, client proposals - is submitted to that thread's queue, so the "caller serializes all calls" rule holds without locks. This is the same trick as Node.js: no shared mutable state, only a queue.
* Timers: after every call the loop reads `node.nextTimerMs()` and schedules ONE wake-up at that absolute time (replacing the previous one). On wake-up it calls `tick(System.nanoTime()/1_000_000)`. Time must come from a monotonic clock (`nanoTime`), never wall-clock time.
* Many groups (one per partition) can share a pool of loops; a group is pinned to one loop forever.
* Clients call `propose()` through the loop and get a future that completes when `onCommitted` reports that index (and fails if leadership is lost).

## 2. A real transport
* An implementation of `RaftTransport` that serializes `RaftMessage` with the Phase 2 protocol conventions (a new apiKey family, length-prefixed frames, versions) over Netty, with one connection per peer.
* `send` must never block the loop: it enqueues to the connection's outbound buffer and drops when the buffer is full (Raft tolerates loss; back-pressure is replaced by dropping + retry through heartbeats).
* Messages carry the sender's group id so one connection can carry many groups.

## 3. A durable RaftStorage on the Phase 1 log
* `currentTerm` + `votedFor`: a tiny file replaced atomically (temp file + fsync + rename), exactly like the topic metadata file in Phase 3.
* The Raft log: an instance of the Phase 1 segmented `Log` (append-only, CRC, recovery). `truncateFrom(index)` needs a new Phase 1 capability (delete a suffix) that does not exist yet - add it and test it with the Phase 1 chaos test.
* `sync()` = fsync of the log and the term/vote file. **fsync batching (group commit):** the loop drains everything that arrived, handles it, and calls `sync()` once before releasing any of the resulting messages - the outbox pattern already guarantees correctness; batching only changes how many events share one fsync. The Phase 1 numbers (fsync ~4 ms) show why: one fsync per message limits a group to a couple hundred writes per second, one per batch removes the limit.
* The `InMemoryRaftStorage` crash model ("unsynced writes vanish") is the specification the real storage must meet; run the whole simulator against the real storage with simulated crashes (kill the process at fault points) before trusting it.

## 4. Applying committed entries
* `onCommitted` runs on the loop. A slow state machine would stall consensus, so hand entries to an apply thread through a queue and track `applied` separately; report the applied index back for client futures.
* After restart a node re-applies from index 1 (`lastApplied` is volatile). That is slow for big logs: **snapshots** (log compaction) become necessary - a later phase.

## 5. Things the runtime must add (non-goals of Phase 4)
* Cluster membership from configuration (fixed set at first), then membership changes.
* Leader leases / ReadIndex for linearizable reads; client sessions for exactly-once writes.
* PreVote and CheckQuorum to stop disruptive nodes and stranded leaders (the simulator shows both effects: a partitioned node forces an extra election after healing; a stranded leader accepts commands it can never commit).
* Metrics (term, role, commit lag, elections per minute) and structured logging of role changes.

## 6. Safety rails
* `RaftConfig.injectedBug` must be `Bug.NONE` in production; add a check that fails startup otherwise.
* Keep the simulator in the build: every change to `RaftNode` runs the default 300 seeds; run `-Praft-long` before releases.
* Plan cluster-level chaos tests with the same checkers over the real runtime (kill -9 at random points, drop packets with a proxy), using the same `InvariantChecker` ideas on collected logs.
* Wiring to the broker: one Raft group per partition (Phase 3 partitions), the leader of each group serves produce/fetch; METADATA v1 `leaderNodeId` (added in Phase 3) is where that leadership is advertised.
