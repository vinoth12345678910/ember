# Step 4 - The simulator core: virtual time, a hostile network, crashes, and a trace

## What we are building
`SimCluster`: a whole Raft cluster inside one thread. It has a **virtual clock**, a **priority queue of events**, a **fake network** (latency, drops, duplicates, partitions), **crash/restart** of nodes (with a storage that really loses unsynced writes), a **trace** you can read, and deterministic randomness. `SimMain` is the command-line way to watch it.

## The Raft rules involved
None are new: this step builds the world the rules are tested in. The failure model comes from the paper's assumptions (Section 2 / Figure 2 preamble): servers may crash and restart with stable storage intact; the network may lose, delay, duplicate and reorder messages, but not corrupt them (no Byzantine behaviour).

## Why it is designed this way
**Distributed bugs live in rare interleavings.** "Node 3 crashes between sending a reply and flushing the disk, while a delayed AppendEntries from the old leader arrives" will not appear in a hand-written test, and on a real cluster it appears once a month and cannot be reproduced. A simulator turns that into a *search problem*: run thousands of random schedules, check the safety laws after every single event, and when one fails, replay it from its seed.

**Virtual time, event-driven.** Real clocks make runs unrepeatable and slow (waiting 300 ms for a timeout). Our clock is a `long`. The main loop picks the next thing that happens (the earliest queued message delivery or the earliest node timer reported by `nextTimerMs()`), jumps the clock there, and handles exactly that one event. 45 seconds of cluster life takes a fraction of a real second. (Same idea as game engines with fixed timesteps or Redux-style event replays.)

**Determinism, enforced by construction.**
* One thread. No `HashMap` iteration with non-integer keys; nodes always iterate in ascending id order; the queue orders events by `(time, sequence number)` so ties never depend on memory addresses.
* Randomness only from `java.util.Random(seed)`. One random stream per node (derived from the global seed and the node id) and one for the network, so adding a log line or a node never shifts another stream.
* Same seed -> byte-identical trace. A **rolling hash of every trace line** makes that testable without keeping gigabytes of text.

**The network model is deliberately nasty.** A message gets a random latency, so messages overtake each other (reordering for free). It can be dropped, duplicated (the copy gets its own latency), blocked by a partition at *delivery* time (so a message already "on the wire" when the cable is cut is lost), or addressed to a dead node (lost).

**Crash = lose unsynced writes.** `crash(n)` throws the `RaftNode` away and calls `storage.crash()`. The simulator can also crash a node *in the middle of a call*, right after it sent a message (the transport throws a private exception that unwinds the node). With correct code the state was already synced before anything is sent, so nothing a peer relied on is lost; with `Bug.ACK_BEFORE_SYNC` the promise vanishes - and that is how Step 9 proves the checkers notice.

**The trace** is a compact line per significant event:
```
t=00152 n2 election timeout -> CANDIDATE term=1
t=00152 n2 -> n1 RequestVote(term=1,lastIdx=0,lastTerm=0)
t=00171 n1 -> n2 RequestVoteResponse(term=1,granted=true)
t=00171 n2 LEADER term=1
```
Only the last N lines are kept in a ring buffer (so a violation can show its last 60 lines); the full text is produced by **re-running the failing seed** with full recording on, which determinism makes cheap and exact.

## What goes wrong if you get it wrong
* A single `System.currentTimeMillis()` or `HashSet` iteration in the core and "same seed, same run" silently stops being true, and a failing seed can no longer be replayed. The determinism test is the alarm.
* Dropping a message at *send* time only would miss "link cut while in flight".
* Delivering to a crashed node's old `RaftNode` object would hide the crash.

## Alternatives I rejected and why
* Real threads and sockets with random delays (Jepsen-style): realistic but not replayable and 1000x slower.
* Ticking every 1 ms: needlessly slow; `nextTimerMs()` exists to avoid it.
* A mocking framework: our own 300 lines are easier to read and fully deterministic.

## Tradeoffs and honest limits
The simulator models message loss, delay, duplication, partitions and crashes with lost unsynced writes. It does not model torn partial writes, disk corruption, clock drift between nodes (all nodes share one perfect virtual clock, so a bug that depends on skewed clocks cannot be found), or slow (but not dead) nodes except through latency.

## How it is tested
Determinism (same seed -> same trace hash and final state; different seeds -> different traces); partitions block delivery and heal restores it; messages to a dead node are lost; drop and duplicate probabilities have the expected effect on the statistics; the virtual clock only moves forward and `runFor` lands exactly on its target; a crash loses only unsynced writes.
