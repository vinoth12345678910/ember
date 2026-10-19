# Step 2 - GroupCommands, GroupStateMachine and the Assignor

## What we are building
* `GroupCommands`: the two commands that live inside the `__groups` Raft log. `SET_MEMBERSHIP` (type 1) installs a complete new membership with its assignment, guarded by a compare-and-set on the generation. `OFFSET_COMMIT` (type 2) stores offsets, fenced by (memberId, generation).
* `GroupStateMachine`: the state machine of that log. Per group: generation, members (with subscriptions and assigned partitions), committed offsets per (topic, partition).
* `Assignor`: a pure function that divides partitions among members (range assignment).

## Why it is designed this way
**Think of waiters sharing tables.** The group is a team of waiters, the topic partitions are tables. The *generation* is the version number of the seating chart. Whenever somebody joins or leaves, a new chart with a new number is issued, and waiters must work from the latest one.

**The leader computes, the state machine only installs.** The coordinator (the leader of `__groups`) decides who gets which partitions and proposes ONE command that contains the entire new chart. `apply()` does nothing but check "is this chart based on the version I have?" and swap it in. Why: every broker applies the log, now and again years later on a fresh machine. If `apply()` computed the assignment itself it would need the current topic list (a different Raft group that may be at a different point in its own log), a clock, or a random number. Replicas would then disagree, silently, forever. This is the rule "apply is a pure function of (state, command)": no clocks, no randomness, no I/O, no looking at other groups. Everything the command needs (member ids, even the generated `<clientId>-<uuid>` ids) is created by the leader before proposing.

**Compare-and-set on the generation.** Two coordinator actions can race (two joins at once, or an expiry and a join). Both are computed from generation N. The first one commits and makes N+1; the second still says "based on N" and is answered `STALE_GENERATION` with no change. The coordinator recomputes from the new state and retries. Nothing is lost, and nobody ever installs a chart computed from stale data.

**Fencing happens in apply.** `OFFSET_COMMIT` checks the member id, the generation and the ownership of each partition at *apply* time, so the check is ordered by the Raft log against membership changes. A zombie consumer (paused, partitioned or just slow, already replaced) can never overwrite the offsets of the new owner, even when its commit was already in flight when the new chart was committed: the log decides which came first.

**Range assignor.** For each topic: members subscribed to it, sorted by member id; with `m` members each gets `floor(P/m)` contiguous partitions and the first `P mod m` members get one extra. Deterministic, balanced to within one, simple to explain and test.

## Alternatives I rejected and why
* **Compute the assignment inside `apply()`.** Breaks determinism (see above).
* **Replicate heartbeats.** Thousands of tiny log entries for no durable value; heartbeats stay in coordinator memory (step 3).
* **Round-robin, sticky, cooperative assignors.** Round-robin spreads better across many topics but scatters partitions. Sticky keeps most partitions where they were (fewer duplicates after a change) but needs the previous assignment as input and more tests. Cooperative rebalancing needs a two-phase protocol. All are good future work; the range assignor is the smallest correct thing. Its known downside: it is **not sticky**, so adding one member can move many partitions.
* **Per-partition error codes in OFFSET_COMMIT.** All-or-nothing is simpler to reason about and matches one atomic log entry.

## What goes wrong if you get it wrong
* Non-deterministic apply: replicas diverge, e.g. broker 2 thinks member A owns partition 3 and broker 3 thinks B does. Nobody notices until a failover.
* Checking the generation before proposing but not at apply: a commit in flight during a rebalance would be accepted after it, overwriting the new owner's offsets.
* A decode error thrown from `apply()` would stop the group on every replica. A malformed command is therefore answered `INVALID` deterministically and changes nothing.
* Forgetting that an EMPTY group keeps its offsets: a consumer that restarts after everyone left would start from scratch.

## Tradeoffs and honest limits
* State is copy-on-write per group: a commit copies that group's offset map. Fine for tens of thousands of partitions, not for millions. A persistent map is the scale-out answer.
* Offsets are never expired and groups are never deleted (non-goals). The `__groups` log is never compacted (Phase 8).
* The assignor knows only partition counts; it does not balance by load.

## How it is tested
`AssignorTest` (contiguity, balance within one, determinism, exactly-one-owner, different subscriptions, one member, more members than partitions, unknown topics ignored, 2,000 random inputs), `GroupCommandsTest` (round trips, truncation at every length, trailing bytes, bad counts), `GroupStateMachineTest` (create and change membership, stale generation changes nothing, every offset-commit accept and reject case and its all-or-nothing behaviour, empty group keeps offsets, replay into a fresh machine gives an identical snapshot, 1,000 random command sequences applied to two machines give identical results and snapshots).
