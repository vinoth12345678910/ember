# Step 3 - The GroupCoordinator

## What we are building
`GroupCoordinator`: the logic that runs on the leader of the `__groups` Raft group and answers JOIN_GROUP, HEARTBEAT, LEAVE_GROUP, OFFSET_COMMIT, OFFSET_FETCH and DESCRIBE_GROUP. It also runs the session-expiry check. It talks to Raft only through a tiny interface (`GroupLog`: am I the ready leader? which term? propose these bytes) and reads the replicated state from the `GroupStateMachine`. Time comes from an injected clock, so tests use a manual clock and never wait.

## Why it is designed this way
**Two kinds of state, on purpose.** Everything that must survive a crash (generation, members, assignments, committed offsets) lives in the Raft log. The *time of the last heartbeat* does not: a consumer heartbeats every few seconds, and putting each beat in the Raft log would turn the log into a heartbeat diary: thousands of entries, each fsynced and replicated, none of which is worth remembering after a minute. So heartbeat times are kept in the coordinator leader's memory only.

Analogy: the seating chart (generation, who has which table) is written in ink and copied to every manager. Whether a waiter has recently waved at the manager is just something the manager on duty remembers.

**What happens on failover.** The new leader has no heartbeat memory. It treats every member as if it had just heartbeated at the moment the leader took over: a full session timeout of grace. Consequence (documented honestly): a dead member is detected up to one extra session timeout later after a coordinator failover. The alternative (expire everybody who cannot be proven alive) would kick every consumer out of every group whenever a broker dies.

**The leader computes.** Join/leave/expiry compute the complete new membership and assignment locally and propose ONE `SET_MEMBERSHIP` guarded by the generation they started from. If another change committed first, the answer is `STALE_GENERATION`; the coordinator re-reads the state, recomputes and tries again (at most 5 times, then `REBALANCE_IN_PROGRESS`).

**Idempotent rejoin prevents rebalance storms.** A member told `REBALANCE_IN_PROGRESS` rejoins; if its subscription is unchanged, no new generation is created. It simply receives the current assignment. Without this, every member rejoining after one change would trigger another change, and the group would never settle.

**Heartbeats never touch Raft.** A heartbeat checks the soft state and the applied state, answers, done. Generation older than the group's: `REBALANCE_IN_PROGRESS`. Newer than the leader knows: `ILLEGAL_GENERATION`.

**A fresh leader must be READY** (it has applied an entry of its own term) before it answers anything: otherwise its applied state could be behind and it might say "unknown member" about a member that is perfectly fine.

## Alternatives I rejected and why
* Expiring members inside the state machine using timestamps: needs a clock in `apply()`; breaks determinism.
* Heartbeat counters in Raft "just every N beats": still log traffic, and still wrong after failover.
* One lock around the entire join: serialises all groups; the generation compare-and-set already makes races harmless, so only the tiny soft-state map is locked, never across a Raft wait.

## What goes wrong if you get it wrong
* Expiring on a stale clock after a failover: a mass kick-out of healthy consumers.
* Retrying `STALE_GENERATION` without recomputing: installs an assignment built from an old member list.
* Two members expiring at the same instant producing two proposals: the second is stale, and the group churns through two generations instead of one. Expired members are therefore removed together in one command.
* A rejoin that bumps the generation: rebalance storm.
* Holding the soft-state lock while waiting for Raft: heartbeats would block behind a slow commit and healthy members would expire.

## Tradeoffs and honest limits
* A join that times out with `REQUEST_TIMED_OUT` is ambiguous: a brand-new member may have been created whose id the client never learned. It is a ghost that expires after one session timeout. Harmless but visible in DESCRIBE.
* Reads (OFFSET_FETCH, DESCRIBE) come from the leader's applied state. A deposed leader that has not noticed yet can serve slightly old data (CheckQuorum bounds that to about one election timeout). Commits are always safe because they go through Raft.
* The coordinator is a single Raft group for all groups (Kafka shards groups over the partitions of `__consumer_offsets`); that is the scale-out path.

## How it is tested
`GroupCoordinatorTest` with a manual clock and a fake `GroupLog` that applies commands to a real `GroupStateMachine`: first join; second member triggers a new generation and a changed assignment; idempotent rejoin does not bump the generation; heartbeat with an old generation; session expiry removes only the silent member; two members expiring together produce ONE command; leave; concurrent joins that lose the compare-and-set converge; a new leader gives a full grace period; invalid session timeouts rejected; commit fencing; not-leader and not-ready answers.

## Update (Phase 7)
The tick also calls `refreshAssignment`: if the assignment derived from the CURRENT topic list differs from the installed one (typically because a topic reached this broker after the members joined), a new generation is proposed with the same members. See `docs/phase7/design.md` part 7. `commitSync()` also now returns a boolean (true only if a commit was sent), and `poll()` no longer drops records already fetched when a later partition's fetch fails.
