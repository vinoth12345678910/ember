# Phase 6 design: consumer groups

A consumer group lets many consumers read one set of topics together. The partitions are divided among the live members; when a member joins, leaves, crashes, or the coordinator broker fails over, the group repairs itself, and every member resumes from the group's last **committed** offset. The committed offsets are replicated by Raft, so they survive the death of any minority of brokers. Delivery is **at-least-once**.

Think of a restaurant. The partitions are tables, the consumers are waiters, the coordinator is the head waiter. The **generation** is the version number of the seating chart: whenever a waiter arrives or leaves, a new chart with a new number is issued. A waiter who still works from an old chart is recognised by its number and is not allowed to write on the new chart.

## 1. The life of a consumer

```
 consumer (user thread)        consumer (heartbeat thread)       coordinator (leader of __groups)          Raft (__groups log)
        |                               |                                   |                                     |
        |-- FIND_COORDINATOR ---------->|(any broker answers)               |                                     |
        |-- JOIN_GROUP (id "", topics) ------------------------------------>| 1 compute members + range assignment|
        |                               |                                   | 2 propose SET_MEMBERSHIP(base=N)    |
        |                               |                                   |------------------------------------>| commit + apply: generation N+1
        |<------------- generation N+1, memberId, my partitions -------------|<------------------------------------|
        |-- OFFSET_FETCH (my partitions) --------------------------------->|  (read the applied state)           |
        |   start = committed offset, or the reset policy (earliest/latest/none)                                   |
        |                               |                                   |                                     |
        |== poll(): FETCH from each partition leader, records returned to the application =========================|
        |   process the records ...     |-- HEARTBEAT(member, N+1) -------->| refresh last-seen (memory only)     |
        |                               |<------------ OK ------------------|  (NO Raft entry)                    |
        |-- OFFSET_COMMIT(member, N+1, positions) ------------------------>| pre-check, then propose            |
        |                               |                                   |------------------------------------>| apply: member known? generation
        |<------------- OK ---------------------------------------------- |<------------------------------------|        current? partitions owned?
        |                               |                                   |                                     |
        |        ... another member joins: coordinator commits SET_MEMBERSHIP(base=N+1) -> generation N+2 ...        |
        |                               |-- HEARTBEAT(member, N+1) -------->| generation is older                  |
        |                               |<-- REBALANCE_IN_PROGRESS ---------|                                     |
        |   poll(): flag seen -> drop positions and ownership                |                                     |
        |-- JOIN_GROUP (same member id, same topics) ---------------------->| idempotent rejoin: NO new generation |
        |<------------- generation N+2, my NEW partitions -------------------|                                     |
        |   positions re-read from the committed offsets; uncommitted work of the old chart is repeated            |
```

Rules the consumer follows: **process, then commit** (positions advance only for records `poll()` returned; auto-commit, checked inside `poll()`, commits the positions as of the *previous* poll); a commit refused with `ILLEGAL_GENERATION`/`UNKNOWN_MEMBER_ID` throws `CommitFailedException` and the consumer rejoins on the next poll; `close()` commits (if auto-commit), then leaves, so its partitions move at once.

## 2. Durable state versus soft state (and why heartbeats never touch Raft)

| | Durable, replicated (in the `__groups` Raft log) | Soft, leader-only (coordinator memory) |
|---|---|---|
| What | generation, members (id, client id, session timeout, subscription, assigned partitions), committed offsets | time of each member's last heartbeat |
| Changes | on join / leave / expiry / commit | every few seconds per member |
| Survives a broker crash | yes (replay of the log) | no, and it should not |
| Cost | one fsynced, replicated log entry | a map update |

A consumer beats every few seconds. With a thousand consumers that would be hundreds of fsynced, replicated Raft entries per second, all worthless a minute later. So heartbeats are answered from memory; only *changes of membership* and *offset commits* go through Raft.

**What a coordinator failover costs.** The new leader has no heartbeat memory. It starts every member's session fresh, a **full session timeout of grace**; it also does this whenever its Raft term changes. Consequence: a member that died just before the failover is noticed up to one extra session timeout later. The alternative (expire everybody who cannot be proven alive) would throw every consumer out of every group whenever one broker dies.

## 3. The leader computes, the state machine installs (determinism)

Every broker applies the `__groups` log. Replicas, today and when they replay the log in five years, must reach exactly the same state. So `GroupStateMachine.apply()` is a pure function of `(state, command)`: no clock, no randomness, no I/O, no look at other Raft groups.

**The tempting alternative** is to send a small command ("member X joined") and let `apply()` compute the new assignment. It needs the list of topic partition counts (which lives in the META group, at a different log position on each broker), the member order (fine), and a new member id (random). Two brokers applying the same entry would see different topic lists, or generate different ids, and *silently disagree for ever about who owns partition 3*. No exception, no log line: just two brokers answering differently after the next failover.

**What we do instead.** The coordinator leader computes everything, including the complete assignment and the generated member id, and proposes **one** command with the whole new membership, guarded by a compare-and-set on the generation:

```
SET_MEMBERSHIP(group, baseGeneration = N, newGeneration = N+1, [members with their assigned partitions])
apply: group exists? stored generation must equal N (new group: N must be 0) else STALE_GENERATION, nothing changes.
```

If two changes race (two joins, a join and an expiry) both are computed from generation N; the first commits, the second answers `STALE_GENERATION`; the coordinator re-reads the state, recomputes and retries (at most 5 times, then `REBALANCE_IN_PROGRESS`). A command that is malformed or inconsistent (a partition with two owners, a member twice) is rejected as `INVALID` identically on every replica, never thrown.

## 4. Generation fencing: a worked zombie example

Setting: group `g`, topic `orders` with partitions 0 and 1. Session timeout 10 s.

1. Consumer **A** joins. Generation 1: A owns {0, 1}. A reads partition 0 up to offset 40 and 1 up to offset 25, but has not committed yet.
2. A's machine freezes for 15 s (a stop-the-world pause, a VM migration, a network cut). Its heartbeats stop.
3. After 10 s the coordinator expires A: `SET_MEMBERSHIP(base 1 -> generation 2, no members)`. Consumer **B** joins: generation 3, B owns {0, 1}. B starts from the committed offsets (none) and reads from the beginning; it commits 10 and 12 as it goes.
4. A wakes up. It does not know anything happened. It calls `OFFSET_COMMIT(member A, generation 1, {0:40, 1:25})`.
5. The coordinator's cheap pre-check says `UNKNOWN_MEMBER_ID` (A is not a member any more) and nothing is even proposed. Suppose instead that A was still a member (it had only been replaced, not yet expired): the commit reaches the Raft log, and **apply** rejects it with `ILLEGAL_GENERATION` because the group is at generation 3, or `PARTITION_NOT_ASSIGNED` if the partition moved. Nothing is stored (all or nothing).
6. A's `commitSync()` throws `CommitFailedException`; A drops its positions and rejoins on the next poll. B's offsets were never touched.

**Why the check is in `apply` and not only before proposing:** a commit and a rebalance can be in flight at the same time. The Raft log puts them in one order. If the commit is ordered first, it is a legitimate commit by the owner of that moment; if second, it is fenced. A check made only *before* proposing would accept a commit that is ordered after the rebalance.

## 5. The eager rebalance and its duplicate window

**What we do.** Eager and server-assigned, with no barrier. When membership changes, the new generation and the new assignment are committed *immediately*. Members that are still on the old generation find out on their next heartbeat (`REBALANCE_IN_PROGRESS`), stop consuming, and rejoin (an idempotent rejoin just returns their new assignment).

**The honest consequence.** As soon as the new generation is committed, the old owner can no longer commit offsets: it is fenced. Work it processed but had not committed yet will be redone by the new owner. The window of repeated records is therefore *the time since the last commit* (bounded by the commit interval, 5 s by default), plus whatever the old owner delivers between the rebalance and its next heartbeat.

| | Ember (this phase) | Kafka classic eager (JoinGroup + SyncGroup) | Kafka cooperative |
|---|---|---|---|
| Protocol | one round trip; the coordinator commits the result | all members must rejoin (a barrier), then one member computes and the leader distributes it | two rebalance rounds; only the partitions that really move are revoked |
| Who computes the assignment | the coordinator (broker side) | a group leader (client side), custom assignors possible | same |
| Members stop consuming | all, until they learn the new assignment | all, during the barrier | only the ones that move |
| Duplicates | up to one commit interval per moved partition (and the old owner cannot commit them) | similar, plus the barrier time | much smaller |
| Complexity | low | medium | high |

The barrier of classic Kafka gives the old owners a chance to commit *before* the new assignment is final. We trade that away for simplicity: no waiting for slow members and no client-side assignors. Cooperative rebalancing and sticky assignors are the real fix for the duplicate window and are left as future work. Our range assignor is also not sticky: adding one member shifts many ranges.

## 6. Failure matrix

| Situation | What happens | What the user sees |
|---|---|---|
| **Consumer crashes** (`kill -9`) | Heartbeats stop. After its session timeout the coordinator removes it in one `SET_MEMBERSHIP`; its partitions go to the others, who start at the committed offsets. | Other members see a rebalance; records read but not committed by the dead member are delivered again to the new owners (at-least-once). Detection takes up to session timeout + one expiry tick. |
| **Consumer paused longer than its session** (GC, VM freeze) | Same as a crash for the group. When it wakes, its heartbeat gets `UNKNOWN_MEMBER_ID`; a commit gets `UNKNOWN_MEMBER_ID` or `ILLEGAL_GENERATION`. | `CommitFailedException`; the consumer rejoins as a new member. Its stale commit changed nothing (fencing). |
| **Consumer slow but alive** (long processing) | The heartbeat thread keeps the session alive. After `maxPollIntervalMs` (default 5 min) without `poll()` the consumer leaves the group. | Nothing until the limit; then a rebalance. Process within the limit or raise it. |
| **Coordinator broker crashes** | The other brokers elect a new `__groups` leader. It has the replicated state (members, generation, offsets) and gives every member a full session of grace. Clients get `NOT_COORDINATOR`/connection errors, run FIND_COORDINATOR again and continue with the SAME member id and generation. | A pause of roughly an election timeout; commits during it are retried by the client. No rebalance in the normal case. A dead member is noticed up to one extra session timeout later. |
| **Coordinator isolated** from the others | CheckQuorum makes it step down within about one election timeout; its commits cannot reach a majority and fail. The majority side elects a new coordinator. | `REQUEST_TIMED_OUT`/`NOT_COORDINATOR` on commits, then normal operation at the new coordinator. A DESCRIBE answered by the old one in that window may be slightly stale. |
| **Whole-cluster restart** | Every broker replays `__groups`: groups, generations, members and offsets are back. Sessions start fresh at the new coordinator. | Consumers get connection errors, then reconnect. Members whose sessions ran out rejoin; consumption resumes from the committed offsets. |
| **Commit lost in flight** (timeout, connection reset) | The commit may or may not have been applied. | `REQUEST_TIMED_OUT`/`IOException`; the next commit covers it. If the old commit is applied *after* a newer one, the stored offset moves back: only more duplicates, never a skipped record. |
| **Rebalance during a commit** | The Raft log orders the two. Commit first: accepted (the owner of that moment). Rebalance first: fenced at apply. | `OK`, or `CommitFailedException`. Both are safe. |
| **Retention removes the committed offset** (committed offset below the log start) | The start position falls back to the reset policy. | `earliest`: continue at the oldest record that still exists (data was lost to retention, by configuration). `latest`: continue at the end. `none`: `EmberException(OFFSET_OUT_OF_RANGE)`. Topics cannot be deleted in Ember, so "topic deleted" cannot occur. |

## 7. Exact guarantees and non-guarantees

**Guaranteed** (while a majority of the brokers is alive and Raft logs are fsynced):
1. **At-least-once.** A record is never skipped by the group: positions are committed only for records `poll()` has returned, and a committed offset is never ahead of the log.
2. **Exclusive ownership.** In any generation each partition is assigned to at most one member, and an offset commit is accepted only from the current owner in the current generation (fencing in `apply`).
3. **Durable offsets.** An acknowledged `OFFSET_COMMIT` survives the loss of any minority of brokers and a whole-cluster restart.
4. **Deterministic group state.** All brokers' `__groups` state machines are identical after catching up.
5. **Idempotent rejoin.** Rejoining with an unchanged subscription never starts a rebalance.

**Not guaranteed:**
* **Exactly-once.** Duplicates happen after rebalances, consumer crashes and ambiguous commits. The duplicate window is bounded by the commit interval (plus the heartbeat detection delay).
* **No duplicates between rebalances either**, if the application crashes after processing and before the next commit.
* **Fresh reads from a deposed coordinator.** DESCRIBE and OFFSET_FETCH are served from the leader's applied state; a coordinator that has been deposed but has not noticed yet may answer slightly stale (CheckQuorum bounds it to about an election timeout). Commits always go through Raft.
* **Rebalance when partitions are added.** The partition count of a topic is fixed, so there is nothing to rebalance. (A topic that appears *after* the members subscribed, or whose creation had not yet reached the coordinator when they joined, IS picked up: since Phase 7 the coordinator re-derives each group's assignment from the current topic list on its tick and installs a new generation if it differs. Before that fix such members could be left with an empty assignment for ever; found by the Phase 7 chaos test.)
* **A ghost member after an ambiguous JOIN.** A join that times out may have created a member whose id the client never saw; it expires after one session timeout.
* Ordering across partitions (as before).

## 8. Known limitations and the plan

* **One coordinator for all groups.** Kafka shards groups over the partitions of `__consumer_offsets` by hash of the group id, so different brokers coordinate different groups. Sharding `__groups` that way is the scale-out path (it also needs a mapping from group id to shard in FIND_COORDINATOR).
* **The `__groups` log is never compacted.** Every commit is one more entry, forever; a busy group makes the log (and the replay at startup) grow without bound. Phase 8 (log compaction / snapshots) fixes it.
* **No offset expiry, no group deletion.**
* **Eager rebalancing, range assignor.** Cooperative rebalancing and sticky assignment would shrink the duplicate window; custom client-side assignors are a non-goal.
* **Commit cost.** Every `OFFSET_COMMIT` is a Raft entry with an fsync on a majority (see the measured numbers in `docs/ember-part7-notebooklm.md`); commit at intervals, not per record.
* **Simple stuck-consumer protection** (`maxPollIntervalMs`) instead of Kafka's full rebalance-timeout protocol.
* No long polling, no quotas, no ACLs, no transactions, no exactly-once, no idempotent producer (all later or non-goals).
