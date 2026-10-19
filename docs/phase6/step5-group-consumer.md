# Step 5 - The client: coordinator discovery and EmberGroupConsumer

## What we are building
* `ClusterClient` learns the group APIs. It finds the coordinator with FIND_COORDINATOR, caches it, and forgets it on `NOT_COORDINATOR`, `COORDINATOR_NOT_AVAILABLE` or a connection failure; then it asks again after a back-off.
* `EmberGroupConsumer`: `subscribe`, `poll(Duration)`, `commitSync`, `position`, `assignment`, `generation`, `close`. The old manual `EmberConsumer` is unchanged.
* `GroupConsumerConfig`, `GroupRecord`, `CommitFailedException`.

## Why it is designed this way
**A consumer is a waiter with a tray.** `poll()` is "go to my tables and bring what is ready". Its job order is: make sure I have a valid seating chart (join if not), commit what I finished last time (auto-commit), then fetch.

**Start position.** For each partition I was assigned: the group's committed offset if it exists and is still inside the log; otherwise the `autoOffsetReset` policy (`earliest`, `latest`, or `none` = throw). The same policy applies when a fetch says `OFFSET_OUT_OF_RANGE` because retention removed the data.

**Process, then commit.** Positions advance only for records `poll()` returned. Auto-commit, checked inside `poll()`, commits the positions as of the *previous* poll, i.e. only after the user had a chance to process those records. A crash between processing and committing means the records are delivered again: at-least-once, never skipped.

**A background heartbeat thread with its own connections.** If heartbeats came only from `poll()`, a consumer busy with one slow batch would look dead and lose its partitions. The thread uses a separate `ClusterClient` (the main one allows one request in flight, and its user thread may be inside a long commit or fetch). The thread never joins or consumes; it only beats and, when the coordinator says `REBALANCE_IN_PROGRESS`, `UNKNOWN_MEMBER_ID` or `ILLEGAL_GENERATION`, sets an atomic flag. `poll()` sees the flag, drops its positions and ownership and rejoins. The rejoin is idempotent on the server, so a stale flag causes at most one harmless extra join, never a rebalance storm.

**The trade-off.** A stuck user thread keeps the membership alive for ever, because the heartbeat thread is healthy. Kafka solved this with `max.poll.interval.ms`; we do the same in its simplest form: after `maxPollIntervalMs` (default 300000) without a `poll()` the consumer leaves the group.

**Coordinator failover is invisible to the group.** The member id and generation are replicated, so after the coordinator moves the consumer simply finds the new one (FIND_COORDINATOR) and continues with the same member id and generation.

**Fenced commits.** If `commitSync()` is answered `ILLEGAL_GENERATION` or `UNKNOWN_MEMBER_ID`, someone else owns these partitions now. The consumer throws `CommitFailedException` (the batch may be reprocessed) and rejoins on the next poll.

## Alternatives I rejected and why
* Heartbeating from `poll()` only: kills slow-but-healthy consumers.
* One shared connection for heartbeats and fetches: a long fetch would delay the beat, and the lock in `ClusterClient` would serialise them.
* Throwing a checked exception for a fenced commit: users need to catch it in the middle of normal loops; an unchecked exception with a clear name is the Kafka convention.
* Re-joining when a topic is created after `subscribe()`: it needs periodic metadata refresh and a rebalance trigger. Left out; a subscription to a missing topic gives an empty assignment (documented limitation).

## What goes wrong if you get it wrong
* Advancing the position before the user has the records: a crash loses them.
* Committing positions at `close()` after a rebalance has taken the partitions: the commit is rejected (generation fencing), which is exactly the intended safety.
* Clearing the rebalance flag after the join instead of before it: a signal that arrives during the join is lost and the consumer runs on a stale chart.

## Tradeoffs and honest limits
* No cooperative rebalancing: on every rebalance the member stops everything and starts again from the committed offsets, so up to one commit interval of work is repeated.
* A timed-out JOIN is ambiguous (see step 3): the client retries, and the abandoned member id disappears after one session timeout.
* The consumer is not thread-safe except for the heartbeat thread's own use; call `poll`, `commitSync` and `close` from one thread.

## How it is tested
`EmberGroupConsumerTest` (real brokers): one consumer reads all 120 records of a 6-partition topic and commits; a second consumer in the same group sees only the new records; `close()` commits and leaves; a missing topic gives an empty assignment. The rebalance, crash, zombie, failover and restart scenarios are step 7.
