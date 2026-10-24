# Cooperative rebalancing (sticky assignor + two-phase handoff)

Off by default. Turn it on per cluster with `--group-rebalance=cooperative` on every broker, and per consumer with `GroupConsumerConfig.withIncrementalRejoin(true)`. `GET /status` shows the broker's mode as `groupRebalance`.

## The problem it solves

In the default (eager) mode a rebalance throws everything away. Every member rejoins, forgets the position of **every** partition (even the ones it keeps) and restarts from the last *committed* offset. Whatever it had processed since its last commit is processed again, on all partitions, and consumption stops on all of them while it happens. With a 5 s commit interval and a busy topic that is up to 5 s of repeated work for each member, at each join, leave or crash. The range assignor makes it worse: adding one member shifts the ranges of many members, so many partitions change owner for no reason.

## What changes

1. **Sticky assignor** (`Assignor.assignSticky`). Same balance as the range assignor (every member of a topic gets floor(P/m) or ceil(P/m) partitions), but a partition stays with its current owner whenever the balance allows it. Going from 2 to 3 members over 12 partitions moves exactly 4 partitions (the newcomer's share); the range assignor moves more. Deterministic, a fixed point (assigning again with the result as history changes nothing), randomized-churn tested for "one owner per partition, balanced within one".
2. **Two-phase handoff** (`GroupCoordinator`). A partition whose *current owner is still a live member* but is no longer the target owner is first **revoked**: the next generation gives it to nobody. The new owner receives it in a later generation, installed by the coordinator's tick **only after every member has heartbeated the current generation** (a heartbeat at generation N is the member's acknowledgement that it processed N's revocations). A partition whose owner has left or expired is given straight to its new owner: nobody can still be using it.
   The acknowledgements are soft state on the leader and are forgotten at a leader change; after a failover the members simply acknowledge again within a heartbeat interval. The handoff state needs no other bookkeeping: it is re-derived from the durable assignment (a partition owned by nobody whose target owner exists).
3. **Last-words commit.** While a partition is unowned its previous owner may still commit its final position. That is a separate state-machine command (`OFFSET_COMMIT_REVOKED`, type 3): same payload and same fencing (member must exist, generation must be current), but it may also store partitions that **no** member owns. A partition owned by *another* member is still refused, and the ordinary `OFFSET_COMMIT` stays strict, so eager mode is unchanged.
4. **Incremental rejoin in the client.** On a rebalance the consumer keeps consuming the partitions it still owns (same position, no re-read of committed offsets), commits the positions of the partitions taken away, and reads committed offsets only for the new ones. It adopts the new generation **last**, so the heartbeat thread acknowledges it only after the revoked partitions were committed. A member that was expired (UNKNOWN_MEMBER_ID) starts over from scratch.

## Why it is safe

* One owner per partition at all times: a partition changes owner only through an *unowned* generation, and the new owner is installed only after the old owner acknowledged (or is gone). The state machine still rejects any SET_MEMBERSHIP with a partition owned twice.
* A slow member delays a handoff (its partitions stay unowned until it acknowledges or its session expires) but never causes two owners. The price is availability of the *moved* partitions, bounded by the heartbeat interval in the normal case and the session timeout in the worst case.
* Delivery is still at-least-once. A crash loses the in-memory positions exactly as before.
* Mixing modes is safe: an incremental client against an eager broker just sees assignments change in one step (it keeps positions of partitions it keeps, and the new owner of a moved partition may repeat work, as before); an eager client against a cooperative broker rejoins from the committed offsets as before.

## Measured

`CooperativeRebalanceTest`: 3 brokers, 6 partitions, 2 consumers, 240 records processed, auto-commit effectively off so the only offsets the group ever stores come from the rebalance itself, then a third consumer joins and 240 more records are produced.

| mode | records delivered twice after one join |
|---|---|
| eager (default) | 240 (everything processed since the last commit, on every partition) |
| cooperative | 0 |

The test is deliberately extreme about the commit interval (that is what isolates the effect); with a short commit interval the eager number is smaller, bounded by what was processed since the last commit. The cooperative cost is a delay for the *moved* partitions: they are unowned until the old owner acknowledges, typically one heartbeat interval.

Chaos: `ChaosRunTest.cooperativeRebalanceSeedOne/Two` run the full nemesis (crashes, pauses, isolation, link cuts, slow fsync, drops, delays, consumer pauses) with cooperative brokers and incremental consumers; the history checker (P7 every acknowledged write is delivered to the group, P8 no zombie commit, P9 replicas identical) finds no violation. From the command line: `ember-chaos ... --rebalance=cooperative`.

## Limits

* A member that does not call `poll()` does not acknowledge, so a handoff waits for it until its `maxPollIntervalMs` or session expires.
* The assignor balances each topic independently (like the range assignor); it does not balance the total across topics.
* Not implemented: static membership, rack awareness, or letting the application choose the assignor.
