# Ack queues: per-message acknowledgement on Ember (design, branch `ack-queue-core`)

Status: **proposal, no code yet.** Decisions marked **[D1]..[D5]** need a yes/no before implementation.

## 1. The problem

A Kafka-style consumer group tracks one number per partition: "read up to offset N". That has three consequences that hurt real-time systems:

1. **A poison message blocks everything behind it.** Skip it and it is lost; retry it and the partition stalls.
2. **Parallelism is capped by the partition count.** Ten workers on three partitions: seven idle.
3. **Any slow message delays all later ones in its partition**, even unrelated ones.

An *ack queue* tracks each message separately: a worker **claims** messages, **acks** the ones it finished, and anything not acked in time is **redelivered** to someone else.

## 2. Goals and non-goals

Goals (this branch): claim / ack / extend / expire-and-redeliver, replicated through Raft so a broker loss loses no ack state; many workers per partition; at-least-once delivery; the existing produce/fetch/consumer-group paths are untouched.

Not in this branch (later branches): backoff, max attempts and the dead-letter queue (`retry-dead-letter`); per-key ordering with parallel workers (`parallel-per-key-workers`); client SDK and HTTP API (`app-sdk`). Not goals ever: exactly-once, changing how records are stored.

## 3. Where the state lives

Records never move: they stay in the partition's data log. The queue only stores *progress*, keyed by `(subscription, topic, partition)`. A **subscription** is a named reader of a topic (for example `matcher` reading `ride-requests`); it is independent of consumer groups, and one topic can have many subscriptions, each with its own progress.

**[D1] Proposal: one new Raft group `__acks`**, next to `__meta` and `__groups`, led by one broker, replicated on all (same pattern as the group coordinator).
- Why not the partition's own Raft group: `PartitionStateMachine` assumes every entry is a produce batch and offsets equal a running record counter. Mixing ack commands in risks the data path, which is the part chaos-verified for 100k+ writes.
- Cost: one leader handles all ack traffic. Acks are batched (one Raft entry carries many offsets), so the ceiling is roughly (entries per second the group can commit) x (offsets per entry): hundreds of thousands of acks/s on paper, bounded in practice by fsync (README: ~30-100 commits/s on a shared disk). State is keyed per partition so splitting into `__acks-0..N` later is a routing change, not a redesign.

## 4. State per `(subscription, topic, partition)`

| Field | Meaning |
|---|---|
| `floor` | every offset below it is finished |
| `done` | finished offsets at or above `floor` (out-of-order acks), kept as ranges; `floor` advances over contiguous finished offsets |
| `next` | first offset never handed out |
| `leases` | offset -> (worker, attempt, deadline) for messages currently being processed |
| `redeliver` | offsets whose lease expired or were released, waiting to be handed out again, with their attempt count |

Memory is bounded by messages in flight plus gaps, not by history.

## 5. Commands (all go through Raft; `apply()` stays pure)

The leader decides everything non-deterministic (which offsets, the deadline timestamp, the worker id) *before* proposing, exactly like `SET_MEMBERSHIP` today.

| Command | Effect |
|---|---|
| `CLAIM(sub, topic, partition, worker, offsets[], attempts[], deadline)` | records leases. Leader picks offsets: due `redeliver` first, then from `next`. |
| `ACK(sub, topic, partition, worker, (offset, attempt)[])` | finishes an offset **only if** the lease is still held by that worker with that attempt. A late ack from a worker whose lease expired and was reassigned is rejected (fencing, same idea as consumer-group generations). |
| `RELEASE(..., offsets[])` | worker gives messages back (shutdown, or a failure it wants retried). Goes to `redeliver`, attempt+1. |
| `EXTEND(..., offsets[], deadline)` | slow but alive worker keeps its lease. |
| `EXPIRE(..., offsets[], now)` | leader's timer proposes this for leases past their deadline; `apply` checks `deadline <= now` using the `now` carried in the command, so replicas agree. |

Clock safety: time only decides *when* to redeliver. A leader with a fast clock redelivers early (a duplicate, which at-least-once allows); one with a slow clock redelivers late. No safety property depends on clocks being right.

## 6. Delivery path

**[D2] Proposal: CLAIM returns the records inline.** Every broker holds a full copy of every partition (replication factor = cluster size), so the `__acks` leader reads the claimed offsets from its *local* data log and returns them in the CLAIM response: one round trip instead of claim-then-fetch. It only hands out offsets below its local log end (the committed, applied prefix), so it never serves a record the cluster has not committed.

Wire: new API keys `QUEUE_CLAIM`, `QUEUE_ACK`, `QUEUE_RELEASE`, `QUEUE_EXTEND`, `QUEUE_DESCRIBE`, routed to the `__acks` leader like `FIND_COORDINATOR` routes to the group coordinator (`NOT_COORDINATOR`-style redirect, same client retry path).

## 7. Failure behaviour

| Event | Result |
|---|---|
| worker crashes mid-message | lease expires, message goes to another worker (attempt+1) |
| worker acks after its lease was reassigned | rejected; the new holder's ack counts |
| `__acks` leader dies | new leader replays/continues from the committed state; leases stay valid until their deadline, then expire normally. In-flight claims whose Raft entry was not committed are simply never handed out |
| network cut, duplicate ACK | idempotent: acking a finished offset is a no-op success |
| acked message | never handed out again |

## 8. Known risks (stated up front)

- **No Raft snapshots exist yet**, so `__acks` grows with every claim/ack entry and restart replays all of it. Batching limits the entries per second but not the total. Fine for demos and tests; real use needs the snapshot/compaction work already listed in the README. **[D3] Proposal: accept this for the demo, document it, schedule `raft-snapshots` after the demo branches.**
- One `__acks` leader is a throughput and availability hotspot (failover is ~0.8 s median for the coordinator today, during which claims/acks stall).
- Delivery is at-least-once; consumers must be idempotent.

## 9. Verification plan

1. Pure state-machine unit tests (every command, stale/duplicate/out-of-order cases, floor advancement, expiry boundary).
2. Raft-level tests through `LocalTransport` (leader change mid-claim, replay equals live state).
3. In-process 3-broker integration tests (worker crash, late ack, many workers on one partition get disjoint messages, nothing lost).
4. Branch `ack-chaos-checks`: add checker properties - an acked offset is never claimed again; every produced offset ends acked or leased; no offset stays unclaimed forever once workers exist; no ack accepted from a fenced attempt - and run them under the existing nemesis (kills, freezes, partitions).
5. Mutation tests for the new checker properties, as for the existing nine.

## 10. Decisions needed

- **[D1]** single `__acks` Raft group (recommended) vs ack state inside each partition's group.
- **[D2]** CLAIM returns records inline (recommended) vs claim offsets then fetch.
- **[D3]** accept unbounded `__acks` log for now, snapshots later (recommended).
- **[D4]** default lease 30 s, extendable, per-subscription override. OK?
- **[D5]** subscription is created implicitly on first claim (recommended, like auto-created topics off by default here so: *explicit create by an admin or a registered app with consume permission*). Which one?
