# Ack queues: per-message acknowledgement on Ember (design, branch `ack-queue-core`)

Status: **implemented in branch `ack-queue-core`** with the recommended choices for D1-D5 (the user approved them). Section 11 records what was built and measured. Backoff, max attempts and the dead-letter queue, per-key ordering and the HTTP/SDK layer are later branches.

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

## 11. What was built and measured

- **D1** one `__acks` Raft group (`AckStateMachine`, `AckCommands`), **D2** claim returns the records inline (read from the leader's local copy of the partition), **D3** no snapshots yet, **D4** default lease 30 s (per-subscription `--lease-ms`, per-claim override), **D5** subscriptions are created explicitly (`queue-create`).
- Wire: API keys 20-26 (`QUEUE_FIND`, `QUEUE_CREATE`, `QUEUE_CLAIM`, `QUEUE_ACK`, `QUEUE_RELEASE`, `QUEUE_EXTEND`, `QUEUE_DESCRIBE`), error codes 21-24. Client: `QueueConsumer`. CLI: `queue-create`, `queue-describe`, `queue-bench`. `/status` lists `__acks-0` and every subscription's per-partition progress.
- Behaviour worth knowing:
  * A claim serves only what the `__acks` leader has already applied from the partition, so right after a produce it may return fewer messages than exist. Callers poll.
  * Leases use wall-clock milliseconds written into the commands. Expiry runs on the leader every 200 ms.
  * A message that retention deleted before delivery is finished on the claimer's behalf instead of waiting for ever.
  * Claims for one subscription are planned one at a time (serialised) on the leader; the command already carries a list of allotments, so coalescing several workers' claims into one Raft entry is a later optimisation that needs no protocol change.
- Measured on a cloud VM, 3 brokers, fsync on, everything on one disk (one run each, treat as one setup):
  * 8 workers, 6 partitions, claim up to 100, no simulated work: **12,371 messages/s** claimed and acked (60,000 messages, 0 redelivered, 0 rejected, claim p50 15 ms / p99 121 ms, ack p50 3 ms).
  * 100 ms of simulated work per batch of 10, same 6 partitions: **529 messages/s with 6 workers** (the most a consumer group can use) versus **1,788 messages/s with 24 workers** (3.4x).
- Tests: 13 state-machine, 9 coordinator (fake Raft log), 4 codec, and 3 real 3-broker integration tests (6 workers on 3 partitions get every message exactly once; a worker that never acks loses its messages and is fenced; killing the `__acks` leader loses no ack and brings no acked message back). Replaying a random 300-step command log on two replicas gives identical state.

## 12. Retries, backoff and the dead-letter set (branch `retry-dead-letter`)

**Two counters per message.** The *attempt* number identifies one delivery and only grows (it is what fences a stale ack). The *failure* count says how many deliveries failed. A **nack** and a **lease that ran out** are failures; a **release** is not (a worker shutting down must not burn a message's attempts).

**Policy per subscription:** `maxAttempts` (default 5), `backoffBaseMs` (1 s), `backoffMaxMs` (60 s), chosen at `queue-create`. After a nack the message waits `base * 2^(failures-1)` capped at the maximum, plus up to 25% jitter. The jitter is derived from the offset, not from a random generator, so every replica computes the same value (the state machine stays a pure function; the time comes from the `now` the leader wrote into the NACK command). A lease that ran out is redelivered at once (it already waited a whole lease) but counts as a failure, so a message that crashes or hangs every worker is dead-lettered too, after `maxAttempts` leases.

**Dead letters live in the same Raft state, not in a second topic.** When failures reach `maxAttempts` the message moves from the retry set to a per-partition `dead` map (attempt, failures, time, reason). No cross-group write, so there is no window where a message is both dead and still queued, or neither. The record content stays in the partition log; the dead-letter list reads it from there. Consequences, stated plainly:
- a dead message is **not finished**: it holds the partition's `floor` (the "everything below is done" marker) until an admin retries or discards it; later messages keep flowing and are tracked above the floor;
- if retention deletes the record before an admin looks, the entry stays but its content shows as gone (a copy into a separate dead-letter topic would avoid that, at the price of a non-atomic move; not done here).

**Admin operations** (any broker can be asked; the `__acks` leader serves them): `QUEUE_DEAD_LIST`, `QUEUE_DEAD_RETRY` (back into the queue due at once, failure count reset, attempt numbers keep growing so old acks stay fenced) and `QUEUE_DEAD_DISCARD` (finished for good). The console has a Queues page with the policy, live counts and a dead-letter table with Retry / Discard (one or all shown).

**Wire:** API keys 27-30 (`QUEUE_NACK`, `QUEUE_DEAD_LIST`, `QUEUE_DEAD_RETRY`, `QUEUE_DEAD_DISCARD`); `QUEUE_CREATE` carries the policy and `QUEUE_DESCRIBE` returns it plus a dead count per partition. A `CREATE` entry written by the previous version (no policy fields) is still read, with the defaults.

**Measured** (cloud VM, 3 brokers, fsync on, 8 workers, claim 50, 12,000 messages on 6 partitions, every 100th message always nacked, policy 3 attempts / 200-1000 ms): all **11,878 healthy messages were acked (2,916 messages/s end to end, including the tail while the poison messages finished retrying)**, and **122 poison messages were dead-lettered after exactly 3 failed attempts each**. Nothing waited behind them: `doneAbove` shows the healthy ones finished on both sides of every poison offset.

**Tests:** 8 new state-machine tests (backoff growth and cap, max attempts, release is not a failure, retry resets the budget, discard moves the floor, fencing, a random 400-step log replays identically on two replicas, old-format CREATE), 3 coordinator tests (backoff-aware claiming, dead list with content, retry/discard), codec tests, and 2 real 3-broker tests (poison messages retried 3 times then dead-lettered while 98 others finish, dead set survives killing the `__acks` leader, retry after "the fix" delivers them; discard moves the floor).

## 13. HTTP API and the Node SDK (branch `app-sdk`)

Apps reach ack queues through the console, not the brokers (the brokers have no authentication). `/api/app/queues/{queue}/{claim,ack,nack,release,extend}` and `GET /api/app/queues/{queue}`, authenticated by the app's API key; the queue is allowed when the app may consume the queue's topic.

- **Identity.** The worker name an app sends is stored as `<appId>:<name>`; the broker's fencing (a stale or foreign handle is rejected) therefore also separates apps from each other. Verified by test: a second app with read access to the same topic, using the same worker name, has its ack rejected.
- **Connections.** Queue calls use a pool of 16 broker connections borrowed per call (the shared client of the other endpoints is synchronised and would serialise all workers). A long-polling claim sleeps without holding a connection, so thousands of idle workers cost virtual threads, not connections.
- **Two performance findings from measuring it** (both also help every other console endpoint): the JDK HTTP server needs `sun.net.httpserver.nodelay=true` (headers and body are written separately, and Nagle plus the client's delayed ACK added about 40 ms to every response: ack 50 ms to 11 ms), and the SDK must not claim every time one slot frees (each claim is a Raft commit): it claims again once a quarter of its slots are free.
- **SDK** (`sdk/node`): `Ember.publish / publishBatch / worker / claim / ack / nack / release / extend / queue`; worker semantics: resolve = ack, throw = nack with the message as reason, `Ember.RELEASE` = give back without a failure; batched acks; automatic lease extension; `stop()` drains and releases.
- **Tests:** 6 console tests against a real in-process broker (claim/ack round trip, identity isolation and ACL, nack to dead, extend and release, long poll timing, bad input), 11 SDK tests against an in-process fake server (run in CI), plus a live crash test (SIGKILL of one of three worker processes: no message lost).
