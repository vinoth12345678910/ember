# Honest limits

What Ember does not do, what the numbers do not prove, and where the demos cheat. Read this before you trust a claim in the README.

## Delivery semantics

* **At-least-once, not exactly-once**, for topics, consumer groups and ack queues alike. A worker that crashes after doing the work and before acknowledging gets the message again. Ember gives you the **attempt number** (it only grows) and fences stale settles, so a handler can be made idempotent; it cannot make a side effect happen once. The Fleet's billing is safe only because the (mock) bank accepts an idempotency key.
* No idempotent producer: a produce retried after a timeout can write the record twice.
* The Node SDK's `order: 'key'` keeps one key's messages in order inside one worker only, and `latestWins` remembers state per worker in memory; neither is a cluster-wide guarantee.
* Ordering at the broker is per partition. An ack queue deliberately gives up per-partition ordering of *processing* (that is how 24 workers can share 6 partitions); a latest-wins consumer or key-affine worker is the application's job.

## Cluster

* Replication factor = cluster size. Brokers cannot be added or removed; the partition count of a topic is fixed; topics cannot be deleted.
* **No Raft snapshots.** Raft logs never shrink and a restart replays them. This includes `__groups` and `__acks`, so a long-lived busy queue grows without bound.
* Every record is written twice (Raft log + data log). One coordinator (`__groups`) for all consumer groups and one leader (`__acks-0`) for all ack queues: that leader is a throughput ceiling and a single place a failover pauses.
* Reads of the data log on the `__acks` leader are local, so a claim right after a produce may return fewer messages than exist; callers must be prepared to claim again.
* No TLS, no authentication on the broker. The **console** authenticates people and apps and enforces ACLs, but anybody who can reach a broker port can do anything. Peers are trusted.

## Ack queues

* A dead letter keeps holding the partition's progress marker until an operator retries or discards it. The record itself stays in the partition log; if retention removes it first, the dead letter loses its payload.
* Retry policy (attempts, backoff, jitter) is per subscription and fixed at creation.
* Lease expiry is checked every 200 ms on the leader: redelivery after a crash is not faster than that, and an extended lease is only as precise as your clock-free deadline logic (the leader writes its own wall-clock deadlines into commands, so a leader with a badly wrong clock would shorten or lengthen leases).
* Chaos coverage is real but finite: three 20 s seeds in CI and longer ones on demand. Zero violations in those runs is evidence, not proof; the checker itself is validated by mutation tests that plant bugs.

## Consumer groups

* Eager rebalancing is the default and it hurts: all members stop and repeat uncommitted work. `--group-rebalance=cooperative` fixes the repeated work for the partitions that stay and for the ones that move, at the cost of a moved partition being unowned for about one heartbeat interval; it is opt-in and young (details in [`phase6/cooperative-rebalance.md`](phase6/cooperative-rebalance.md)).
* Untuned consumers that process hundreds of records between polls get expired after a rebalance and can trigger a cascade; the comparison benchmark had to tune `maxPollBytes` for this reason.

## The Fleet demo and its numbers

* The bank, the GPS pings and the riders are **simulated**. Nothing is a real payment or a real map. The services are small on purpose.
* The headline numbers come from **one cloud VM** with three brokers on the same host (so no real network and a shared CPU), a single run each. They show a *shape* (stuck, lossy, complete, faster with more workers), not a capacity plan.
* The comparison measures the same poisoned workload through a consumer group and an ack queue. The group consumers are tuned like an operator would (about 10 records per poll). It does **not** measure the fifth option: hand-built retry and dead-letter topics on top of a consumer group, which works but is application code.
* The demo's own port (`8099`) has no login and listens on localhost; only the console proxies it, for administrators.

## Tooling

* The Node SDK is the only non-Java client. It speaks the console's HTTP API, not the broker protocol, so it inherits the gateway's limits (long-poll claims, `Retry-After` backpressure) and adds a hop.
* Local demo processes (brokers, console, `live.js`) do not restart themselves; a crashed one stays down.
