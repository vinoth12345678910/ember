# Step 2 - Instrumentation, the admin HTTP server, readiness and /status

## What we are building
* **Metrics** for the broker, every Raft group, the peer links, every partition's data log, the consumer groups, and the JVM, all named `ember_*` (the list is in `design.md`, part 1).
* A small **admin HTTP server** (the JDK's own `com.sun.net.httpserver`, no new dependency): `GET /metrics`, `GET /health/live`, `GET /health/ready`, `GET /status`, and a debug endpoint `GET /debug/partition-checksum` that exists only with `--enable-debug-endpoints`.
* A hand-written **JSON writer** for `/status`.
* **Structured log prefixes**: every Raft or replication log line starts with the node id and the group.

## Why it is designed this way
**Dashboard, not autopsy.** The chaos tests of Phases 5 and 6 told us *that* the cluster survived. An operator needs to see *while it runs* who is leader, whether a replica is behind, why produce is slow. So each question has a metric (see `design.md` part 1: "is the cluster healthy?", "why is produce slow?", "who is leader?", "are the replicas behind?", "is a consumer group stuck?").

**ember-raft stays pure.** The Raft core does not import the metrics library. It already reports role changes and commits through `RaftListener`; Phase 7 adds one more default method, `onCheckQuorumStepDown`, so a leader's "I stepped down because I lost my majority" can be counted. The *broker* turns listener calls and read-only getters into counters and gauges.

**Gauges are read at scrape time.** `raft_term`, `raft_commit_index`, `log_end_offset`... are suppliers over getters that read volatile fields the Raft group already publishes; they take no locks, so a scrape during an election never blocks.

**Produce latency means what the client feels.** `produce_latency_seconds` runs from the moment the request arrived to the moment the response was written, so it includes the Raft commit and apply. `raft_apply_latency_seconds` is the part inside: propose to applied on the leader. If the first is high and the second is low, look at the network or the handler threads; if both are high, look at `raft_fsync_seconds`.

**Readiness is more than "the process runs".** `/health/live` answers 200 as soon as the server is up (restarting a live process does not help anybody). `/health/ready` answers 200 only when: the broker has joined the cluster; the META group and the `__groups` group have a known leader; and every local Raft group has **applied everything it knows is committed** (`lastApplied >= commitIndex`). A restarted follower learns the commit index from its first heartbeat, so while it replays its log it is *not ready*. Otherwise a load balancer could send clients to a broker that is still rebuilding its data logs. The 503 body is JSON with the reason.

**Security stance.** The admin port is unauthenticated plain HTTP. It binds to `127.0.0.1` by default (`--admin-host`), prints a warning at start-up, never exposes keys or values (only metadata and counters), and the debug endpoint is off by default.

## Alternatives I rejected and why
* Micrometer / Prometheus client libraries: see step 1.
* Counting inside `ember-raft`: it would break the "pure, no dependencies" rule that made Phase 4's simulator possible.
* Using a Netty HTTP handler for admin traffic: it would share event loops with the data path; the JDK server has its own small thread pool, so a stuck scraper cannot slow produce.
* Pushing per-group-member and per-record labels: unbounded label sets (see the cardinality guard). `group_lag` is per (group, topic, partition) and documented with a warning.

## What goes wrong if you get it wrong
A readiness check that only tests "port open" gives a false green during catch-up. A gauge that takes a lock held by Raft makes `/metrics` hang during the exact incident you are debugging. A histogram that starts its clock after the handler finished hides queueing. An admin port bound to `0.0.0.0` on a laptop exposes the broker's internals to the coffee shop.

## Tradeoffs and honest limits
* `group_rebalances_total` and the heartbeat times live in the coordinator's memory, so after a coordinator failover the counter restarts from zero for the new coordinator (Prometheus treats that as a counter reset).
* `group_lag` is computed on the coordinator leader from its *local* data log end, which can trail the partition leader's by a moment.
* Readiness cannot tell a replica that is truly caught up from one that has not yet heard the leader's commit index; it is not ready until it has heard it, which is the safe side.
* The `/status` JSON is a snapshot assembled from several volatile reads, not an atomic cut.

## How it is tested
Admin tests against real in-process brokers (`TestCluster`): `/metrics` has the expected names and the counters INCREASE by the expected amounts after a produce; `/health/live`; `/health/ready` flips 503 -> 200 once the cluster is up and again after a restart; `/status` parses (with a test JSON parser) and agrees with METADATA v2; `--admin-port=0` opens no port; the checksum endpoint is absent unless enabled and equal across replicas. Raft gauges: kill the leader and the new leader exports `raft_role 2` and `leader_changes` increased; `peer_connected` goes 0 when a link is blocked and back to 1 after healing. A JSON writer test checks escaping.
