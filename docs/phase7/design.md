# Phase 7 design: observability, verification and operations

Phase 5 made Ember replicate; Phase 6 made it share work among consumers. Phase 7 answers three questions about the running system: *what is it doing right now* (metrics, health, status), *can I trust what it told its clients* (the chaos checker), and *can I run it somewhere other than my IDE* (Docker, soak, benchmarks).

## 1. The metrics map: which metric answers which question

Metrics are exposed at `GET /metrics` of the admin port (Prometheus text). `node` is added by Prometheus from the scrape target. Names below are shortened: every one starts with `ember_`.

| Question | Metrics | Example query (PromQL) |
|---|---|---|
| **Is the cluster healthy?** | `/health/ready` (HTTP), `raft_role`, `peer_connected`, `raft_checkquorum_stepdowns_total` | `count(raft_role{group="__meta-0"} == 2) == 1` (exactly one controller). `sum(peer_connected) == 6` (every ordered pair of three brokers is connected) |
| **Who is leader?** | `raft_role` (0 follower, 1 candidate, 2 leader) per Raft group | `raft_role{group="orders-0"} == 2` returns the node that leads partition 0. The leader of `__meta-0` is the controller; of `__groups-0` the consumer-group coordinator |
| **Is leadership stable?** | `raft_leader_changes_total`, `raft_elections_started_total`, `raft_term` | `increase(raft_leader_changes_total[10m]) > 3` means flapping: look at `peer_messages_dropped_total` and `raft_fsync_seconds` |
| **Why is produce slow?** | `produce_latency_seconds`, `raft_apply_latency_seconds`, `raft_fsync_seconds`, `raft_pending_proposals`, `peer_queue_depth` | `histogram_quantile(0.99, sum by (le) (rate(produce_latency_seconds_bucket[1m])))` is what clients feel. If `raft_apply_latency` p99 is about the same, the time is inside Raft; compare with the fsync p99: if that is the floor, the disk is the limit. A growing `raft_pending_proposals` means requests arrive faster than commits |
| **Are the replicas behind?** | `log_end_offset` per partition and node, `raft_commit_index`, `raft_last_applied`, `raft_last_index` | `max by (topic,partition) (log_end_offset) - min by (topic,partition) (log_end_offset)` is the spread between replicas; `raft_commit_index - raft_last_applied` is a replica applying a backlog |
| **Is the network the problem?** | `peer_queue_depth`, `peer_messages_dropped_total`, `peer_reconnects_total`, `peer_connected` | `rate(peer_messages_dropped_total[1m]) > 0` for a peer: messages are being lost between those two brokers |
| **Is a consumer group stuck?** | `group_lag`, `group_members`, `group_generation`, `group_rebalances_total`, `group_offset_commits_total{result}` | `group_lag` that only grows: the group is too slow or stuck. A `group_generation` that keeps increasing: rebalancing too often (sessions too short, consumers crashing). `rate(group_offset_commits_total{result!="ok"}[5m]) > 0`: commits are being refused (fencing) |
| **Is the disk filling?** | `log_size_bytes`, `retention_deleted_segments_total`, `log_segments` | `sum by (node) (log_size_bytes)`. Raft logs are not included (they never shrink: no snapshots) |
| **Is the JVM healthy?** | `jvm_heap_used_bytes`, `jvm_heap_after_gc_bytes`, `jvm_threads`, `process_open_fds`, `jvm_direct_buffer_used_bytes`, `jvm_gc_collection_seconds_total` | `jvm_heap_after_gc_bytes` is the live set (heap "used" is a sawtooth); it should be flat over hours. Threads and fds that only climb are leaks |

`group_lag` has one series per (group, topic, partition): it is the one high-cardinality metric. The registry caps all metrics at 10,000 series and logs once if that is exceeded.

## 2. What the chaos checker proves, and what it cannot

The checker reads a *recorded history* of client operations and reports violations of nine properties (P1-P9, step 4). If it reports none, you know: **in that run**, no acknowledged write was lost, no two reads disagreed about an offset, no value appeared that nobody sent, offsets were unique, per-key order held, reads never went backwards, every acknowledged record reached the group, no stale-generation commit was accepted, and all replicas ended with identical data, Raft logs (committed prefix), group state and topic state.

What it **cannot** prove:

* **It is a test, not a proof.** It sees one execution. A bug that needs a rare interleaving can survive many green runs. (Phase 4's simulator covers the Raft core's interleavings systematically; this covers the whole system but only by sampling.)
* **The seed reproduces the fault *schedule*, not the timing.** Same seed, same faults in the same order; but threads, the OS scheduler and the network make the exact interleaving different each time. A failure is therefore re-*checked* from the saved history file, not re-*run*.
* **It only sees what clients observed.** An internal inconsistency that no client read (and no replica comparison exposed) stays invisible. P9 closes much of that gap for the data logs, Raft logs and replicated state, but not for volatile state (e.g. the coordinator's heartbeat times).
* **Unknown outcomes are never wrong.** An INFO produce (a timeout) may or may not exist. Good: it avoids false alarms. Bad: a bug that turns *acknowledged* writes into *unknown* ones would hide there; the run reports how many unknown outcomes occurred so that is visible.
* **Single-node faults.** At most one broker is unavailable at a time (a majority always exists). Majority loss has its own test, but two simultaneous independent faults on different brokers (a slow disk on one while another is isolated) are not generated.
* **No real hardware faults.** Crashes keep the OS page cache in in-process runs; the Docker and process runs use real `kill -9` but the disk stays intact. Power loss, corrupted sectors and clock jumps are covered only by file-level fuzz tests (Phase 5) and the Phase 4 simulator.
* **What Jepsen-style testing also misses:** performance under faults (we only require progress), security, upgrades from an older version, very long histories (the soak is minutes, not weeks), and anything the model of "operations on a log" does not express.

## 3. The checker's own mutation tests

A checker that passes everything is worthless, so the checker is attacked the way Phase 4 attacked Raft. `CheckerMutationTest` builds a realistic, known-good history (2 producers, 3 partitions, keyed records, unknown and refused outcomes, readers, two group consumers with commits, three identical replicas) and injects exactly **one** bug per test:

| Injected bug | Required code |
|---|---|
| drop an acknowledged record from the final read | `P1_LOST_ACK` |
| two different values for one offset | `P2_DIVERGENCE` |
| acknowledge the same offset twice | `P4_DUPLICATE_OFFSET` |
| invent a value | `P3_PHANTOM` |
| a failed write that became visible | `P3_FAILED_WRITE_VISIBLE` |
| swap two keyed records | `P5_KEY_ORDER` |
| a read that shrinks | `P6_SHRINK` |
| an acknowledged record that never reached the group | `P7_NOT_DELIVERED` |
| a commit beyond the log end | `P8_COMMIT_BEYOND_END` |
| accept a commit with a stale generation | `P8_ZOMBIE_COMMIT` |
| one replica data log differs by one byte | `P9_DATA_DIVERGENCE` |
| a divergent Raft log / group state | `P9_RAFT_DIVERGENCE`, `P9_GROUP_STATE_DIVERGENCE` |

All are caught with the expected code, and the untouched history reports nothing. The tests also pin the *leniency* the checker must have: a refused commit is fine, a retention-trimmed record below the log start is fine, an unknown-outcome produce may or may not exist, duplicates in group delivery are counted but allowed.

**What the real runs taught about the checker's inputs** (more valuable than any mutation): the first alarm was not an Ember bug but a wrong *history* (`commitSync()` returned normally without sending anything, and the workload recorded an acknowledgement that never happened). The checker is only as good as the history; the recorder must write what the system really said.

## 4. Coordinated omission, explained simply

Say you test a shop by sending one customer at a time: the next customer walks in only when the previous one has left. The cashier freezes for 10 minutes. Your log says: *one* customer waited 10 minutes; all the others were served in 1 minute. Average: great.

In real life customers keep arriving every minute. During those 10 minutes, ten more arrive and wait in the queue; they would have waited 10, 9, 8, ... minutes. Your test never sent them, because it was waiting for the frozen customer. It "coordinated" with the stall and *omitted* the customers who would have suffered.

`bench --rate=N` fixes the arrival schedule in advance and measures every request from the time it *should* have been sent. In `OpenLoopScheduleTest` a 60 ms stall at a 10 ms interval makes 6 requests slow (60, 51, 42, 33, 24, 15 ms and then 6 ms before it catches up); the same stall in a closed loop makes exactly 1 request slow. The closed-loop mode is still the default because it measures capacity ("how fast can it go"); the fixed-rate mode measures experience ("what does a user feel at 50% load").

## 5. The advertised-address problem

A broker is reachable under *different names depending on who asks*. Inside the Docker network the others call it `ember2:9092`. From your laptop it is `localhost:19093` (a host-mapped port). METADATA and FIND_COORDINATOR tell clients where to connect, so whatever address a broker reports must be valid *for the client*. If it reports `ember2:9092` to a laptop, the first connection (the bootstrap) works, then the client is redirected to a name it cannot resolve and every produce hangs: "bootstrap works, nothing else does".

Ember: `--cluster=1@ember1:9092/localhost:19092,...` gives each broker an internal address (used between brokers for Raft) and an **advertised** address (used in METADATA v0/v1/v2 and FIND_COORDINATOR). It supports **one** advertised address per broker. Kafka supports several *listeners*, so a client inside the Docker network and a client on the host can each be told the right name. Without multiple listeners, the compose demo requires clients on the host. `AdvertisedAddressTest` proves the separation by making the advertised addresses unreachable and checking that the cluster still replicates over the internal ones.

## 6. Docker design choices

* **`exec java`** in the entrypoint so SIGTERM reaches the JVM (otherwise `docker stop` is a `kill -9` after 10 s).
* **JVM flags for containers:** `MaxRAMPercentage=60` (the heap follows the memory limit, the rest is for direct buffers, stacks, metaspace and the page cache), `MaxDirectMemorySize=256m`, `ExitOnOutOfMemoryError`.
* **Non-root user**, data in `VOLUME /data`.
* **Healthcheck = our own CLI** (`ember healthcheck`): the image has no `curl`, and the check uses the same `/health/ready` logic as everything else.
* **`restart: "no"`:** the chaos tool owns broker lifecycle.
* **Two addresses per broker** (container names between brokers, host ports for clients) as in part 5.
* **Admin port:** unauthenticated; listens on `0.0.0.0` only inside the container; the compose file publishes it, so keep the host on a trusted network.
* **Observability is a profile** (`--profile observability`), so the default `docker compose up` starts exactly three brokers.

## 7. Bugs the new tools found (real)

1. **Readiness lied.** A restarted broker answered `/health/ready` 200 while it had applied nothing, because Raft records the leader *before* it checks that its log matches, so "applied 0 == committed 0" looked caught up. Rule added: a group that has not learned a commit index of at least 1 is not ready.
2. **A group consumer could skip records.** If a later partition's fetch failed after an earlier one succeeded, `poll()` threw and dropped the earlier records while their positions stayed advanced. Fixed; regression test with a fake fetcher.
3. **A consumer could be left with an empty assignment for ever** when it subscribed right after its topic was created and the coordinator's copy of the topic list lagged. The coordinator now re-derives assignments on its tick.
4. **`commitSync()` could return normally without sending anything**, which made the checker's first alarm a false one; it now says whether it sent.
5. Metrics formatting (`5.0E-5` bucket labels), series that only appear after their first event, and a link "cut" by a test hook still counting as connected.
