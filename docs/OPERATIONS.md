# Operating Ember

A runbook for a cluster of 3 (or 5) Ember brokers. Everything here was exercised on the Docker Compose cluster or on real processes started by `scripts/run-cluster.sh`.

## 1. Start, stop, restart

| | Docker Compose | Plain processes |
|---|---|---|
| start | `docker compose up -d --build --wait` | `scripts/run-cluster.sh 3 19001 ./cluster-data` |
| stop (graceful) | `docker compose stop` (SIGTERM; the JVM gets it because the entrypoint `exec`s java) | `scripts/stop-cluster.sh ./cluster-data` |
| stop everything and delete data | `docker compose down -v` | `rm -rf ./cluster-data` |
| restart one broker | `docker restart ember2` | kill it, then run `scripts/run-cluster.sh ...` again (it starts only the ones that are not running) |

**Rolling restart / upgrade:** restart ONE broker at a time and wait until `GET /health/ready` is 200 on it (`ember healthcheck --admin=host:port`, exit code 0) before touching the next. A broker that is not ready is still replaying its log; taking a second one down at that moment would lose the majority of a 3-broker cluster. Upgrade notes: wire formats are versioned per API; the Raft log and data log formats have not changed since Phase 5. A Phase 2/3/4 data directory (no `__raft/`) is **refused** with a clear message: there is no migration, start from an empty directory.

**Flags that matter** (full list in the README): `--node-id`, `--cluster=1@host:port[/advertisedHost:port],...` (the same list on every broker; the data port is also the Raft port), `--advertise=host:port`, `--data-dir`, `--admin-port` (default data port + 1000, `0` = off), `--admin-host` (default `127.0.0.1`), `--enable-debug-endpoints` (off), `--raft-election-min-ms/-max-ms/--raft-heartbeat-ms` (300/600/100), `--produce-timeout-ms` (5000), `--unsafe-no-raft-fsync` (**never in production**: acknowledged data can be lost on power failure), `--group-min-session-ms/--group-max-session-ms`.

**The admin port is unauthenticated plain HTTP.** It never returns record keys or values, but it does show topic names, offsets and group members. Keep it on a trusted network.

## 2. Looking at a running cluster

```
ember cluster status  --admin=h1:p1,h2:p2,h3:p3    # one table: node, ready?, controller, coordinator, role of every node in every Raft group
ember cluster metrics --admin=h:p --filter=ember_raft_   # scrape one broker, print matching samples
ember healthcheck     --admin=h:p                   # exit 0 only if /health/ready is 200
ember group describe  --group=g                     # members, assignment, committed offset, log end, LAG per partition
ember metadata --topic=t                            # partition leaders, replicas, offsets
curl -s h:p/status                                  # the same JSON the commands above read
```
(`ember` = `java -jar ember-client/target/ember-client-*-all.jar`.)

* **`/health/live`**: the server runs. Restarting a live process does not help anybody.
* **`/health/ready`**: 200 only when the broker has joined the cluster, the META and `__groups` groups have a leader, and every local Raft group has learned a commit index and applied everything committed. The 503 body names the reason (for example `group orders-1 is still applying (applied 812 of committed 1040)`).
* **`/metrics`**: see `docs/phase7/design.md`, part 1, for which metric answers which question.

### Reading the logs
Logs go to stdout (`System.Logger`). Every line about Raft or replication starts with the broker and the group: `broker 2: group orders-1 is now LEADER in term 7 (leader 2)`; coordinator lines say `broker N: group G: ...`. Rare events (role changes, session expiry, CheckQuorum step-downs, topic creation, torn-tail repair) are INFO or WARNING. Chatty events (heartbeats, individual messages) are not logged at all. Startup prints one `resources:` line (heap max, processors, free disk of the data directory, admin address) and one `started:` line. `WARNING: admin HTTP ... UNAUTHENTICATED` is expected.

## 3. Symptom, likely cause, what to check, what to do

| Symptom | Likely cause | What to check | What to do |
|---|---|---|---|
| **No leader** for a partition (`ember metadata` shows leader -1; clients get `LEADER_NOT_AVAILABLE`) | An election is in progress, or no majority is alive/connected | `ember cluster status`: how many brokers are up? `peer_connected` on each broker; `raft_term` still rising? | Wait one election timeout (0.3-0.6 s default). If a majority is down, start the brokers; nothing is acknowledged without a majority and nothing is lost |
| **Leader flapping** (`raft_leader_changes_total` rising every few seconds) | Heartbeats not arriving in time: slow disk, GC pause, dropped messages, CPU starvation | `raft_fsync_seconds` p99, `jvm_gc_collection_seconds_total`, `peer_messages_dropped_total`, container CPU limit | Fix the cause (faster disk, bigger heap/CPU). As a stop-gap raise `--raft-election-min-ms/-max-ms` on **all** brokers (keep heartbeat below a third of the minimum) |
| **Produce timeouts** (`REQUEST_TIMED_OUT`, `ember_produce_requests_total{result!="ok"}`) | The commit is slow or impossible: no majority, a follower far behind, slow fsync | `raft_pending_proposals` climbing? `raft_fsync_seconds`, `raft_apply_latency_seconds`, `peer_connected` | Restore the majority; look at the slowest broker. Remember a `REQUEST_TIMED_OUT` is ambiguous: the record may exist, a retry can duplicate it |
| **A replica is lagging** (`log_end_offset` of one node below the others; its `raft_commit_index` behind) | Broker was down, paused or partitioned; or its disk is slow | `/health/ready` of that broker (503 with a reason while it catches up); `peer_queue_depth`/`dropped` towards it | Usually nothing: it catches up by itself. If `peer_messages_dropped_total` keeps growing, fix the network path |
| **Disk filling** | Data logs grow until retention removes segments; the **Raft log never shrinks** (no snapshots yet) | `log_size_bytes` per partition; `du -sh data/__raft` | Lower `--retention-ms/--retention-bytes` for the data logs. The Raft log is the limit: plan disk for the total ever written, or wait for snapshots (future work) |
| **Consumer group stuck / rebalancing too often** | Consumers crash or miss heartbeats (session too short, GC pauses); processing slower than the commit interval; a stuck user thread | `group_generation` rising, `group_rebalances_total`, `group_lag` growing, `ember group describe` | Raise `--session-ms` (above worst GC pause, below your failure-detection need); process faster or give the group more members; remember `maxPollIntervalMs` (5 min) removes a consumer that does not call `poll()` |
| **High fsync latency** | Slow or shared disk, `fsync` contention with other processes | `raft_fsync_seconds` p99; the produce-latency floor equals it | Move the data directory to a faster disk. `--unsafe-no-raft-fsync` removes the cost AND the durability guarantee: tests/benchmarks only |
| **`/health/ready` stays 503** | Still replaying/applying (normal for a while after a restart), or the group has failed | The reason in the 503 body; logs for `failed; it stops processing` | If a group has failed (an exception in a state machine), the broker needs a restart and the cause needs a bug report: it is never supposed to happen |
| **Clients connect but produce hangs** | The broker advertises an address the client cannot reach | `/status` shows `host/port` and `advertisedHost/advertisedPort` per broker | Fix `--cluster=...` third parts or `--advertise` (see `docs/phase7/design.md`, part 5) |
| **Broker refuses to start: "legacy single-node data directory"** | The directory is from Phase 2/3 | message names the directory | Start from an empty directory; there is no migration |
| **Broker refuses to start: "data directory is locked"** | Another broker process uses it | `lsof` on the directory | Stop the other process; never share a data directory |

## 4. Backup and restore of a data directory

Stop the broker first (a graceful stop fsyncs everything). Then copy the whole `--data-dir` (it contains `__raft/`, the data logs and a lock file). Restore by putting the copy back **under the same node id** with the broker stopped and starting it: it replays its Raft log and then catches up from the others. Do not restore an old copy of one broker into a cluster that has moved on without understanding the consequence: it will simply be brought up to date by the leader (Raft guarantees that), but an old copy of *every* broker restores the old state for everybody. A copy taken while the broker runs is not guaranteed consistent.

## 5. Example Prometheus alert rules (text only; nothing in the project loads them)

```yaml
groups:
  - name: ember
    rules:
      - alert: EmberNoController
        expr: count(ember_raft_role{group="__meta-0"} == 2) != 1
        for: 30s
      - alert: EmberLeaderFlapping
        expr: increase(ember_raft_leader_changes_total{group="__meta-0"}[10m]) > 6
      - alert: EmberBrokerDown
        expr: up{job="ember"} == 0
        for: 30s
      - alert: EmberProduceSlow
        expr: histogram_quantile(0.99, sum by (le) (rate(ember_produce_latency_seconds_bucket[5m]))) > 0.5
        for: 5m
      - alert: EmberFsyncSlow
        expr: histogram_quantile(0.99, sum by (le, node) (rate(ember_raft_fsync_seconds_bucket[5m]))) > 0.1
        for: 5m
      - alert: EmberPeerDrops
        expr: rate(ember_peer_messages_dropped_total[5m]) > 1
        for: 5m
      - alert: EmberConsumerLagGrowing
        expr: deriv(ember_group_lag[10m]) > 0 and ember_group_lag > 1000
        for: 15m
      - alert: EmberMemoryLeak
        expr: deriv(ember_jvm_heap_after_gc_bytes[1h]) > 1e6
        for: 2h
```
