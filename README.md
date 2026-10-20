# Ember

**A Kafka-style message broker, written from scratch in plain Java 21, to learn how replicated logs really work.**
Topics are split into partitions. Every partition is replicated on three brokers and kept consistent by **Raft**, so a message that Ember acknowledged survives the loss of any one broker, including the leader. Consumers share work in **consumer groups** with committed offsets that are replicated too. The cluster can be watched (metrics, health, status), attacked (a chaos tool with an independent history checker), and started with one `docker compose up`.

Only Netty and the Maven shade plugin are used as dependencies: the storage engine, the wire protocol, Raft, the metrics library, the JSON handling and the chaos checker are all in this repository.

**Tests:** 747 tests, 0 failures (metrics 15, storage 55, raft 182, protocol 97, replication 92, client 39, broker 215, chaos 52), default run about 5.5 minutes; the `cluster-long` profile adds three 120 s chaos runs (8 chaos run tests, 7.5 minutes), all green.

## Architecture

```
   producer / consumer / CLI                    (client library: finds leaders, retries, consumer groups)
            |   binary protocol over TCP (PRODUCE, FETCH, METADATA, CREATE_TOPIC, group APIs ...)
            v
 +----------------------------------------------------------------------------------------------+
 |  broker 1                         broker 2                         broker 3                  |
 |  (Netty server, admin HTTP)       (the same)                       (the same)                |
 |                                                                                              |
 |  one Raft group per partition, EVERY broker is a voter in EVERY group:                       |
 |     orders-0 [L]----------------- orders-0 [F] ------------------ orders-0 [F]              |
 |     orders-1 [F]----------------- orders-1 [L] ------------------ orders-1 [F]              |
 |  two special groups:                                                                         |
 |     __meta   : the list of topics (its leader is the "controller")                           |
 |     __groups : consumer groups, members, committed offsets (its leader is the "coordinator") |
 |                                                                                              |
 |  per partition replica, TWO logs:                                                            |
 |     Raft log  (fsync, the truth)  --apply, once committed-->  data log (what consumers read) |
 +----------------------------------------------------------------------------------------------+
        ^ broker-to-broker traffic is only RAFT_MESSAGE frames (one connection per peer)
```

A produce is acknowledged only after the entry is committed on a **majority** of Raft logs and applied on the leader. Reading is zero-copy from the data log, served by the partition's leader only. Details and a failure matrix: [`docs/phase5/design.md`](docs/phase5/design.md), [`docs/phase6/design.md`](docs/phase6/design.md), [`docs/phase7/design.md`](docs/phase7/design.md).

## Features

* **Storage engine** (Phase 1): segmented log, sparse index, CRC per record, crash recovery of torn tails, retention, fsync policy.
* **Network and protocol** (Phase 2-3): binary request/response protocol, Netty broker with zero-copy fetch, partitions with murmur2 key routing, a blocking client library and CLI.
* **Raft, proven by simulation** (Phase 4): a pure, deterministic Raft core with a simulator that attacks it with thousands of failure schedules and checks all five Raft safety properties after every event; five injected classic bugs are all caught.
* **Replicated partitions** (Phase 5): per-partition Raft groups, leader failover without losing an acknowledged record, a restarted broker catches up by itself, CheckQuorum, idempotent apply of the Raft log into the data log.
* **Consumer groups** (Phase 6): generations and fencing (a paused "zombie" consumer cannot overwrite its successor's offsets), eager rebalancing, session timeouts, offsets committed through Raft, at-least-once delivery.
* **Observability** (Phase 7): own metrics library with Prometheus text output, admin HTTP server (`/metrics`, `/health/live`, `/health/ready`, `/status`), structured log prefixes, a `cluster status` command.
* **Chaos verification** (Phase 7): a recorder of client histories, a checker for nine properties (no lost acknowledged write, no divergence, no phantoms, unique offsets, per-key order, reads never shrink, group delivery, commits and fencing, replica agreement) that is itself mutation-tested, and a nemesis that crashes, freezes, isolates, slows and drops.
* **Benchmark tooling** (Phase 7): percentiles, warm-up, CSV, and a fixed-rate mode that avoids *coordinated omission*.
* **Docker** (Phase 7): a 3-broker `docker compose` cluster, optional Prometheus + Grafana with a provisioned dashboard.

## Quickstart (Docker)

```
mvn -q package -DskipTests                        # builds the jars (also done inside the image)
docker compose up -d --build --wait               # 3 brokers; --wait returns when all are healthy
alias ember='java -jar ember-client/target/ember-client-0.1.0-SNAPSHOT-all.jar'
B=localhost:19092,localhost:19093,localhost:19094 ; A=localhost:18081,localhost:18082,localhost:18083

ember cluster status --admin=$A                                       # nodes, readiness, who leads which Raft group
ember create-topic --topic=orders --partitions=3 --bootstrap=$B
ember produce --topic=orders --count=300 --keys=30 --value=order --bootstrap=$B
ember metadata --topic=orders --bootstrap=$B                          # leader, replicas, offsets per partition
ember consume --group=shop --topic=orders --follow --bootstrap=$B     # a consumer group (run several to share the work)
ember group describe --group=shop --bootstrap=$B                      # members, assignment, committed offset, LAG

docker kill ember2                                # kill a broker (try it while a producer runs): clients carry on
docker start ember2                               # it catches up by itself
scripts/demo.sh --kill                            # the whole story, narrated
docker compose --profile observability up -d      # + Prometheus :19090 and Grafana :13000 (dashboard "Ember cluster")
docker compose down -v                            # stop and delete the data
```

Without Docker: `scripts/run-cluster.sh 3 19001 ./cluster-data` starts 3 real broker processes (`scripts/stop-cluster.sh` stops them; `scripts/demo-failover.sh` and `scripts/demo-groups.sh` are narrated `kill -9` demos).

Run the chaos tool (in-process brokers by default; 20 seconds, all fault types, exit code 0 only if the checker finds nothing):
```
java -jar ember-chaos/target/ember-chaos-0.1.0-SNAPSHOT-all.jar --duration=20 --history=history.jsonl
java -jar ember-chaos/target/ember-chaos-0.1.0-SNAPSHOT-all.jar --check-only=history.jsonl     # re-check a saved history
scripts/chaos-docker.sh 300                       # 5 minutes against the docker compose cluster
```

## Guarantees and non-guarantees

| Guaranteed (while a majority of brokers is alive and the Raft log is fsynced) | Not guaranteed |
|---|---|
| An acknowledged produce is on a majority of Raft logs and is never lost or reordered, across any number of leader changes | **Exactly-once.** Retries after a timeout or lost connection can duplicate a record (at-least-once) |
| Replicas converge to byte-identical data logs | Availability without a majority (2 of 3 down: nothing is acknowledged, nothing is lost) |
| A fetch returns only committed data | Linearizable reads from a leader that was just deposed (no ReadIndex) |
| Per-partition order; same key, same partition, same order | Ordering across partitions |
| Each consumer-group partition has one owner per generation; a stale-generation commit is rejected | Exactly-once consumption (duplicates after rebalances/crashes, bounded by the commit interval) |
| Committed offsets survive the loss of any minority of brokers and a whole-cluster restart | Durability with `--unsafe-no-raft-fsync` (benchmarks only) |
| Metrics, health and status endpoints never expose record keys or values | Security: no TLS, no authentication; the admin port is plain HTTP; peers are trusted |

## Measured performance

One laptop (**Apple M4, 10 cores, 16 GiB, macOS, Java 21**), real broker processes, **all brokers and clients sharing one SSD**; 4 producers, batches of 100; numbers are from one run each (recipe and raw files: [`docs/phase7/step9-benchmarks.md`](docs/phase7/step9-benchmarks.md), [`docs/benchmarks/`](docs/benchmarks/)). Treat them as a description of this setup, not as a claim about other hardware.

| | records/s | p50 | p99 |
|---|---|---|---|
| 3 brokers, Raft fsync **on**, 100 B, 3 partitions | 3,491 | 114.7 ms | 151.6 ms |
| 3 brokers, fsync on, 1 KB, 3 partitions | 3,841 (4.1 MB/s) | 106.5 ms | 151.6 ms |
| 3 brokers, fsync **off** (unsafe), 100 B, 3 partitions | 126,680 | 2.56 ms | 8.96 ms |
| 1 broker, fsync on / off, 100 B | 9,845 / 272,292 | 41.0 / 1.28 ms | 51.2 / 3.71 ms |
| single-record produce, fixed rate at 25% / 50% / 75% of max (117/s) | 29 / 58 / 87 | 29.7 / 30.2 / 28.7 ms | 44.0 / 49.2 / **143.4** ms |

* **Failover** (20 x `kill -9` of the partition leader, time to the first successful produce): median **770 ms** (min 583, p90 979, max 1,598) with default Raft timings; median **559 ms** with 150-300 ms elections.
* **A broker that missed 100,000 records** was ready and complete 2.2-2.8 s after its restart.
* **Consumer groups** (Phase 6): after `kill -9` of a consumer (session timeout 3 s) its partitions were reassigned after a median of 2.6 s; after `kill -9` of the coordinator broker the next commit succeeded after a median of 0.76 s; one `OFFSET_COMMIT` takes a median of 8.3 ms with fsync, 0.26 ms without.
* Honest reading: fsync is the cost (all three brokers fsync the same disk here), and 12 partitions were *slower* than 3 because the producer sends one request per partition in sequence.

## How I tested it

* **Raft core:** 1,000+ random seeds with all safety checkers, plus mutation tests (five injected bugs, each caught).
* **Whole system chaos:** real brokers, real sockets, a seeded schedule of crashes, freezes, isolations, cut links, slow fsync, dropped and delayed messages, frozen consumers. The recorded history is checked offline against nine properties. The checker is mutation-tested: one injected bug per test, each caught with the expected code, the untouched history reports nothing.
* **External runs:** 5 minutes against the Docker cluster (6 container kills, 4 freezes, 5 network isolations, 3 frozen consumers, 102,840 acknowledged writes): 0 violations. A 15-minute soak with a crash or freeze every 30-60 s and resource-trend analysis: [`docs/phase7/step8-soak.md`](docs/phase7/step8-soak.md).
* **Real bugs these tools found** (all fixed, all with regression tests): [`docs/phase7/design.md`](docs/phase7/design.md) part 7 and the "Bugs and surprises" sections of the NotebookLM documents.
* The chaos checker is a test, not a proof; what it cannot show is listed in [`docs/phase7/design.md`](docs/phase7/design.md) part 2.

## Failure matrix (summary)

| Failure | What happens |
|---|---|
| Follower crashes | Nothing visible. It replays its Raft log and catches up when it returns |
| Leader crashes | A new leader after one election (about 0.6-0.8 s measured); no acknowledged record is lost; a record in flight may be duplicated by a retry |
| Leader cut off from the others | CheckQuorum makes it step down; the majority elects a new one; its uncommitted proposals are overwritten |
| Majority lost | Nothing is acknowledged, nothing is lost; progress resumes when a majority returns |
| Controller or coordinator crashes | The other brokers elect a new one; topics, members, generations and offsets are replicated, so consumers carry on |
| Torn tail of the data log or the Raft log | Repaired on start (the data log is rebuilt from the Raft log by idempotent apply) |
| Consumer crashes or freezes | Its partitions move to the others after the session timeout; its uncommitted records are delivered again |

The full table with what the client sees: [`docs/phase5/design.md`](docs/phase5/design.md) section 5 and [`docs/phase6/design.md`](docs/phase6/design.md) section 6.

## Project structure

| Module | What it is |
|---|---|
| `ember-storage` | `Log`, segments, index, retention, flush policy, zero-copy slices (no dependencies) |
| `ember-metrics` | `Counter`, `Gauge`, `Histogram`, `MetricsRegistry`, Prometheus text format (no dependencies) |
| `ember-raft` | the pure deterministic Raft core and its simulator (no dependencies) |
| `ember-protocol` | request/response types and codecs for every API, `Partitioner` |
| `ember-replication` | `FileRaftStorage`, `GroupRuntime`, partition/metadata/group state machines, `GroupCoordinator`, `Assignor`, fault-injection hooks |
| `ember-broker` | the Netty broker, `ReplicaManager`, `PeerNetwork`, admin HTTP server |
| `ember-client` | `ClusterClient`, `EmberProducer`, `EmberConsumer`, `EmberGroupConsumer`, the CLI, bench |
| `ember-chaos` | history recorder, checker, nemesis, in-process and external runners, failover/catch-up benchmarks |

Documentation: [`docs/PROTOCOL.md`](docs/PROTOCOL.md) (byte layouts and semantics), [`docs/OPERATIONS.md`](docs/OPERATIONS.md) (runbook), `docs/phase1..7/` (one note per step explaining *why*, plus a design document per phase), `docs/ember-part*-notebooklm.md` (study guides), `docs/DEMO-SCRIPT.md`, `docs/INTERVIEW-QA.md`, `docs/RESUME-BULLETS.md`.

## Build and run

```
mvn clean verify                 # compiles, runs every test, builds the runnable jars (about 4-5 minutes)
mvn -q test                      # tests only
mvn verify -Pcluster-long        # also the slow tests: 60 s and 120 s chaos runs
```
Needs JDK 21 and Maven 3.9. Jars: `ember-broker/target/ember-broker-0.1.0-SNAPSHOT-all.jar`, `ember-client/target/ember-client-0.1.0-SNAPSHOT-all.jar`, `ember-chaos/target/ember-chaos-0.1.0-SNAPSHOT-all.jar`.

A broker by hand:
```
java -jar ember-broker/target/ember-broker-0.1.0-SNAPSHOT-all.jar --node-id=1 --data-dir=./data-1 \
     --cluster=1@localhost:9001,2@localhost:9002,3@localhost:9003      # run the same command with --node-id=2 and 3 (their own data dirs)
java -jar ember-broker/target/ember-broker-0.1.0-SNAPSHOT-all.jar --port=9099 --data-dir=./ember-data           # a single broker (a cluster of one)
```
Main flags (`--name=value`): `node-id`, `cluster` (`id@host:port[/advertisedHost:port],...`), `advertise`, `port`, `data-dir`, `admin-port` (default port+1000, `0` = off), `admin-host` (127.0.0.1), `enable-debug-endpoints` (off), `raft-election-min-ms/-max-ms/-heartbeat-ms` (300/600/100), `produce-timeout-ms` (5000), `unsafe-no-raft-fsync` (**unsafe**), `data-flush`, `segment-bytes`, `retention-ms/-bytes`, `default-partitions`, `max-partitions`, `max-frame-bytes`, `handler-threads`, `group-min/max-session-ms`. More in `docs/OPERATIONS.md`. SIGTERM/Ctrl+C shuts down cleanly. The admin port is **unauthenticated**: keep it on a trusted network.

### CLI reference
```
create-topic --topic=t --partitions=N          produce --topic=t [--key=k] --value=v | --count=N [--keys=100]
consume --topic=t [--from-beginning] [--follow]    consume --group=g --topic=a,b [--from=earliest|latest] [--max=N] [--follow]
fetch --topic=t [--partition=0] [--offset=0]       metadata [--topic=t]
group describe|offsets --group=g                   cluster status|metrics --admin=h:p,...      healthcheck --admin=h:p
bench [--producers=1] [--records=200000] [--value-bytes=100] [--batch=500] [--warmup-records=0] [--rate=N] [--csv=FILE]
```
All commands take `--bootstrap=h1:p1,h2:p2` (or `--host`/`--port`) and `--retries=N`.

### The Java client
```java
try (EmberProducer producer = new EmberProducer(ClientConfig.defaults().withBootstrap(List.of("localhost:19092")))) {
    RecordMetadata md = producer.send("orders", "alice".getBytes(), "order-1".getBytes());     // md.partition(), md.offset()
}
GroupConsumerConfig config = GroupConsumerConfig.of(List.of("localhost:19092"), "shop");
try (EmberGroupConsumer consumer = new EmberGroupConsumer(config)) {
    consumer.subscribe("orders");
    while (running) {
        for (GroupRecord r : consumer.poll(Duration.ofSeconds(1))) { process(r); }              // process ...
        consumer.commitSync();                                                                  // ... THEN commit (at-least-once)
    }
}
```

## Known limitations

Replication factor = cluster size, with no adding or removing brokers; no Raft snapshots (the Raft log never shrinks and restart replays it); every record is written twice (Raft log + data log); one coordinator for all consumer groups and no compaction of the `__groups` log; eager rebalancing with a non-sticky range assignor; the producer sends one request per partition in sequence (more partitions can mean *lower* single-producer throughput); one advertised address per broker (no multiple listeners); no TLS or authentication; no exactly-once or idempotent producer; no long-poll fetch; the partition count of a topic is fixed and topics cannot be deleted.

## What I would build next

Raft snapshots and log compaction; an idempotent producer (removes the duplicate caveat); a pipelining producer; sharded group coordinators; cooperative rebalancing and a sticky assignor; adding and removing brokers; tiered storage; TLS and authentication.

## What I learned

* In a distributed system the bugs live in the **glue** (thread pools, readiness checks, retries, bookkeeping), not in the consensus algorithm, which a simulator can check on its own.
* A **checker is only as good as its input**: my first chaos alarm was my own recorder writing down an acknowledgement that never happened.
* **Measure, then explain**: the 12-partition slowdown and the fsync cost were not what I expected, and the explanation (one request per partition; three brokers on one disk) came from looking at the numbers.
* A benchmark clock that includes the shutdown, or a closed loop that waits for the stall, silently lies.
* Writing down *why* before the code kept every later phase honest.
