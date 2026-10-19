# Ember

A Kafka-style message broker written from scratch in plain Java 21, to learn distributed systems.
Phase 1 built the storage engine (log segments, index, recovery, retention, fsync policy).
Phase 2 added the network layer: a binary wire protocol, a Netty broker, a blocking client and a CLI, with zero-copy fetch.
Phase 3 added **partitions**: a topic is N independent logs, producers route records by key (murmur2) or round-robin, consumers read any partition, and topics survive restarts with their partition count intact.
Phase 4 built **Raft consensus in isolation**: a pure, deterministic Raft core (`ember-raft`) and a simulator that attacks it with thousands of failure schedules.
Phase 5 made partitions **replicated**: a cluster of 1, 3 or 5 brokers where every partition (and the topic metadata) is its own Raft group. A produce is acknowledged only after a majority has it and the leader has applied it. Leaders can die (`kill -9`) without losing an acknowledged record, and a restarted broker catches up by itself. Design: [`docs/phase5/design.md`](docs/phase5/design.md).
**Phase 6 (this one)** adds **consumer groups**: many consumers read a set of topics as a group. The coordinator (the leader of one more Raft group, `__groups`) divides the partitions among the live members, rebalances when members join, leave or crash, and stores the **committed offsets** in Raft, so a consumer, or a whole broker, can die and the group resumes from the last commit. Delivery is at-least-once. Design: [`docs/phase6/design.md`](docs/phase6/design.md).

## Modules

| Module | What it is | Depends on |
|--------|-----------|------------|
| `ember-storage` | `Log`, `Segment`, `LogRecord`, index, retention, flush policy; thread-safe; `openSlice` for zero-copy | nothing |
| `ember-protocol` | request/response types and codecs (pure `ByteBuffer`), `Partitioner` (murmur2 + round-robin), METADATA v2, RAFT_MESSAGE | storage, raft |
| `ember-replication` | `FileRaftStorage`, `GroupRuntime` (serial mailbox + timers + propose-and-wait around one `RaftNode`), `PartitionStateMachine` (idempotent apply into the data log), `MetadataStateMachine`, `LocalTransport`; Phase 6: `GroupStateMachine` (generations, members, offsets; deterministic), `GroupCoordinator` (leader logic, session expiry), `Assignor` (range) | raft, storage, protocol |
| `ember-broker` | Netty server, `ReplicaManager` (META group + one Raft group per partition), `PeerNetwork`, `BrokerApis`, `EmberBroker` main | replication, protocol, storage, Netty |
| `ember-client` | `ClusterClient` (leader routing, controller redirect, coordinator discovery, retries), `EmberProducer`, `EmberConsumer` (manual), `EmberGroupConsumer` (groups), `EmberCli` | protocol |
| `ember-raft` | `RaftNode` (leader election + log replication, single-threaded, deterministic), `RaftStorage`, and the simulator `com.ember.raft.sim` (`SimCluster`, invariant checkers, random scenarios, `SimMain`) | nothing (JUnit for tests) |

Docs: [`docs/PROTOCOL.md`](docs/PROTOCOL.md) (byte layouts, hex dump, semantics), `docs/phase2/` (one note per step explaining the *why*), `docs/phase3/` (the same for Phase 3, plus `bench.md`), `docs/phase4/` (Raft: one note per step, `raft-rules-to-code.md`, `simulator.md`, `bug-injection.md`, `phase5-notes.md`), `docs/phase5/` (replication: one note per step and `design.md` with the failure matrix), `docs/phase6/` (consumer groups: one note per step and `design.md`), `docs/ember-part6-notebooklm.md`, `docs/ember-part7-notebooklm.md`, `docs/ember-part3-notebooklm.md`, `docs/ember-part4-notebooklm.md`, `docs/ember-part5-notebooklm.md`.

## Build

```
mvn clean verify          # compiles, runs every test, builds the two runnable jars
mvn -q test               # tests only
```

Needs JDK 21 and Maven 3.9. Jars: `ember-broker/target/ember-broker-0.1.0-SNAPSHOT-all.jar` and `ember-client/target/ember-client-0.1.0-SNAPSHOT-all.jar`.

## Run the broker

```
java -jar ember-broker/target/ember-broker-0.1.0-SNAPSHOT-all.jar --port=9099 --data-dir=./ember-data
```

Flags (`--name=value`): `node-id` (0), `host` (localhost, advertised in METADATA), `port` (9099, 0 = any free port), `data-dir` (./ember-data), `segment-bytes` (64 MiB), `index-interval-bytes` (4096), `flush-messages` (never), `flush-ms` (1000), `retention-ms` (7 days), `retention-bytes` (unlimited), `default-partitions` (3, for auto-created topics), `max-partitions` (256), `max-frame-bytes` (8 MiB), `max-fetch-bytes` (1 MiB), `idle-timeout-ms` (300000), `auto-create-topics` (true), `handler-threads` (4), `maintenance-interval-ms` (250). For the limits, `never` / `unlimited` mean "off". Ctrl+C or SIGTERM shuts down cleanly (in-flight requests finish, every log is fsynced).

## Run a 3-broker cluster (Phase 5)

```
mvn -q package -DskipTests
scripts/run-cluster.sh 3 19001 ./cluster-data          # N, first port, data root (extra flags after, e.g. --unsafe-no-raft-fsync)
CLI="java -jar ember-client/target/ember-client-0.1.0-SNAPSHOT-all.jar --bootstrap=localhost:19001,localhost:19002,localhost:19003"
$CLI create-topic --topic=orders --partitions=3
$CLI produce --topic=orders --key=user-1 --value=hello
$CLI metadata --topic=orders                           # brokers, controller, per-partition leader / replicas / offsets
$CLI consume --topic=orders --from-beginning
kill -9 $(cat cluster-data/broker-1.pid)               # crash a broker; the client keeps working
scripts/run-cluster.sh 3 19001 ./cluster-data          # restarts only the dead one; it catches up by itself
scripts/stop-cluster.sh ./cluster-data
scripts/demo-failover.sh                               # the whole kill -9 demo with checks
```

Cluster flags: `--node-id=N --cluster=1@host:port,2@host:port,3@host:port` (same list on every broker; must contain self; data port = Raft port), `--raft-election-min-ms` (300), `--raft-election-max-ms` (600), `--raft-heartbeat-ms` (100), `--produce-timeout-ms` (5000), `--max-pending-produces` (1000), `--peer-queue-size` (10000), `--data-flush=never|message|<N>ms`, and `--unsafe-no-raft-fsync` (**unsafe**: acknowledged data can be lost on power failure; for benchmarks only). Without `--cluster` the broker is a cluster of one on the same code path. Phase 2 data directories are refused with a clear message (no migration).

Measured on one laptop (Apple M4, 3 broker processes + CLI on the same machine): kill -9 of a partition leader -> first acknowledged produce after **0.82 / 0.84 / 1.65 s** (min / median / max of 5); 4 partitions x 4 producers x 100 B x 50,000 records: 1 broker with Raft 22.8k records/s, 3 brokers fsync ON 10.9k records/s, 3 brokers without Raft fsync 42.1k records/s. Details in `docs/phase5/step9-real-processes.md`.

**Guarantees:** an acknowledged produce is on a majority of Raft logs and never lost or reordered while a majority survives; replicas converge byte for byte. **Not guaranteed:** exactly-once (retries can duplicate), availability without a majority, linearizable reads from a just-deposed leader. No snapshots yet: the Raft log grows and a restart replays it.

## Consumer groups (Phase 6)

```
scripts/run-cluster.sh 3 19001 ./cluster-data
CLI="java -jar ember-client/target/ember-client-0.1.0-SNAPSHOT-all.jar --bootstrap=localhost:19001,localhost:19002,localhost:19003"
$CLI create-topic --topic=orders --partitions=6
$CLI produce --topic=orders --count=3000 --keys=100 --value=order      # 3,000 keyed records with one producer
# three consumers in one group (run each in its own terminal, or in the background)
$CLI consume --group=demo --topic=orders --follow --session-ms=3000 --client-id=c1
$CLI consume --group=demo --topic=orders --follow --session-ms=3000 --client-id=c2
$CLI consume --group=demo --topic=orders --follow --session-ms=3000 --client-id=c3
$CLI group describe --group=demo        # coordinator, generation, members + assignment, per-partition committed / log end / LAG
$CLI group offsets --group=demo
kill -9 <pid of one consumer>           # its partitions move to the others after the session timeout
kill -9 $(cat cluster-data/broker-<coordinator>.pid)   # the coordinator broker: the group carries on
scripts/demo-groups.sh                  # the whole story with real processes and checks
scripts/stop-cluster.sh ./cluster-data
```

```java
GroupConsumerConfig config = GroupConsumerConfig.of(List.of("localhost:19001"), "demo");
try (EmberGroupConsumer consumer = new EmberGroupConsumer(config)) {
    consumer.subscribe("orders");
    while (running) {
        for (GroupRecord r : consumer.poll(Duration.ofSeconds(1))) { process(r); }     // process ...
        consumer.commitSync();                                                           // ... THEN commit (at-least-once)
    }
}   // close(): final commit if auto-commit is on, then leaves the group
```

Broker flags: `--group-min-session-ms` (1000), `--group-max-session-ms` (60000). New API keys 11-17 (FIND_COORDINATOR, JOIN_GROUP, HEARTBEAT, LEAVE_GROUP, OFFSET_COMMIT, OFFSET_FETCH, DESCRIBE_GROUP) and error codes 14-20: see `docs/PROTOCOL.md`.

**Guarantees:** at-least-once; each partition has one owner per generation; an offset commit is accepted only from the current owner in the current generation (a paused "zombie" consumer cannot overwrite its successor's offsets); committed offsets survive the loss of any minority of brokers and a whole-cluster restart. **Not guaranteed:** exactly-once (duplicates after rebalances and crashes, bounded by the commit interval), no automatic rebalance when a topic is created after subscribing, and the `__groups` log is not compacted yet.

Measured on one laptop (Apple M4, 3 broker processes + CLI processes on the same machine; details in `docs/phase6/step9-real-processes.md`): after `kill -9` of a consumer (session timeout 3 s) its partitions were reassigned after a median of 2.6 s; after `kill -9` of the coordinator broker the next commit succeeded after a median of 0.76 s (max 1.57 s); a synchronous OFFSET_COMMIT takes a median of 8.3 ms with Raft fsync on and 0.26 ms without.

## Raft simulator (Phase 4)

Watch elections, crashes and replication, or hunt for bugs:

```
mvn -q -pl ember-raft compile
SIM="java -cp ember-raft/target/classes com.ember.raft.sim.SimMain"
$SIM --seed=42 --nodes=5 --scenario=leader-crash        # election, replication, leader dies, re-election, old leader returns
$SIM --seed=7 --nodes=3 --scenario=election --proposals=3 --duration-ms=3000
$SIM --scenario=figure8                                 # the Figure 8 trap from the Raft paper: nothing is lost
$SIM --scenario=figure8 --bug=COMMIT_OLD_TERM_BY_COUNTING   # the same schedule with the classic mistake: the checkers shout
$SIM --scenario=random --seed=164 --nodes=3 --bug=FORGET_VOTED_FOR_ON_RESTART --quiet   # a violation report with seed and trace tail
mvn -q -pl ember-raft test                              # 166 tests, 300 random seeds
mvn -pl ember-raft test -Praft-long                     # 5,000 seeds + the slow tests (2,000 more)
mvn -q -pl ember-raft test -Dtest=RandomizedSafetyTest -Draft.seeds=1 -Draft.seedStart=164   # replay one seed
```

See `docs/phase4/simulator.md` for how to read a trace and replay a failing seed.

## Use the CLI

```
CLI="java -jar ember-client/target/ember-client-0.1.0-SNAPSHOT-all.jar"
$CLI create-topic --topic=orders --partitions=4
$CLI produce --topic=orders --key=user-1 --value=hello      # partition=2 offset=0
$CLI produce --topic=orders --key=user-1 --value=again      # same key -> same partition: partition=2 offset=1
$CLI produce --topic=orders --value=no-key                  # no key -> round-robin over partitions
$CLI consume --topic=orders --from-beginning                # prints partition, offset, key, value; stops when caught up
$CLI consume --topic=orders --follow                        # only new records, polls every 200 ms until Ctrl+C
$CLI fetch --topic=orders --partition=2 --offset=0 --max-bytes=65536
$CLI metadata                                               # table: topic, partition, leader, start offset, end offset
$CLI bench --topic=b --partitions=4 --producers=4 --records=200000 --value-bytes=100 --batch=500
```

All commands accept `--host=` and `--port=`. `produce` creates a missing topic with 3 partitions (use `--partitions=N` to change that).

### Example session

This is real output from the shaded jars (4 partitions, 20 keyed records; first lines shown). The same key always lands in the same partition: users 1 and 2 went to partition 0, users 0, 3 and 4 to partition 3. With only five keys it is normal that two partitions stay empty.

```
$ $CLI create-topic --topic=orders --partitions=4
created topic orders with 4 partition(s)
$ for i in ...; do $CLI produce --topic=orders --key=user-$((i % 5)) --value=order-$i; done
user-0 order-0  -> partition=3 offset=0
user-1 order-1  -> partition=0 offset=0
user-2 order-2  -> partition=0 offset=1
user-3 order-3  -> partition=3 offset=1
user-4 order-4  -> partition=3 offset=2
user-0 order-5  -> partition=3 offset=3
user-1 order-6  -> partition=0 offset=2
...
$ $CLI consume --topic=orders --from-beginning
partition=0 offset=0 key=user-1 value=order-1
partition=0 offset=1 key=user-2 value=order-2
partition=0 offset=2 key=user-1 value=order-6
partition=0 offset=3 key=user-2 value=order-7
partition=0 offset=4 key=user-1 value=order-11
partition=0 offset=5 key=user-2 value=order-12
...
$ $CLI metadata
broker: node=0 localhost:19100
TOPIC   PARTITION  LEADER  START OFFSET    END OFFSET
orders          0       0             0             8
orders          1       0             0             0
orders          2       0             0             0
orders          3       0             0            12
```

## Use the client from Java

```java
try (EmberProducer producer = new EmberProducer("localhost", 9099)) {         // creates the topic if missing
    RecordMetadata md = producer.send("orders", "alice".getBytes(), "order-1".getBytes());
    // md.partition(), md.offset()
}
try (EmberConsumer consumer = new EmberConsumer("localhost", 9099)) {
    consumer.assignAll("orders", EmberConsumer.StartPosition.BEGINNING);       // or assign(topic, partition, offset)
    for (ConsumedRecord r : consumer.poll(1_000_000)) { /* r.partition(), r.record().offset() ... */ }
}
```

`EmberClient` is the low-level one-request-at-a-time client (`createTopic`, `produce(topic, partition, records)`, `fetch`, `metadata`).

**Ordering:** records are ordered within a partition; records with the same key are ordered (same partition); there is no ordering across partitions.

## Known limitations (through Phase 6)

The partition count of a topic is fixed at creation and topics cannot be deleted; replication factor = cluster size, with no adding or removing brokers; no Raft snapshots (the Raft log grows, restart replays it); PRODUCE and FETCH name one partition per request; one request in flight per connection; consumer groups use eager rebalancing with a range assignor (not sticky), one coordinator for all groups, no offset expiry or group deletion, and the `__groups` log is never compacted; no TLS/auth (peers are trusted); no compression; no long-poll fetch; at-least-once produce (retries may duplicate); every record is written twice (Raft log + data log). See `docs/phase5/design.md` section 7 and `docs/phase6/design.md` section 8.
