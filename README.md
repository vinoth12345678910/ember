# Ember

A Kafka-style message broker written from scratch in plain Java 21, to learn distributed systems.
Phase 1 built the storage engine (log segments, index, recovery, retention, fsync policy).
Phase 2 added the network layer: a binary wire protocol, a Netty broker, a blocking client and a CLI, with zero-copy fetch.
Phase 3 added **partitions**: a topic is N independent logs, producers route records by key (murmur2) or round-robin, consumers read any partition, and topics survive restarts with their partition count intact.
**Phase 4 (this one)** builds **Raft consensus in isolation**: a pure, deterministic Raft core (`ember-raft`) and a simulator that attacks it with thousands of failure schedules, checking all five Raft safety properties after every event. It is not wired into the broker yet (Phase 5).

## Modules

| Module | What it is | Depends on |
|--------|-----------|------------|
| `ember-storage` | `Log`, `Segment`, `LogRecord`, index, retention, flush policy; thread-safe; `openSlice` for zero-copy | nothing |
| `ember-protocol` | request/response types and codecs (pure `ByteBuffer`), `Partitioner` (murmur2 + round-robin) | storage (for `LogRecord`) |
| `ember-broker` | Netty server, `LogManager` (topic -> partitions -> logs), `BrokerApis`, data-dir lock, `EmberBroker` main | protocol, storage, Netty |
| `ember-client` | blocking `EmberClient`, `EmberProducer`, `EmberConsumer`, `EmberCli` (plain sockets) | protocol |
| `ember-raft` | `RaftNode` (leader election + log replication, single-threaded, deterministic), `RaftStorage`, and the simulator `com.ember.raft.sim` (`SimCluster`, invariant checkers, random scenarios, `SimMain`) | nothing (JUnit for tests) |

Docs: [`docs/PROTOCOL.md`](docs/PROTOCOL.md) (byte layouts, hex dump, semantics), `docs/phase2/` (one note per step explaining the *why*), `docs/phase3/` (the same for Phase 3, plus `bench.md`), `docs/phase4/` (Raft: one note per step, `raft-rules-to-code.md`, `simulator.md`, `bug-injection.md`, `phase5-notes.md`), `docs/ember-part3-notebooklm.md`, `docs/ember-part4-notebooklm.md`, `docs/ember-part5-notebooklm.md`.

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

## Known limitations (Phase 3)

The partition count of a topic is fixed at creation and topics cannot be deleted; PRODUCE and FETCH name one partition per request; one request in flight per connection; partitioning is done by the client (all clients must use the same algorithm); consumer positions live in memory only (no groups, no committed offsets); a single broker, so the "leader" is always this node and there is no failover; no TLS/auth/compression; no replication yet (Raft exists only as an isolated, simulated component); no long-poll fetch; at-least-once produce; no write back-pressure for very slow readers; every segment keeps its files open. See `docs/phase3/` for the reasoning.
