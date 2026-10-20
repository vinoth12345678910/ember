package com.ember.broker;

import com.ember.client.*;
import com.ember.protocol.ClusterMetadataResponse;
import com.ember.protocol.ErrorCode;
import com.ember.protocol.ProducerRecord;
import com.ember.storage.LogRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/** Real brokers, real sockets, real client: the promise "an acknowledged record is never lost or reordered". */
class ClusterIntegrationTest {

    static ClientConfig cfg(TestCluster c) {
        return ClientConfig.defaults().withBootstrap(List.of(c.bootstrap().split(","))).withRetries(40, 50, 400)
                .withTimeouts(2_000, 12_000).withAutoCreate(false, 3);
    }

    static void createTopic(TestCluster c, String topic, int partitions) throws Exception {
        try (ClusterClient admin = new ClusterClient(cfg(c))) {
            try {
                admin.createTopic(topic, partitions);
            } catch (EmberException e) {
                if (e.errorCode() != ErrorCode.TOPIC_ALREADY_EXISTS) throw e;
            }
        }
        c.awaitTopicEverywhere(topic, partitions);
        for (int p = 0; p < partitions; p++) c.awaitLeader(topic, p);
    }

    /** Like createTopic, but through the brokers' INTERNAL addresses (for clusters whose advertised addresses are unreachable). */
    static void createTopic3(TestCluster c, String topic, int partitions) throws Exception {
        int controller = c.awaitController();
        try (EmberClient client = new EmberClient("localhost", c.port(controller))) {
            client.createTopic(topic, partitions);
        }
        c.awaitTopicEverywhere(topic, partitions);
        for (int p = 0; p < partitions; p++) c.awaitLeader(topic, p);
    }

    static Map<Integer, List<LogRecord>> readAll(TestCluster c, String topic) throws Exception {
        Map<Integer, List<LogRecord>> out = new TreeMap<>();
        try (EmberConsumer consumer = new EmberConsumer(cfg(c))) {
            consumer.assignAll(topic, EmberConsumer.StartPosition.BEGINNING);
            consumer.assignment().forEach(tp -> out.put(tp.partition(), new ArrayList<>()));
            int emptyPolls = 0;
            while (emptyPolls < 2) {
                List<ConsumedRecord> batch = consumer.poll(1_000_000);
                emptyPolls = batch.isEmpty() ? emptyPolls + 1 : 0;
                for (ConsumedRecord r : batch) out.get(r.partition()).add(r.record());
            }
        }
        return out;
    }

    static List<String> values(List<LogRecord> records) {
        List<String> out = new ArrayList<>();
        for (LogRecord r : records) out.add(new String(r.value()));
        return out;
    }

    /** Waits until every up broker holds an identical data log for the partition, then asserts it byte for byte. */
    static void assertReplicasIdentical(TestCluster c, String topic, int partition) throws Exception {
        Await.until(30_000, "all up replicas of " + topic + "-" + partition + " to be identical\n" + c.describe(), () -> {
            try {
                List<String> reference = null;
                for (int i = 1; i <= c.n; i++) {
                    if (!c.isUp(i)) continue;
                    List<String> s = c.summary(i, topic, partition);
                    if (reference == null) reference = s;
                    else if (!reference.equals(s)) return false;
                }
                return true;
            } catch (Exception e) {
                return false;
            }
        });
        byte[] reference = null;
        for (int i = 1; i <= c.n; i++) {
            if (!c.isUp(i)) continue;
            byte[] bytes = c.dataLogBytes(i, topic, partition);
            if (reference == null) reference = bytes;
            else assertArrayEquals(reference, bytes, "broker " + i + " differs byte-for-byte");
        }
    }

    // ---------------------------------------------------------------- creation, routing, ordering

    @Test
    void aTopicCreatedThroughANonControllerIsRedirectedAndAppearsEverywhere(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            int controller = c.awaitController();
            int other = controller == 1 ? 2 : 1;
            ClientConfig onlyOther = ClientConfig.defaults().withBootstrap(List.of("localhost:" + c.port(other))).withRetries(40, 50, 400);
            try (ClusterClient client = new ClusterClient(onlyOther)) {
                assertEquals(3, client.createTopic("orders", 3), "bootstrapping from a follower, the client finds the controller itself");
                ClusterMetadataResponse m = client.clusterMetadata("orders");
                assertEquals(3, m.brokers().size());
                assertEquals(controller, m.controllerId());
            }
            c.awaitTopicEverywhere("orders", 3);
            for (int p = 0; p < 3; p++) c.awaitLeader("orders", p);
            try (ClusterClient client = new ClusterClient(cfg(c))) {
                ClusterMetadataResponse m = client.clusterMetadata("orders");
                for (var part : m.topics().get(0).partitions()) {
                    assertEquals(List.of(1, 2, 3), part.replicas());
                }
                assertEquals(ErrorCode.TOPIC_ALREADY_EXISTS, assertThrows(EmberException.class, () -> client.createTopic("orders", 3)).errorCode());
            }
        }
    }

    @Test
    void producedRecordsAreByteExactOnEveryReplica(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            createTopic(c, "t", 1);
            try (EmberProducer producer = new EmberProducer(cfg(c))) {
                producer.sendBatch("t", List.of(new ProducerRecord("k".getBytes(), "v".getBytes()), new ProducerRecord(null, new byte[0]),
                        new ProducerRecord(new byte[]{0, (byte) 0xFF}, new byte[]{(byte) 0x80, 1, 2})));
            }
            Map<Integer, List<LogRecord>> all = readAll(c, "t");
            List<LogRecord> r = all.get(0);
            assertEquals(3, r.size());
            assertArrayEquals("k".getBytes(), r.get(0).key());
            assertNull(r.get(1).key());
            assertEquals(0, r.get(1).value().length);
            assertArrayEquals(new byte[]{0, (byte) 0xFF}, r.get(2).key());
            assertArrayEquals(new byte[]{(byte) 0x80, 1, 2}, r.get(2).value());
            assertReplicasIdentical(c, "t", 0);
        }
    }

    @Test
    void keyedOrderingHoldsAcrossAThreeBrokerCluster(@TempDir Path dir) throws Exception {
        int keys = 50, perKey = 200, threads = 5;
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            createTopic(c, "ordered", 3);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int id = t;
                futures.add(pool.submit(() -> {
                    try (EmberProducer producer = new EmberProducer(cfg(c))) {
                        for (int seq = 0; seq < perKey; seq++) {
                            for (int k = id * (keys / threads); k < (id + 1) * (keys / threads); k++) {   // each thread owns 10 keys
                                producer.send("ordered", ("key-" + k).getBytes(), ("key-" + k + ":" + seq).getBytes());
                            }
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> f : futures) f.get(180, TimeUnit.SECONDS);
            pool.shutdown();

            Map<Integer, List<LogRecord>> all = readAll(c, "ordered");
            Map<String, Integer> next = new HashMap<>();
            int total = 0;
            for (var e : all.entrySet()) {
                long expected = 0;
                for (LogRecord r : e.getValue()) {
                    assertEquals(expected++, r.offset());
                    String[] parts = new String(r.value()).split(":");
                    assertEquals(next.getOrDefault(parts[0], 0), Integer.parseInt(parts[1]), "key " + parts[0] + " out of order");
                    next.merge(parts[0], 1, Integer::sum);
                    total++;
                }
            }
            assertEquals(keys * perKey, total, "no retries were needed in a calm cluster, so no duplicates and no losses");
            for (int p = 0; p < 3; p++) assertReplicasIdentical(c, "ordered", p);
        }
    }

    // ---------------------------------------------------------------- failure scenarios

    /** What the producer was told, in the order it was told. */
    static final class Producing implements AutoCloseable {
        final List<String> acked = new CopyOnWriteArrayList<>();
        final AtomicBoolean stop = new AtomicBoolean();
        final EmberProducer producer;
        final Thread thread;
        volatile long lastAckNanos;
        volatile Throwable crashed;

        Producing(TestCluster c, String topic, String prefix) throws Exception {
            producer = new EmberProducer(cfg(c));
            thread = new Thread(() -> {
                int seq = 0;
                while (!stop.get()) {
                    String value = prefix + (seq++);
                    try {
                        producer.send(topic, null, value.getBytes());
                        acked.add(value);
                        lastAckNanos = System.nanoTime();
                    } catch (EmberException | IOException e) {
                        // not acknowledged: it may or may not exist; the next value is a fresh one
                    } catch (Throwable t) {
                        crashed = t;
                        return;
                    }
                }
            }, "producing-" + prefix);
            thread.setDaemon(true);
            thread.start();
        }

        void awaitAcks(int count) {
            int target = acked.size() + count;
            Await.until(40_000, count + " more acknowledgements", () -> acked.size() >= target);
        }

        @Override
        public void close() throws Exception {
            stop.set(true);
            thread.join(30_000);
            producer.close();
            assertNull(crashed);
        }
    }

    /** Every acknowledged value present, in acknowledgement order; extra copies only if the client retried. */
    static void auditAcknowledged(List<String> acked, List<String> stored, boolean retriesHappened) {
        Map<String, Integer> firstPosition = new HashMap<>();
        Map<String, Integer> copies = new HashMap<>();
        for (int i = 0; i < stored.size(); i++) {
            firstPosition.putIfAbsent(stored.get(i), i);
            copies.merge(stored.get(i), 1, Integer::sum);
        }
        int previous = -1;
        for (String v : acked) {
            Integer pos = firstPosition.get(v);
            assertNotNull(pos, "acknowledged record " + v + " is MISSING");
            assertTrue(pos > previous, "acknowledged record " + v + " is out of order");
            previous = pos;
        }
        if (!retriesHappened) {
            copies.forEach((v, n) -> assertEquals(1, n, v + " is duplicated although the client never retried"));
        }
    }

    @Test
    void leaderFailoverUnderContinuousProductionLosesNoAcknowledgedRecord(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            createTopic(c, "t", 1);
            int leader = c.awaitLeader("t", 0);
            try (Producing p = new Producing(c, "t", "v")) {
                p.awaitAcks(30);
                int before = p.acked.size();
                long killedAt = System.nanoTime();
                c.crash(leader);
                Await.until(15_000, "a new leader", () -> c.leaderOf("t", 0) > 0 && c.leaderOf("t", 0) != leader);
                Await.until(15_000, "the producer to continue after the failover", () -> p.acked.size() > before + 5);
                long recoverMs = (p.lastAckNanos - killedAt) / 1_000_000;
                assertTrue(recoverMs < 15_000, "recovery took " + recoverMs + " ms");
                p.awaitAcks(30);
                p.stop.set(true);
                p.thread.join(30_000);
                c.restart(leader);
                assertReplicasIdentical(c, "t", 0);
                List<String> stored = c.summary(1, "t", 0).stream().map(s -> s.substring(s.lastIndexOf(':') + 1)).toList();
                auditAcknowledged(p.acked, stored, p.producer.retriedRequests() > 0);
            }
        }
    }

    @Test
    void aCrashedBrokerRestartsAndCatchesUpByItself(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            createTopic(c, "t", 2);
            try (EmberProducer producer = new EmberProducer(cfg(c))) {
                for (int i = 0; i < 40; i++) producer.send("t", ("k" + i).getBytes(), ("a" + i).getBytes());
                int victim = c.leaderOf("t", 0);
                c.crash(victim);
                for (int i = 0; i < 60; i++) producer.send("t", ("k" + i).getBytes(), ("b" + i).getBytes());   // while it is dead
                c.restart(victim);
                for (int i = 0; i < 20; i++) producer.send("t", ("k" + i).getBytes(), ("c" + i).getBytes());
            }
            for (int p = 0; p < 2; p++) assertReplicasIdentical(c, "t", p);
            int total = 0;
            for (int p = 0; p < 2; p++) total += c.summary(1, "t", p).size();
            assertEquals(120, total);
        }
    }

    @Test
    void anIsolatedLeaderStepsDownAndItsUnacknowledgedProposalIsGoneAfterHealing(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            createTopic(c, "t", 1);
            try (EmberProducer producer = new EmberProducer(cfg(c))) {
                for (int i = 0; i < 10; i++) producer.send("t", null, ("before" + i).getBytes());
                int old = c.awaitLeader("t", 0);
                c.isolate(old);
                // a client that still reaches the old leader: its produce can never be committed
                try (EmberClient direct = new EmberClient("localhost", c.port(old))) {
                    EmberException e = assertThrows(EmberException.class, () -> direct.produce("t", 0, List.of(new ProducerRecord(null, "unacked".getBytes()))));
                    assertTrue(e.errorCode() == ErrorCode.NOT_LEADER_OR_FOLLOWER || e.errorCode() == ErrorCode.REQUEST_TIMED_OUT, "got " + e.errorCode());
                }
                Await.until(20_000, "the majority to elect a new leader", () -> c.leaderOf("t", 0) > 0 && c.leaderOf("t", 0) != old);
                for (int i = 0; i < 10; i++) producer.send("t", null, ("after" + i).getBytes());   // the majority keeps committing
                c.heal();
            }
            assertReplicasIdentical(c, "t", 0);
            List<String> stored = c.summary(1, "t", 0).stream().map(s -> s.substring(s.lastIndexOf(':') + 1)).toList();
            assertFalse(stored.contains("unacked"), "the old leader's uncommitted proposal must be gone");
            assertEquals(20, stored.size());
            assertEquals("before0", stored.get(0));
            assertEquals("after9", stored.get(19));
        }
    }

    @Test
    void withTwoOfThreeBrokersDownNothingIsAcknowledgedAndProgressResumesWhenOneReturns(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false, "--produce-timeout-ms=1500")) {
            createTopic(c, "t", 1);
            ClientConfig quick = cfg(c).withRetries(2, 50, 100).withTimeouts(2_000, 6_000);
            int leader = c.awaitLeader("t", 0);
            try (EmberProducer producer = new EmberProducer(quick)) {
                producer.send("t", null, "acked-1".getBytes());
                int a = leader == 1 ? 2 : 1;
                int b = 6 - leader - a;
                c.crash(a);
                c.crash(b);
                assertThrows(Exception.class, () -> producer.send("t", null, "never-acked".getBytes()), "no majority: the produce must not be acknowledged");
                c.restart(a);
                ClientConfig patient = cfg(c);
                try (EmberProducer again = new EmberProducer(patient)) {
                    again.send("t", null, "acked-2".getBytes());
                }
                c.restart(b);
            }
            assertReplicasIdentical(c, "t", 0);
            List<String> stored = c.summary(1, "t", 0).stream().map(s -> s.substring(s.lastIndexOf(':') + 1)).toList();
            assertTrue(stored.contains("acked-1") && stored.contains("acked-2"));
            int unackedCopies = Collections.frequency(stored, "never-acked");
            assertTrue(unackedCopies <= 1, "an unacknowledged record may exist (once) after the leader returns, or not at all");
        }
    }

    @Test
    void aWholeClusterRestartKeepsTopicsAndDataAndKeepsWorking(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            createTopic(c, "a", 3);
            createTopic(c, "b", 1);
            try (EmberProducer producer = new EmberProducer(cfg(c))) {
                for (int i = 0; i < 60; i++) producer.send("a", ("k" + i).getBytes(), ("v" + i).getBytes());
                producer.send("b", null, "only".getBytes());
            }
            Map<Integer, List<String>> before = new TreeMap<>();
            for (var e : readAll(c, "a").entrySet()) before.put(e.getKey(), values(e.getValue()));
            for (int i = 1; i <= 3; i++) c.stop(i);
            for (int i = 1; i <= 3; i++) c.restart(i);
            c.awaitController();
            c.awaitTopicEverywhere("a", 3);
            c.awaitTopicEverywhere("b", 1);
            for (int p = 0; p < 3; p++) c.awaitLeader("a", p);
            c.awaitLeader("b", 0);
            Map<Integer, List<String>> after = new TreeMap<>();
            for (var e : readAll(c, "a").entrySet()) after.put(e.getKey(), values(e.getValue()));
            assertEquals(before, after);
            try (EmberProducer producer = new EmberProducer(cfg(c))) {
                RecordMetadata md = producer.send("b", null, "new".getBytes());
                assertEquals(1, md.offset(), "offsets continue where they stopped");
            }
            for (int p = 0; p < 3; p++) assertReplicasIdentical(c, "a", p);
        }
    }

    @Test
    void createTopicWhileTheControllerCrashesLeavesExactlyOneTopic(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            int controller = c.awaitController();
            ExecutorService pool = Executors.newSingleThreadExecutor();
            Future<Object> creation = pool.submit(() -> {
                try (ClusterClient client = new ClusterClient(cfg(c))) {
                    try {
                        return client.createTopic("racy", 4);
                    } catch (EmberException e) {
                        assertEquals(ErrorCode.TOPIC_ALREADY_EXISTS, e.errorCode(), "the first attempt may have won before the crash");
                        return -1;
                    }
                }
            });
            c.crash(controller);
            creation.get(60, TimeUnit.SECONDS);
            pool.shutdown();
            Await.until(30_000, "the topic on both survivors", () -> {
                for (int i = 1; i <= 3; i++) {
                    if (!c.isUp(i)) continue;
                    var t = c.servers[i].replicaManager().topic("racy");
                    if (t == null || t.partitionCount() != 4) return false;
                }
                return true;
            });
            c.restart(controller);
            c.awaitTopicEverywhere("racy", 4);
            for (int i = 1; i <= 3; i++) {
                assertEquals(1, c.servers[i].replicaManager().topics().stream().filter(t -> t.name().equals("racy")).count());
            }
        }
    }

    @Test
    void fiveBrokersTolerateTwoFailuresButNotThree(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 5, false, "--produce-timeout-ms=1500")) {
            createTopic(c, "t", 1);
            ClientConfig quick = cfg(c).withRetries(3, 50, 100).withTimeouts(2_000, 6_000);
            try (EmberProducer producer = new EmberProducer(cfg(c))) {
                producer.send("t", null, "r0".getBytes());
                c.crash(1);
                c.crash(2);
                producer.send("t", null, "r1".getBytes());               // 3 of 5 left: still a majority
                c.crash(3);
            }
            try (EmberProducer producer = new EmberProducer(quick)) {
                assertThrows(Exception.class, () -> producer.send("t", null, "no-majority".getBytes()));
            }
            c.restart(3);
            try (EmberProducer producer = new EmberProducer(cfg(c))) {
                producer.send("t", null, "r2".getBytes());
            }
            c.restart(1);
            c.restart(2);
            assertReplicasIdentical(c, "t", 0);
            List<String> stored = c.summary(4, "t", 0).stream().map(s -> s.substring(s.lastIndexOf(':') + 1)).toList();
            assertEquals(List.of("r0", "r1"), stored.subList(0, 2));
            assertTrue(stored.contains("r2"));
        }
    }

    @Test
    void withFsyncOnAcknowledgedDataSurvivesACrashOfEveryBroker(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, true)) {
            createTopic(c, "t", 2);
            try (EmberProducer producer = new EmberProducer(cfg(c))) {
                for (int i = 0; i < 30; i++) producer.send("t", ("k" + i).getBytes(), ("v" + i).getBytes());
            }
            Map<Integer, List<String>> before = new TreeMap<>();
            for (var e : readAll(c, "t").entrySet()) before.put(e.getKey(), values(e.getValue()));
            for (int i = 1; i <= 3; i++) c.crash(i);
            for (int i = 1; i <= 3; i++) c.restart(i);
            c.awaitController();
            for (int p = 0; p < 2; p++) c.awaitLeader("t", p);
            Map<Integer, List<String>> after = new TreeMap<>();
            for (var e : readAll(c, "t").entrySet()) after.put(e.getKey(), values(e.getValue()));
            assertEquals(before, after);
        }
    }

    @Test
    void aBrokerWhoseDataLogWasDeletedRebuildsItFromItsRaftLog(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            createTopic(c, "t", 1);
            try (EmberProducer producer = new EmberProducer(cfg(c))) {
                for (int i = 0; i < 25; i++) producer.send("t", null, ("v" + i).getBytes());
            }
            assertReplicasIdentical(c, "t", 0);
            byte[] reference = c.dataLogBytes(1, "t", 0);
            int victim = 2;
            c.stop(victim);
            try (var files = java.nio.file.Files.walk(dir.resolve("broker-" + victim).resolve("t-0"))) {
                for (Path p : files.sorted(Comparator.reverseOrder()).toList()) java.nio.file.Files.delete(p);
            }
            c.restart(victim);
            assertReplicasIdentical(c, "t", 0);
            assertArrayEquals(reference, c.dataLogBytes(victim, "t", 0), "rebuilt byte for byte from the Raft log");
        }
    }
}
