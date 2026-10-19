package com.ember.broker;

import com.ember.client.*;
import com.ember.protocol.ErrorCode;
import com.ember.protocol.ProducerRecord;
import com.ember.storage.LogRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;

import static com.ember.broker.PartitionTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

/** Restart, scale, retention and fsync behaviour of multi-partition topics. */
class PartitionedBrokerLifecycleTest {

    private static BrokerServer start(Path dir, Clock clock, String... extra) throws Exception {
        return ServerFixture.start(ServerFixture.config(dir, extra), clock);
    }

    private static byte[] b(String s) {
        return s.getBytes();
    }

    /** partition -> list of "key=value" in offset order: a comparable picture of a whole topic. */
    private static Map<Integer, List<String>> snapshot(int port, String topic) throws Exception {
        Map<Integer, List<String>> out = new TreeMap<>();
        for (var e : readAll(port, topic).entrySet()) {
            assertContiguous(e.getValue(), 0);
            List<String> lines = new ArrayList<>();
            for (LogRecord r : e.getValue()) {
                lines.add((r.key() == null ? "null" : new String(r.key())) + "=" + new String(r.value()));
            }
            out.put(e.getKey(), lines);
        }
        return out;
    }

    @Test
    void aFourPartitionTopicSurvivesARestartWithIdenticalContents(@TempDir Path dir) throws Exception {
        Map<Integer, List<String>> before;
        try (BrokerServer server = start(dir, Clock.systemUTC()); EmberClient admin = new EmberClient("localhost", server.port());
             EmberProducer producer = new EmberProducer("localhost", server.port())) {
            admin.createTopic("durable", 4);
            for (int i = 0; i < 400; i++) {
                producer.send("durable", i % 5 == 0 ? null : b("key-" + i % 13), b("value-" + i));
            }
            before = snapshot(server.port(), "durable");
            assertEquals(400, before.values().stream().mapToInt(List::size).sum());
        }
        try (BrokerServer server = start(dir, Clock.systemUTC(), "--default-partitions=9");
             EmberClient admin = new EmberClient("localhost", server.port())) {
            assertEquals(before, snapshot(server.port(), "durable"));          // counts and per-partition order identical
            assertEquals(4, admin.metadata("durable").topics().get(0).partitions().size());
            assertEquals(ErrorCode.TOPIC_ALREADY_EXISTS, assertThrows(EmberException.class, () -> admin.createTopic("durable", 4)).errorCode());
            // offsets continue where they stopped
            long next = admin.metadata("durable").topics().get(0).partitions().get(2).logEndOffset();
            assertEquals(next, admin.produce("durable", 2, List.of(new ProducerRecord(null, b("after")))));
        }
    }

    @Test
    void sixtyFourPartitionsFiftyRecordsEachSurviveARestart(@TempDir Path dir) throws Exception {
        int partitions = 64, perPartition = 50;
        try (BrokerServer server = start(dir, Clock.systemUTC()); EmberClient admin = new EmberClient("localhost", server.port())) {
            assertEquals(partitions, admin.createTopic("wide", partitions));
            for (int p = 0; p < partitions; p++) {
                List<ProducerRecord> records = new ArrayList<>();
                for (int i = 0; i < perPartition; i++) {
                    records.add(new ProducerRecord(null, b("p" + p + "-" + i)));
                }
                assertEquals(0, admin.produce("wide", p, records));
            }
        }
        try (BrokerServer server = start(dir, Clock.systemUTC())) {
            Map<Integer, List<LogRecord>> all = readAll(server.port(), "wide");
            assertEquals(partitions, all.size());
            for (int p = 0; p < partitions; p++) {
                assertEquals(perPartition, all.get(p).size(), "partition " + p);
                assertContiguous(all.get(p), 0);
                assertEquals("p" + p + "-49", new String(all.get(p).get(49).value()));
            }
        }
    }

    @Test
    void legacyPhase2DataDirectoriesAreRefusedWithAClearMessage(@TempDir Path dir) throws Exception {
        // Phase 5: old directories have no Raft log, so adopting them would invent history
        try (com.ember.storage.Log legacy = new com.ember.storage.Log(dir.resolve("orders-0"), 4096)) {
            legacy.append(1, b("k"), b("from phase 2"));
        }
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> start(dir, Clock.systemUTC()));
        assertTrue(e.getMessage().contains("legacy single-node data directory"), e.getMessage());
        assertTrue(e.getMessage().contains("fresh data directory"), e.getMessage());
        // and the lock was released again, so a fresh directory elsewhere (or this one, emptied) works
        try (var files = java.nio.file.Files.walk(dir.resolve("orders-0"))) {
            for (Path p : files.sorted(java.util.Comparator.reverseOrder()).toList()) java.nio.file.Files.delete(p);
        }
        try (BrokerServer server = start(dir, Clock.systemUTC())) {
            assertTrue(server.port() > 0);
        }
    }

    @Test
    void retentionRemovesOldSegmentsInEveryPartition(@TempDir Path dir) throws Exception {
        MutableClock clock = new MutableClock(1_000);
        try (BrokerServer server = start(dir, clock, "--retention-ms=10000"); EmberClient client = new EmberClient("localhost", server.port())) {
            client.createTopic("old", 3);
            for (int p = 0; p < 3; p++) {
                for (int i = 0; i < 20; i++) {
                    List<ProducerRecord> batch = new ArrayList<>();
                    for (int j = 0; j < 10; j++) batch.add(new ProducerRecord(null, new byte[100]));
                    client.produce("old", p, batch);
                }
            }
            clock.set(100_000);                                              // everything so far is now "old"
            for (int p = 0; p < 3; p++) {
                client.produce("old", p, List.of(new ProducerRecord(null, b("fresh"))));
            }
            server.logManager().runMaintenance(clock.millis());

            for (var partition : client.metadata("old").topics().get(0).partitions()) {
                long start = partition.logStartOffset();
                int p = partition.partition();
                assertTrue(start > 0 && start <= 200, "partition " + p + " logStartOffset=" + start);
                assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, assertThrows(EmberException.class, () -> client.fetch("old", p, 0, 1000)).errorCode());
                FetchResult live = client.fetch("old", p, start, 1_000_000);
                assertFalse(live.records().isEmpty());
                assertEquals(start, live.records().get(0).offset());
            }
        }
    }

    @Test
    void fsyncPerMessageOnFourPartitionsWithFourWritersLosesNothing(@TempDir Path dir) throws Exception {
        int writers = 4, perWriter = 60;
        try (BrokerServer server = start(dir, Clock.systemUTC(), "--flush-messages=1"); EmberClient admin = new EmberClient("localhost", server.port())) {
            admin.createTopic("sync", writers);
            ExecutorService pool = Executors.newFixedThreadPool(writers);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int w = 0; w < writers; w++) {
                final int partition = w;
                futures.add(pool.submit(() -> {
                    try (EmberClient client = new EmberClient("localhost", server.port())) {
                        go.await();
                        for (int i = 0; i < perWriter; i++) {
                            client.produce("sync", partition, List.of(new ProducerRecord(null, b(partition + "-" + i))));
                        }
                    }
                    return null;
                }));
            }
            go.countDown();
            for (Future<?> f : futures) {
                f.get(120, TimeUnit.SECONDS);
            }
            pool.shutdown();

            Map<Integer, List<LogRecord>> all = readAll(server.port(), "sync");
            for (int p = 0; p < writers; p++) {
                assertEquals(perWriter, all.get(p).size());
                assertContiguous(all.get(p), 0);
                for (int i = 0; i < perWriter; i++) {
                    assertEquals(p + "-" + i, new String(all.get(p).get(i).value()));
                }
                // every append was fsynced (flush-messages=1), so nothing is left unflushed
                assertEquals(0, server.logManager().logFor("sync", p).unflushedMessages());
            }
        }
    }

    @Test
    void v0MetadataAndPartitionZeroStillWorkThroughTheRealClientStack(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir, Clock.systemUTC(), "--default-partitions=3");
             EmberClient client = new EmberClient("localhost", server.port())) {
            assertEquals(0, client.produce("p2style", List.of(new ProducerRecord(null, b("x")))));    // partition 0
            assertEquals("x", new String(client.fetch("p2style", 0, 0, 1000).records().get(0).value()));
            assertEquals(3, client.metadata("p2style").topics().get(0).partitions().size());
        }
    }
}
