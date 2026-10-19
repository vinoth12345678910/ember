package com.ember.broker;

import com.ember.client.*;
import com.ember.protocol.ErrorCode;
import com.ember.protocol.MetadataResponse;
import com.ember.protocol.ProducerRecord;
import com.ember.storage.LogRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;

import static com.ember.broker.PartitionTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

class PartitionedProducerConsumerTest {

    private static BrokerServer start(Path dir, String... extra) throws Exception {
        return ServerFixture.start(ServerFixture.config(dir, extra), Clock.systemUTC());
    }

    private static byte[] b(String s) {
        return s.getBytes();
    }

    // ---- CREATE_TOPIC through the real client

    @Test
    void createTopicAndDuplicate(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir); EmberClient client = new EmberClient("localhost", server.port())) {
            assertEquals(3, client.createTopic("t", 3));
            EmberException e = assertThrows(EmberException.class, () -> client.createTopic("t", 3));
            assertEquals(ErrorCode.TOPIC_ALREADY_EXISTS, e.errorCode());
            assertEquals(ErrorCode.INVALID_REQUEST,
                    assertThrows(EmberException.class, () -> client.createTopic("u", 100_000)).errorCode());
            // Phase 5: a partition's leader is known once its own Raft election is over
            Await.until(10_000, "partition 2 to have a leader", () -> {
                try {
                    return client.metadata().topics().get(0).partitions().get(2).leaderNodeId() == 0;   // node 0 is the only broker
                } catch (IOException notYet) {
                    return false;
                }
            });
            assertEquals(3, client.metadata().topics().get(0).partitions().size());
        }
    }

    // ---- key routing

    @Test
    void theSameKeyAlwaysLandsInTheSamePartitionAndOffsetsAreContiguous(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir); EmberProducer producer = new EmberProducer("localhost", server.port())) {
            producer.send("k", b("warm-up"), b("x"));                       // auto-creates with 3 partitions
            Map<String, Integer> partitionOfKey = new HashMap<>();
            Map<Integer, List<Long>> offsetsByPartition = new HashMap<>();
            Map<String, RecordMetadata> first = new HashMap<>();
            for (int i = 0; i < 1000; i++) {
                String key = "user-" + (i % 20);
                RecordMetadata md = producer.send("k", b(key), b("v" + i));
                Integer known = partitionOfKey.putIfAbsent(key, md.partition());
                assertTrue(known == null || known == md.partition(), key + " moved from " + known + " to " + md.partition());
                offsetsByPartition.computeIfAbsent(md.partition(), p -> new ArrayList<>()).add(md.offset());
            }
            assertTrue(new HashSet<>(partitionOfKey.values()).size() > 1, "20 keys should not all hash to one partition");
            for (List<Long> offsets : offsetsByPartition.values()) {
                for (int i = 1; i < offsets.size(); i++) {
                    assertEquals(offsets.get(i - 1) + 1, offsets.get(i));        // strictly +1: nobody else was writing
                }
            }
        }
    }

    @Test
    void perKeyOrderingHoldsAcrossAllPartitions(@TempDir Path dir) throws Exception {
        int keys = 50, perKey = 200;
        try (BrokerServer server = start(dir); EmberClient admin = new EmberClient("localhost", server.port());
             EmberProducer producer = new EmberProducer("localhost", server.port())) {
            admin.createTopic("ordered", 4);
            for (int seq = 0; seq < perKey; seq++) {                         // keys interleave: k0#0 k1#0 ... k49#0 k0#1 ...
                for (int k = 0; k < keys; k++) {
                    producer.send("ordered", b("key-" + k), b("key-" + k + ":" + seq));
                }
            }
            Map<Integer, List<LogRecord>> all = readAll(server.port(), "ordered");
            assertEquals(keys * perKey, total(all));
            Map<String, Integer> nextSeq = new HashMap<>();
            Map<String, Integer> partitionOfKey = new HashMap<>();
            for (var entry : all.entrySet()) {
                assertContiguous(entry.getValue(), 0);
                for (LogRecord r : entry.getValue()) {
                    String[] parts = new String(r.value()).split(":");
                    int seq = Integer.parseInt(parts[1]);
                    assertEquals(nextSeq.getOrDefault(parts[0], 0), seq, "key " + parts[0] + " out of order");
                    nextSeq.put(parts[0], seq + 1);
                    Integer partition = partitionOfKey.putIfAbsent(parts[0], entry.getKey());
                    assertTrue(partition == null || partition.equals(entry.getKey()), "a key was split over partitions");
                }
            }
            assertEquals(keys, nextSeq.size());
            nextSeq.values().forEach(n -> assertEquals(perKey, n));          // every record exactly once
            assertTrue(all.values().stream().allMatch(l -> !l.isEmpty()), "every partition should hold some keys");
        }
    }

    // ---- batches

    @Test
    void sendBatchReturnsMetadataInInputOrderAndTheDataMatchesRecordByRecord(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir); EmberClient admin = new EmberClient("localhost", server.port());
             EmberProducer producer = new EmberProducer("localhost", server.port())) {
            admin.createTopic("batch", 4);
            List<ProducerRecord> batch = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                batch.add(new ProducerRecord(i % 3 == 0 ? null : b("key-" + i % 7), b("value-" + i)));
            }
            List<RecordMetadata> md = producer.sendBatch("batch", batch);
            assertEquals(100, md.size());

            Map<Integer, List<LogRecord>> all = readAll(server.port(), "batch");
            assertEquals(100, total(all));
            for (int i = 0; i < 100; i++) {
                RecordMetadata m = md.get(i);
                LogRecord stored = all.get(m.partition()).get((int) m.offset());
                assertEquals(m.offset(), stored.offset());
                assertEquals("value-" + i, new String(stored.value()), "input record " + i);
                assertArrayEquals(batch.get(i).key(), stored.key());
            }
            // inside each partition, input order is kept: offsets grow with the input index
            Map<Integer, Long> lastOffset = new HashMap<>();
            for (RecordMetadata m : md) {
                assertTrue(m.offset() > lastOffset.getOrDefault(m.partition(), -1L));
                lastOffset.put(m.partition(), m.offset());
            }
            assertTrue(producer.sendBatch("batch", List.of()).isEmpty());
        }
    }

    @Test
    void nullKeysSpreadRoundRobinOverAllPartitions(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir); EmberClient admin = new EmberClient("localhost", server.port());
             EmberProducer producer = new EmberProducer("localhost", server.port())) {
            admin.createTopic("rr", 4);
            int[] counts = new int[4];
            for (int i = 0; i < 40; i++) {
                RecordMetadata md = producer.send("rr", null, b("v" + i));
                assertEquals(i % 4, md.partition());
                counts[md.partition()]++;
            }
            assertArrayEquals(new int[]{10, 10, 10, 10}, counts);
        }
    }

    @Test
    void anInjectedCounterMakesNullKeyPlacementDeterministic(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir); EmberClient admin = new EmberClient("localhost", server.port());
             EmberClient client = new EmberClient("localhost", server.port());
             EmberProducer producer = new EmberProducer(client, ClientConfig.defaults(), () -> () -> 2)) {
            admin.createTopic("fixed", 3);
            for (int i = 0; i < 5; i++) {
                assertEquals(2, producer.send("fixed", null, b("v")).partition());
            }
        }
    }

    // ---- concurrency

    @Test
    void eightConcurrentProducersLoseAndDuplicateNothing(@TempDir Path dir) throws Exception {
        int threads = 8, batches = 150, perBatch = 10;
        try (BrokerServer server = start(dir, "--segment-bytes=65536"); EmberClient admin = new EmberClient("localhost", server.port())) {
            admin.createTopic("busy", 4);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int id = t;
                futures.add(pool.submit(() -> {
                    Random random = new Random(id);
                    try (EmberProducer producer = new EmberProducer("localhost", server.port())) {
                        go.await();
                        for (int bt = 0; bt < batches; bt++) {
                            List<ProducerRecord> records = new ArrayList<>();
                            for (int i = 0; i < perBatch; i++) {
                                byte[] key = new byte[8];
                                random.nextBytes(key);
                                records.add(new ProducerRecord(key, b(id + "-" + bt + "-" + i)));
                            }
                            producer.sendBatch("busy", records);
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

            Map<Integer, List<LogRecord>> all = readAll(server.port(), "busy");
            assertEquals(threads * batches * perBatch, total(all));
            Set<String> seen = new HashSet<>();
            for (List<LogRecord> partition : all.values()) {
                assertContiguous(partition, 0);
                for (LogRecord r : partition) {
                    assertTrue(seen.add(new String(r.value())), "duplicate " + new String(r.value()));
                }
            }
            assertEquals(threads * batches * perBatch, seen.size());
        }
    }

    // ---- metadata cache

    @Test
    void aProducerWithAnEmptyCacheCreatesTheTopic(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir);
             EmberClient c1 = new EmberClient("localhost", server.port());
             EmberProducer producer = new EmberProducer(c1, ClientConfig.defaults().withAutoCreate(true, 5));
             EmberClient reader = new EmberClient("localhost", server.port())) {
            assertTrue(reader.metadata("fresh").topics().isEmpty());
            producer.send("fresh", b("k"), b("v"));
            assertEquals(5, reader.metadata("fresh").topics().get(0).partitions().size());
            assertEquals(5, producer.partitionCount("fresh"));
        }
    }

    @Test
    void twoProducersRacingToCreateTheSameTopicBothSucceed(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir)) {
            ExecutorService pool = Executors.newFixedThreadPool(8);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                final int id = i;
                futures.add(pool.submit(() -> {
                    try (EmberProducer p = new EmberProducer("localhost", server.port(), ClientConfig.defaults().withAutoCreate(true, 3))) {
                        go.await();
                        return p.send("contended", b("key-" + id), b("v"));
                    }
                }));
            }
            go.countDown();
            for (Future<RecordMetadata> f : futures) {
                assertNotNull(f.get(30, TimeUnit.SECONDS));
            }
            pool.shutdown();
            assertEquals(3, server.logManager().topic("contended").partitionCount());
        }
    }

    @Test
    void autoCreateOffMeansAnUnknownTopicFails(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir);
             EmberProducer producer = new EmberProducer("localhost", server.port(), ClientConfig.defaults().withAutoCreate(false, 3))) {
            assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                    assertThrows(EmberException.class, () -> producer.send("nope", null, b("v"))).errorCode());
            assertNull(server.logManager().topic("nope"));
        }
    }

    /** A client whose first metadata answer claims MORE partitions than the topic really has. */
    private static final class LyingClient extends EmberClient {
        int metadataCalls = 0;

        LyingClient(int port) throws IOException {
            super("localhost", port);
        }

        @Override
        public synchronized MetadataResponse metadata(String... topics) throws IOException {
            metadataCalls++;
            MetadataResponse real = super.metadata(topics);
            if (metadataCalls > 1 || real.topics().isEmpty()) {
                return real;
            }
            var t = real.topics().get(0);
            List<MetadataResponse.PartitionMetadata> fake = new ArrayList<>(t.partitions());
            for (int p = fake.size(); p < 6; p++) {
                fake.add(new MetadataResponse.PartitionMetadata(p, 0, 0, 0));
            }
            return new MetadataResponse(real.broker(), List.of(new MetadataResponse.TopicMetadata(t.topic(), fake)));
        }
    }

    @Test
    void anInvalidPartitionMakesTheProducerRefreshOnceAndThenFail(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir); EmberClient admin = new EmberClient("localhost", server.port());
             LyingClient lying = new LyingClient(server.port())) {
            admin.createTopic("t", 3);
            int[] counter = {5};                                            // the round-robin counter picks partition 5
            EmberProducer producer = new EmberProducer(lying, ClientConfig.defaults(), () -> () -> counter[0]);
            EmberException e = assertThrows(EmberException.class, () -> producer.send("t", null, b("v")));
            assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, e.errorCode());
            assertEquals(2, lying.metadataCalls, "one lookup, then exactly one refresh");
            assertEquals(3, producer.partitionCount("t"));                  // the refresh fixed the cache
            counter[0] = 1;
            assertEquals(1, producer.send("t", null, b("works now")).partition());
            for (var p : admin.metadata("t").topics().get(0).partitions()) {
                assertEquals(p.partition() == 1 ? 1 : 0, p.logEndOffset());  // the failed send stored nothing
            }
        }
    }

    // ---- the consumer

    @Test
    void assignAllFromTheBeginningPollsUntilCaughtUp(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir); EmberClient admin = new EmberClient("localhost", server.port())) {
            admin.createTopic("c", 3);
            for (int p = 0; p < 3; p++) {
                List<ProducerRecord> recs = new ArrayList<>();
                for (int i = 0; i < 20; i++) recs.add(new ProducerRecord(null, b("p" + p + "-" + i)));
                admin.produce("c", p, recs);
            }
            Map<Integer, List<LogRecord>> all = readAll(server.port(), "c");
            assertEquals(60, total(all));
            for (int p = 0; p < 3; p++) {
                assertContiguous(all.get(p), 0);
                assertEquals("p" + p + "-19", new String(all.get(p).get(19).value()));
            }
            try (EmberConsumer consumer = new EmberConsumer("localhost", server.port())) {
                consumer.assignAll("c", EmberConsumer.StartPosition.BEGINNING);
                assertEquals(3, consumer.assignment().size());
                while (!consumer.poll(1000).isEmpty()) {
                    // keep going until caught up
                }
                for (int p = 0; p < 3; p++) {
                    assertEquals(20, consumer.position("c", p));
                }
                assertTrue(consumer.poll(1000).isEmpty());
            }
        }
    }

    @Test
    void aPollServesEveryPartitionOnceAndTheStartingPartitionRotates(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir); EmberClient admin = new EmberClient("localhost", server.port())) {
            admin.createTopic("rot", 3);
            for (int p = 0; p < 3; p++) {
                admin.produce("rot", p, List.of(new ProducerRecord(null, b("x"))));
            }
            try (EmberConsumer consumer = new EmberConsumer("localhost", server.port())) {
                consumer.assignAll("rot", EmberConsumer.StartPosition.BEGINNING);
                List<Integer> first = consumer.poll(1000).stream().map(ConsumedRecord::partition).toList();
                assertEquals(List.of(0, 1, 2), first);
                consumer.seek("rot", 0, 0);
                consumer.seek("rot", 1, 0);
                consumer.seek("rot", 2, 0);
                List<Integer> second = consumer.poll(1000).stream().map(ConsumedRecord::partition).toList();
                assertEquals(List.of(1, 2, 0), second);                      // starts one partition later
            }
        }
    }

    @Test
    void seekMovesThePositionAndLatestSkipsOldData(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir); EmberClient admin = new EmberClient("localhost", server.port())) {
            admin.createTopic("s", 1);
            List<ProducerRecord> recs = new ArrayList<>();
            for (int i = 0; i < 10; i++) recs.add(new ProducerRecord(null, b("old-" + i)));
            admin.produce("s", 0, recs);

            try (EmberConsumer consumer = new EmberConsumer("localhost", server.port())) {
                consumer.assign("s", 0, 0);
                consumer.seek("s", 0, 7);
                assertEquals(7, consumer.position("s", 0));
                List<ConsumedRecord> polled = consumer.poll(10_000);
                assertEquals(List.of(7L, 8L, 9L), polled.stream().map(r -> r.record().offset()).toList());
                assertEquals(10, consumer.position("s", 0));
                assertThrows(IllegalStateException.class, () -> consumer.seek("s", 5, 0));
                assertThrows(IllegalStateException.class, () -> consumer.position("zzz", 0));
            }
            try (EmberConsumer latest = new EmberConsumer("localhost", server.port())) {
                latest.assignAll("s", EmberConsumer.StartPosition.LATEST);
                assertEquals(10, latest.position("s", 0));
                assertTrue(latest.poll(10_000).isEmpty());
                admin.produce("s", 0, List.of(new ProducerRecord(null, b("new"))));
                List<ConsumedRecord> fresh = latest.poll(10_000);
                assertEquals(1, fresh.size());
                assertEquals("new", new String(fresh.get(0).record().value()));
            }
        }
    }

    @Test
    void offsetOutOfRangeSurfacesAsAnException(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir); EmberClient admin = new EmberClient("localhost", server.port())) {
            admin.createTopic("o", 2);
            admin.produce("o", 0, List.of(new ProducerRecord(null, b("x"))));
            try (EmberConsumer consumer = new EmberConsumer("localhost", server.port())) {
                consumer.assign("o", 0, 999);
                assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, assertThrows(EmberException.class, () -> consumer.poll(1000)).errorCode());
            }
            try (EmberConsumer consumer = new EmberConsumer("localhost", server.port())) {
                assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                        assertThrows(EmberException.class, () -> consumer.assignAll("missing", EmberConsumer.StartPosition.BEGINNING)).errorCode());
                consumer.assign("o", 7, 0);                                  // no such partition
                assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, assertThrows(EmberException.class, () -> consumer.poll(1000)).errorCode());
            }
            try (EmberConsumer empty = new EmberConsumer("localhost", server.port())) {
                assertTrue(empty.poll(1000).isEmpty());                      // nothing assigned: nothing to do
            }
        }
    }

    @Test
    void aFailedPollMovesNoPositionSoNoRecordIsSkipped(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir); EmberClient admin = new EmberClient("localhost", server.port())) {
            admin.createTopic("atomic", 2);
            admin.produce("atomic", 0, List.of(new ProducerRecord(null, b("a")), new ProducerRecord(null, b("b"))));
            try (EmberConsumer consumer = new EmberConsumer("localhost", server.port())) {
                consumer.assign("atomic", 0, 0);
                consumer.assign("atomic", 1, 50);                           // out of range: this fetch fails
                assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, assertThrows(EmberException.class, () -> consumer.poll(1000)).errorCode());
                assertEquals(0, consumer.position("atomic", 0));            // partition 0's records were NOT consumed
                consumer.seek("atomic", 1, 0);
                List<ConsumedRecord> ok = consumer.poll(1000);
                assertEquals(List.of("a", "b"), ok.stream().map(r -> new String(r.record().value())).toList());
            }
        }
    }
}
