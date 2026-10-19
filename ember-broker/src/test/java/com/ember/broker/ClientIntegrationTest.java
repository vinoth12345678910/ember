package com.ember.broker;

import com.ember.client.EmberClient;
import com.ember.client.EmberException;
import com.ember.client.FetchResult;
import com.ember.protocol.ErrorCode;
import com.ember.protocol.MetadataResponse;
import com.ember.protocol.ProducerRecord;
import com.ember.storage.LogRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class ClientIntegrationTest {

    private static ProducerRecord rec(String k, String v) {
        return new ProducerRecord(k == null ? null : k.getBytes(), v.getBytes());
    }

    private static List<LogRecord> fetchAll(EmberClient client, String topic, long from, int maxBytes) throws Exception {
        List<LogRecord> all = new ArrayList<>();
        long next = from;
        while (true) {
            FetchResult r = client.fetch(topic, 0, next, maxBytes);
            if (r.records().isEmpty()) {
                return all;
            }
            all.addAll(r.records());
            next = r.nextFetchOffset();
        }
    }

    @Test
    void produceThenFetchIsByteExact(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
             EmberClient client = new EmberClient("localhost", server.port())) {
            long base = client.produce("t", List.of(rec("k1", "v1"), rec(null, "no key"), rec("k3", ""),
                    new ProducerRecord(new byte[]{0, (byte) 0xFF, 10}, new byte[]{(byte) 0x80, 0, 1})));
            assertEquals(0, base);
            List<LogRecord> got = fetchAll(client, "t", 0, 100_000);
            assertEquals(4, got.size());
            assertArrayEquals("k1".getBytes(), got.get(0).key());
            assertArrayEquals("v1".getBytes(), got.get(0).value());
            assertNull(got.get(1).key());
            assertEquals(0, got.get(2).value().length);
            assertArrayEquals(new byte[]{0, (byte) 0xFF, 10}, got.get(3).key());
            assertArrayEquals(new byte[]{(byte) 0x80, 0, 1}, got.get(3).value());
            assertTrue(got.get(0).timestamp() > 0);
        }
    }

    @Test
    void fetchFromTheMiddleAtTheEndOutOfRangeAndUnknownTopic(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
             EmberClient client = new EmberClient("localhost", server.port())) {
            client.produce("t", List.of(rec(null, "a"), rec(null, "b"), rec(null, "c")));
            FetchResult middle = client.fetch("t", 0, 1, 10_000);
            assertEquals(List.of(1L, 2L), middle.records().stream().map(LogRecord::offset).toList());
            assertEquals(3, middle.nextFetchOffset());

            FetchResult end = client.fetch("t", 0, 3, 10_000);
            assertTrue(end.records().isEmpty());
            assertEquals(3, end.highWatermark());

            assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, assertThrows(EmberException.class, () -> client.fetch("t", 0, 4, 100)).errorCode());
            assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, assertThrows(EmberException.class, () -> client.fetch("t", 0, -1, 100)).errorCode());
            assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, assertThrows(EmberException.class, () -> client.fetch("nope", 0, 0, 100)).errorCode());
            assertEquals(3, client.fetch("t", 0, 0, 10_000).records().size());      // still usable after errors
        }
    }

    @Test
    void aBadTopicNameIsRejectedByTheClientWithoutBreakingIt(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
             EmberClient client = new EmberClient("localhost", server.port())) {
            // the client's own codec refuses a bad name before it reaches the wire
            assertThrows(IllegalArgumentException.class, () -> client.produce("bad name", List.of(rec(null, "x"))));
            assertEquals(0, client.produce("good", List.of(rec(null, "x"))));      // nothing was sent, client still fine
        }
    }

    @Test
    void dataLargerThanOneSegmentComesBackExactlyOnceInOrder(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
             EmberClient client = new EmberClient("localhost", server.port())) {
            int total = 2000;
            for (int i = 0; i < total; i += 50) {
                List<ProducerRecord> batch = new ArrayList<>();
                for (int j = 0; j < 50; j++) {
                    batch.add(rec("k" + (i + j), "value-number-" + (i + j)));
                }
                assertEquals(i, client.produce("big", batch));
            }
            assertTrue(server.logManager().logFor("big", 0).segmentCount() > 10);
            List<LogRecord> all = fetchAll(client, "big", 0, 1_000_000);
            assertEquals(total, all.size());
            for (int i = 0; i < total; i++) {
                assertEquals(i, all.get(i).offset());
                assertEquals("value-number-" + i, new String(all.get(i).value()));
            }
        }
    }

    @Test
    void maxBytesIsRespectedAndTheFirstRecordAlwaysArrives(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir, "--segment-bytes=1000000"), Clock.systemUTC());
             EmberClient client = new EmberClient("localhost", server.port())) {
            byte[] value = new byte[100];
            List<ProducerRecord> batch = new ArrayList<>();
            for (int i = 0; i < 10; i++) batch.add(new ProducerRecord(null, value));   // 128 bytes each
            client.produce("t", batch);
            assertEquals(3, client.fetch("t", 0, 0, 400).records().size());
            assertEquals(1, client.fetch("t", 0, 0, 1).records().size());
            assertEquals(1, client.fetch("t", 0, 9, 1).records().size());
            // the broker's own cap (maxFetchBytes) also applies
        }
    }

    @Test
    void brokerCapsMaxBytesAtItsOwnLimit(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir, "--segment-bytes=1000000", "--max-fetch-bytes=500"), Clock.systemUTC());
             EmberClient client = new EmberClient("localhost", server.port())) {
            List<ProducerRecord> batch = new ArrayList<>();
            for (int i = 0; i < 10; i++) batch.add(new ProducerRecord(null, new byte[100]));
            client.produce("t", batch);
            assertEquals(3, client.fetch("t", 0, 0, 1_000_000).records().size());       // 3 * 128 = 384 <= 500 < 512
        }
    }

    @Test
    void eightConcurrentClientsGetDistinctContiguousBatches(@TempDir Path dir) throws Exception {
        int clients = 8, requests = 500, perRequest = 10;
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir, "--segment-bytes=65536"), Clock.systemUTC())) {
            ExecutorService pool = Executors.newFixedThreadPool(clients);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<long[]>> futures = new ArrayList<>();
            for (int c = 0; c < clients; c++) {
                final int id = c;
                futures.add(pool.submit(() -> {
                    long[] bases = new long[requests];
                    try (EmberClient client = new EmberClient("localhost", server.port())) {
                        go.await();
                        for (int r = 0; r < requests; r++) {
                            List<ProducerRecord> batch = new ArrayList<>();
                            for (int i = 0; i < perRequest; i++) {
                                batch.add(rec(null, id + "-" + r + "-" + i));
                            }
                            bases[r] = client.produce("shared", batch);
                        }
                    }
                    return bases;
                }));
            }
            go.countDown();
            Map<Long, String> owner = new HashMap<>();        // baseOffset -> "client-request"
            for (int c = 0; c < clients; c++) {
                long[] bases = futures.get(c).get(120, TimeUnit.SECONDS);
                for (int r = 0; r < requests; r++) {
                    assertNull(owner.put(bases[r], c + "-" + r), "two requests got baseOffset " + bases[r]);
                }
            }
            pool.shutdown();
            int total = clients * requests * perRequest;
            assertEquals(clients * requests, owner.size());

            try (EmberClient reader = new EmberClient("localhost", server.port())) {
                List<LogRecord> all = fetchAll(reader, "shared", 0, 1_000_000);
                assertEquals(total, all.size());
                for (int i = 0; i < total; i++) {
                    assertEquals(i, all.get(i).offset());     // every offset present exactly once
                }
                for (Map.Entry<Long, String> e : owner.entrySet()) {
                    for (int i = 0; i < perRequest; i++) {     // each request's records are contiguous and in order
                        assertEquals(e.getValue() + "-" + i, new String(all.get((int) (e.getKey() + i)).value()));
                    }
                }
            }
        }
    }

    @Test
    void dataSurvivesABrokerRestart(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
             EmberClient client = new EmberClient("localhost", server.port())) {
            for (int i = 0; i < 100; i++) {
                client.produce("a", List.of(rec("k" + i, "a-" + i)));
            }
            client.produce("b", List.of(rec(null, "only")));
        }
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
             EmberClient client = new EmberClient("localhost", server.port())) {
            List<LogRecord> a = fetchAll(client, "a", 0, 1_000_000);
            assertEquals(100, a.size());
            assertEquals("a-99", new String(a.get(99).value()));
            assertEquals("only", new String(fetchAll(client, "b", 0, 1000).get(0).value()));
            assertEquals(100, client.produce("a", List.of(rec(null, "after restart"))));   // offsets continue
            MetadataResponse meta = client.metadata();
            assertEquals(List.of("a", "b"), meta.topics().stream().map(MetadataResponse.TopicMetadata::topic).toList());
        }
    }

    @Test
    void retentionRemovesOldSegmentsAndFetchesReflectIt(@TempDir Path dir) throws Exception {
        MutableClock clock = new MutableClock(1_000);
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir, "--retention-ms=10000"), clock);
             EmberClient client = new EmberClient("localhost", server.port())) {
            for (int i = 0; i < 20; i++) {
                List<ProducerRecord> batch = new ArrayList<>();
                for (int j = 0; j < 10; j++) batch.add(new ProducerRecord(null, new byte[100]));
                client.produce("t", batch);
            }
            assertTrue(server.logManager().logFor("t", 0).segmentCount() > 3);

            clock.set(100_000);                                  // everything written so far is now "old"
            client.produce("t", List.of(rec(null, "fresh")));
            server.logManager().runMaintenance(clock.millis());

            long start = client.metadata("t").topics().get(0).partitions().get(0).logStartOffset();
            assertTrue(start > 0 && start <= 200, "logStartOffset=" + start);
            assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, assertThrows(EmberException.class, () -> client.fetch("t", 0, 0, 1000)).errorCode());
            FetchResult live = client.fetch("t", 0, start, 1_000_000);
            assertFalse(live.records().isEmpty());
            assertEquals(start, live.logStartOffset());
            assertEquals(start, live.records().get(0).offset());
            assertEquals("fresh", new String(fetchAll(client, "t", start, 1_000_000).getLast().value()));
        }
    }
}
