package com.ember.broker;

import com.ember.client.*;
import com.ember.protocol.ProducerRecord;
import com.ember.storage.LogRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/** The pipelined producer and the batching producer on top of it, against real brokers. */
class PipelinedProducerTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String s(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Test
    void aPipelinedBatchLandsEverywhereInOrderAndMetadataMatchesInputOrder(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            c.awaitController();
            ClusterIntegrationTest.createTopic(c, "t", 6);
            try (EmberProducer p = EmberProducer.pipelined(ClusterIntegrationTest.cfg(c), 6)) {
                List<ProducerRecord> batch = new ArrayList<>();
                for (int i = 0; i < 600; i++) batch.add(new ProducerRecord(b("k" + (i % 30)), b("v" + i)));
                List<RecordMetadata> md = p.sendBatch("t", batch);
                assertEquals(600, md.size());
                Map<Integer, List<LogRecord>> log = ClusterIntegrationTest.readAll(c, "t");
                // the metadata of record i must point at record i
                for (int i = 0; i < 600; i++) {
                    LogRecord at = log.get(md.get(i).partition()).get((int) md.get(i).offset());
                    assertEquals("v" + i, s(at.value()), "metadata for record " + i);
                }
                assertTrue(md.stream().map(RecordMetadata::partition).distinct().count() > 1, "should spread over partitions");
            }
        }
    }

    @Test
    void manyThreadsSharingABatchingProducerLoseNothingAndKeepPerKeyOrder(@TempDir Path dir) throws Exception {
        int threads = 8, perThread = 400;
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            c.awaitController();
            ClusterIntegrationTest.createTopic(c, "t", 6);
            try (BatchingProducer bp = new BatchingProducer(ClusterIntegrationTest.cfg(c), BatchingProducer.Settings.defaults())) {
                ExecutorService pool = Executors.newFixedThreadPool(threads);
                List<Future<List<CompletableFuture<RecordMetadata>>>> all = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    int thread = t;
                    all.add(pool.submit(() -> {
                        List<CompletableFuture<RecordMetadata>> fs = new ArrayList<>();
                        for (int i = 0; i < perThread; i++) {         // one key per thread: its records must stay in call order
                            fs.add(bp.send("t", b("key-" + thread), b(thread + ":" + i)));
                        }
                        return fs;
                    }));
                }
                for (var f : all) for (var cf : f.get()) cf.get(30, TimeUnit.SECONDS);
                pool.shutdown();

                BatchingProducer.Stats st = bp.stats();
                assertEquals(threads * perThread, st.acknowledged());
                assertEquals(0, st.failed());
                assertTrue(st.averageBatch() > 1, "records should have been batched, average " + st.averageBatch());

                Map<Integer, List<LogRecord>> log = ClusterIntegrationTest.readAll(c, "t");
                Map<String, Integer> next = new HashMap<>();
                int total = 0;
                for (List<LogRecord> part : log.values()) {
                    for (LogRecord r : part) {
                        total++;
                        String[] v = s(r.value()).split(":");
                        int expected = next.merge(v[0], 1, Integer::sum) - 1;
                        assertEquals(expected, Integer.parseInt(v[1]), "key of thread " + v[0] + " out of order");
                    }
                }
                assertEquals(threads * perThread, total);
            }
        }
    }

    @Test
    void sendsToAMissingTopicFailTheirFuturesInsteadOfHanging(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            c.awaitController();
            try (BatchingProducer bp = new BatchingProducer(ClusterIntegrationTest.cfg(c), BatchingProducer.Settings.defaults())) {
                var f = bp.send("nope", b("k"), b("v"));
                ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(20, TimeUnit.SECONDS));
                assertInstanceOf(EmberException.class, e.getCause());
                assertEquals(1, bp.stats().failed());
            }
        }
    }

    @Test
    void aFullQueueRejectsInsteadOfGrowingWithoutLimit() throws Exception {
        ClientConfig dead = ClientConfig.defaults().withBootstrap(List.of("localhost:1")).withRetries(3, 150, 300).withTimeouts(200, 200);
        BatchingProducer.Settings tiny = new BatchingProducer.Settings(1, 10, 1 << 20, 0, 2, 0);
        try (BatchingProducer bp = new BatchingProducer(dead, tiny)) {
            List<CompletableFuture<RecordMetadata>> fs = new ArrayList<>();
            for (int i = 0; i < 20; i++) fs.add(bp.send("t", null, b("v" + i)));
            long overloaded = fs.stream().filter(f -> f.isCompletedExceptionally()).filter(f -> {
                try {
                    f.get();
                    return false;
                } catch (Exception e) {
                    return e.getCause() instanceof ProducerOverloadedException;
                }
            }).count();
            assertTrue(overloaded >= 10, "most of 20 sends into a 2-slot queue behind a dead cluster must be rejected, got " + overloaded);
            assertEquals(overloaded, bp.stats().rejected());
        }
    }
}
