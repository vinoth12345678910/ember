package com.ember.broker;

import com.ember.client.*;
import com.ember.protocol.ErrorCode;
import com.ember.protocol.ProducerRecord;
import com.ember.protocol.QueueDescribeResponse;
import com.ember.protocol.QueueMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Ack queues on real brokers: many workers per partition, crashed workers, fencing, and failover of the "__acks" leader. */
class AckQueueIntegrationTest {

    private static String s(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    private static void produce(TestCluster c, String topic, int count) throws Exception {
        try (EmberProducer p = new EmberProducer(ClusterIntegrationTest.cfg(c))) {
            List<ProducerRecord> batch = new ArrayList<>();
            for (int i = 0; i < count; i++) batch.add(new ProducerRecord(("k" + i).getBytes(), ("m" + i).getBytes()));
            p.sendBatch(topic, batch);
        }
    }

    /** A claim serves only what the "__acks" leader has applied from the partition, so right after a produce it may return fewer: collect until n. */
    private static List<QueueMessage> claimAtLeast(QueueConsumer q, int n) throws Exception {
        List<QueueMessage> out = new ArrayList<>();
        long deadline = System.nanoTime() + 20_000_000_000L;
        while (out.size() < n && System.nanoTime() < deadline) {
            List<QueueMessage> got = q.claim(n - out.size());
            out.addAll(got);
            if (got.isEmpty()) Thread.sleep(20);
        }
        return out;
    }

    private static ClientConfig fast(TestCluster c) {
        return ClusterIntegrationTest.cfg(c);
    }

    @Test
    void sixWorkersOnThreePartitionsGetEveryMessageExactlyOnceWhenNobodyFails(@TempDir Path dir) throws Exception {
        int total = 600;
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            c.awaitController();
            ClusterIntegrationTest.createTopic(c, "t", 3);
            produce(c, "t", total);
            assertEquals(3, QueueConsumer.createSubscription(fast(c), "sub", "t", true, 30_000));
            assertEquals(ErrorCode.SUBSCRIPTION_ALREADY_EXISTS,
                    assertThrows(EmberException.class, () -> QueueConsumer.createSubscription(fast(c), "sub", "t", true, 0)).errorCode());

            Map<String, Integer> seen = new ConcurrentHashMap<>();
            Map<String, Integer> byWorker = new ConcurrentHashMap<>();
            AtomicInteger acked = new AtomicInteger();
            ExecutorService pool = Executors.newFixedThreadPool(6);
            List<Future<?>> futures = new ArrayList<>();
            for (int w = 0; w < 6; w++) {
                String id = "worker-" + w;
                futures.add(pool.submit(() -> {
                    try (QueueConsumer q = new QueueConsumer(fast(c), "sub", id)) {
                        long deadline = System.nanoTime() + 60_000_000_000L;
                        while (acked.get() < total && System.nanoTime() < deadline) {
                            List<QueueMessage> got = q.claim(25);
                            if (got.isEmpty()) {
                                Thread.sleep(20);
                                continue;
                            }
                            for (QueueMessage m : got) {
                                seen.merge(s(m.value()), 1, Integer::sum);
                                byWorker.merge(id, 1, Integer::sum);
                            }
                            var r = q.ack(got);
                            assertEquals(got.size(), r.settled());
                            acked.addAndGet(got.size());
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> f : futures) f.get(90, TimeUnit.SECONDS);
            pool.shutdown();

            assertEquals(total, seen.size(), "every message delivered");
            assertTrue(seen.values().stream().allMatch(n -> n == 1), "no duplicates without failures");
            assertTrue(byWorker.size() >= 4, "more workers than partitions all did work: " + byWorker);

            try (QueueConsumer q = new QueueConsumer(fast(c), "sub", "observer")) {
                QueueDescribeResponse d = q.describe();
                for (QueueDescribeResponse.PartitionState p : d.partitions()) {
                    assertEquals(p.logEnd(), p.floor(), "everything finished in partition " + p.partition());
                    assertEquals(0, p.inFlight());
                    assertEquals(0, p.redeliver());
                }
                assertTrue(q.claim(10).isEmpty());
            }
        }
    }

    @Test
    void aWorkerThatNeverAcksLosesItsMessagesAndItsLateAckIsRejected(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            c.awaitController();
            ClusterIntegrationTest.createTopic(c, "t", 3);
            produce(c, "t", 30);
            QueueConsumer.createSubscription(fast(c), "sub", "t", true, 600);
            try (QueueConsumer w1 = new QueueConsumer(fast(c), "sub", "w1"); QueueConsumer w2 = new QueueConsumer(fast(c), "sub", "w2")) {
                List<QueueMessage> lost = claimAtLeast(w1, 30);
                assertEquals(30, lost.size());
                assertTrue(w2.claim(100).isEmpty(), "w1 holds everything while its lease is valid");

                List<QueueMessage> again = new ArrayList<>();
                long deadline = System.nanoTime() + 15_000_000_000L;
                while (again.size() < 30 && System.nanoTime() < deadline) {
                    again.addAll(w2.claim(100));
                    Thread.sleep(50);
                }
                assertEquals(30, again.size(), "after the lease ran out all 30 went to w2");
                assertTrue(again.stream().allMatch(m -> m.attempt() == 2));

                var late = w1.ack(lost);
                assertEquals(0, late.settled());
                assertEquals(30, late.rejected(), "w1 was fenced");
                assertEquals(30, w2.ack(again).settled());
                assertEquals(0, w2.ack(again).rejected(), "acking twice is harmless");
                assertTrue(w1.claim(10).isEmpty());
            }
        }
    }

    @Test
    void afterTheAcksLeaderDiesNothingAckedComesBackAndNothingIsLost(@TempDir Path dir) throws Exception {
        int total = 200;
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            c.awaitController();
            ClusterIntegrationTest.createTopic(c, "t", 3);
            produce(c, "t", total);
            QueueConsumer.createSubscription(fast(c), "sub", "t", true, 1500);

            Set<String> ackedBefore = new HashSet<>();
            try (QueueConsumer w = new QueueConsumer(fast(c), "sub", "w1")) {
                List<QueueMessage> got = claimAtLeast(w, 100);
                assertEquals(100, got.size());
                List<QueueMessage> first50 = got.subList(0, 50);
                assertEquals(50, w.ack(first50).settled());
                first50.forEach(m -> ackedBefore.add(s(m.value())));
                // the other 50 stay claimed and unacked: their leases must survive the failover and then expire
            }

            int leader = -1;
            for (int i = 1; i <= 3; i++) if (c.isUp(i) && c.servers[i].replicaManager().acksGroup().isLeaderReady()) leader = i;
            assertTrue(leader > 0, "an __acks leader");
            c.crash(leader);

            Set<String> ackedAfter = new HashSet<>();
            Set<String> redelivered = new HashSet<>();
            try (QueueConsumer w = new QueueConsumer(fast(c), "sub", "w2")) {
                long deadline = System.nanoTime() + 60_000_000_000L;
                while (ackedBefore.size() + ackedAfter.size() < total && System.nanoTime() < deadline) {
                    List<QueueMessage> got = w.claim(50);
                    if (got.isEmpty()) {
                        Thread.sleep(100);
                        continue;
                    }
                    for (QueueMessage m : got) {
                        assertFalse(ackedBefore.contains(s(m.value())), "an acked message came back: " + s(m.value()));
                        if (m.attempt() > 1) redelivered.add(s(m.value()));
                    }
                    w.ack(got);
                    got.forEach(m -> ackedAfter.add(s(m.value())));
                }
            }
            Set<String> all = new HashSet<>(ackedBefore);
            all.addAll(ackedAfter);
            assertEquals(total, all.size(), "every message was acked exactly once overall (nothing lost)");
            assertTrue(redelivered.size() >= 50, "the 50 unacked in-flight messages were redelivered, got " + redelivered.size());
        }
    }
}
