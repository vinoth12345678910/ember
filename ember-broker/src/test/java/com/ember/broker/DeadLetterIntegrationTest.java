package com.ember.broker;

import com.ember.client.*;
import com.ember.protocol.ProducerRecord;
import com.ember.protocol.QueueDeadListResponse;
import com.ember.protocol.QueueDescribeResponse;
import com.ember.protocol.QueueMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/** Poison messages on real brokers: they are retried with backoff, dead-lettered, never block the rest, and can be retried or discarded. */
class DeadLetterIntegrationTest {

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

    private static boolean allFinishedExcept(QueueDescribeResponse d, int expectedDead) {
        int dead = 0, open = 0;
        for (QueueDescribeResponse.PartitionState p : d.partitions()) {
            dead += p.dead();
            open += p.inFlight() + p.redeliver();
            if (p.next() < p.logEnd()) return false;
        }
        return dead == expectedDead && open == 0;
    }

    @Test
    void poisonMessagesAreRetriedThenDeadLetteredWithoutBlockingOthersAndCanBeRetriedAfterTheFix(@TempDir Path dir) throws Exception {
        int total = 100;
        Set<String> poison = Set.of("m13", "m57");
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            c.awaitController();
            ClusterIntegrationTest.createTopic(c, "t", 3);
            produce(c, "t", total);
            ClientConfig cfg = ClusterIntegrationTest.cfg(c);
            QueueConsumer.createSubscription(cfg, "sub", "t", true, 5_000, 3, 100, 200);

            Set<String> acked = ConcurrentHashMap.newKeySet();
            Map<String, Set<Integer>> poisonAttempts = new ConcurrentHashMap<>();
            AtomicBoolean fixed = new AtomicBoolean(false);        // flips when "the bug is fixed": then poison messages are processed fine
            AtomicBoolean stop = new AtomicBoolean(false);
            ExecutorService pool = Executors.newFixedThreadPool(4);
            List<Future<?>> workers = new ArrayList<>();
            for (int w = 0; w < 4; w++) {
                String id = "w" + w;
                workers.add(pool.submit(() -> {
                    try (QueueConsumer q = new QueueConsumer(cfg, "sub", id)) {
                        while (!stop.get()) {
                            List<QueueMessage> got = q.claim(10);
                            if (got.isEmpty()) {
                                Thread.sleep(20);
                                continue;
                            }
                            List<QueueMessage> good = new ArrayList<>(), bad = new ArrayList<>();
                            for (QueueMessage m : got) {
                                String v = s(m.value());
                                if (poison.contains(v) && !fixed.get()) {
                                    poisonAttempts.computeIfAbsent(v, k -> ConcurrentHashMap.newKeySet()).add(m.attempt());
                                    bad.add(m);
                                } else {
                                    good.add(m);
                                }
                            }
                            if (!bad.isEmpty()) q.nack(bad, "cannot parse payload");
                            if (!good.isEmpty()) {
                                q.ack(good);
                                good.forEach(m -> acked.add(s(m.value())));
                            }
                        }
                    }
                    return null;
                }));
            }

            try (QueueConsumer admin = new QueueConsumer(cfg, "sub", "admin")) {
                Await.until(40_000, "98 good messages acked and the 2 poison ones dead\\n" + acked.size(), () -> {
                    try {
                        return acked.size() == total - poison.size() && admin.deadLetters(10).total() == 2;
                    } catch (Exception e) {
                        return false;
                    }
                });
                assertEquals(Set.of("m13", "m57"), poisonAttempts.keySet());
                for (Set<Integer> attempts : poisonAttempts.values()) {
                    assertEquals(Set.of(1, 2, 3), attempts, "tried exactly 3 times (the policy), each as a new attempt number");
                }
                assertFalse(acked.stream().anyMatch(poison::contains), "poison messages were never acked");

                QueueDeadListResponse dead = admin.deadLetters(10);
                assertEquals(2, dead.total());
                assertEquals(poison, new HashSet<>(dead.items().stream().map(m -> s(m.value())).toList()), "the dead list carries the original content");
                assertTrue(dead.items().stream().allMatch(m -> m.failures() == 3 && m.reason().equals("cannot parse payload")));
                QueueDescribeResponse d = admin.describe();
                assertEquals(3, d.maxAttempts());
                assertEquals(2, d.partitions().stream().mapToInt(QueueDescribeResponse.PartitionState::dead).sum());
                assertTrue(allFinishedExcept(d, 2));

                // the dead set is replicated: it survives losing the "__acks" leader
                int leader = -1;
                for (int i = 1; i <= 3; i++) if (c.isUp(i) && c.servers[i].replicaManager().acksGroup().isLeaderReady()) leader = i;
                assertTrue(leader > 0);
                c.crash(leader);
                Await.until(30_000, "dead letters visible on the new leader", () -> {
                    try {
                        return admin.deadLetters(10).total() == 2;
                    } catch (Exception e) {
                        return false;
                    }
                });

                // "the bug is fixed": retry them, they go through
                fixed.set(true);
                assertEquals(2, admin.retryDead(admin.deadLetters(10).items()).settled());
                Await.until(30_000, "the retried messages to be acked", () -> acked.size() == total);
                stop.set(true);
                for (Future<?> f : workers) f.get(10, TimeUnit.SECONDS);
                pool.shutdown();

                QueueDescribeResponse end = admin.describe();
                for (QueueDescribeResponse.PartitionState p : end.partitions()) {
                    assertEquals(p.logEnd(), p.floor(), "partition " + p.partition() + " fully finished");
                    assertEquals(0, p.dead());
                }
            }
        }
    }

    @Test
    void discardingADeadMessageFinishesItForGood(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            c.awaitController();
            ClusterIntegrationTest.createTopic(c, "t", 1);
            produce(c, "t", 5);
            ClientConfig cfg = ClusterIntegrationTest.cfg(c);
            QueueConsumer.createSubscription(cfg, "sub", "t", true, 5_000, 2, 50, 50);
            try (QueueConsumer q = new QueueConsumer(cfg, "sub", "w")) {
                int dead = 0;
                long deadline = System.nanoTime() + 30_000_000_000L;
                Set<String> acked = new HashSet<>();
                while ((dead < 1 || acked.size() < 4) && System.nanoTime() < deadline) {
                    List<QueueMessage> got = q.claim(10);
                    if (got.isEmpty()) {
                        Thread.sleep(30);
                        dead = q.deadLetters(5).total();
                        continue;
                    }
                    List<QueueMessage> bad = got.stream().filter(m -> s(m.value()).equals("m2")).toList();
                    List<QueueMessage> good = got.stream().filter(m -> !s(m.value()).equals("m2")).toList();
                    if (!bad.isEmpty()) q.nack(bad, "bad");
                    if (!good.isEmpty()) {
                        q.ack(good);
                        good.forEach(m -> acked.add(s(m.value())));
                    }
                    dead = q.deadLetters(5).total();
                }
                assertEquals(1, q.deadLetters(5).total());
                assertEquals(2, q.describe().partitions().get(0).floor(), "the dead message holds the floor at its offset");
                assertEquals(1, q.discardDead(q.deadLetters(5).items()).settled());
                assertEquals(0, q.deadLetters(5).total());
                QueueDescribeResponse.PartitionState p = q.describe().partitions().get(0);
                assertEquals(5, p.floor(), "discarded: everything is finished");
                assertEquals(0, q.discardDead(List.of(new QueueDeadListResponse.DeadMessage(0, 2, 0, 0, 0, "", null, new byte[0]))).settled(),
                        "discarding twice finds nothing");
                assertTrue(q.claim(10).isEmpty());
            }
        }
    }
}
