package com.ember.broker;

import com.ember.client.*;
import com.ember.storage.LogRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Random single failures under continuous production; afterwards every acknowledged record must be present, in order,
 * and all replicas identical. -Dcluster.seed=N picks the schedule (0 = a fresh random seed, printed).
 */
class ClusterChaosTest {

    private static final int PARTITIONS = 3;
    private static final int KEYS_PER_PRODUCER = 6;

    private record Ack(String key, long seq, String value) {
    }

    private static final class ProducerLoop implements Runnable {
        final int id;
        final TestCluster cluster;
        final AtomicBoolean stop;
        final List<Ack> acked = new CopyOnWriteArrayList<>();
        final AtomicInteger failedSends = new AtomicInteger();
        volatile Throwable crashed;
        volatile long retried;

        ProducerLoop(int id, TestCluster cluster, AtomicBoolean stop) {
            this.id = id;
            this.cluster = cluster;
            this.stop = stop;
        }

        @Override
        public void run() {
            try (EmberProducer producer = new EmberProducer(ClusterIntegrationTest.cfg(cluster).withRetries(15, 50, 300).withTimeouts(2_000, 9_000))) {
                long seq = 0;
                while (!stop.get()) {
                    String key = "p" + id + "-k" + (seq % KEYS_PER_PRODUCER);
                    String value = key + ":" + seq;
                    try {
                        producer.send("chaos", key.getBytes(), value.getBytes());
                        acked.add(new Ack(key, seq, value));
                    } catch (EmberException | IOException e) {
                        failedSends.incrementAndGet();            // not acknowledged: it may or may not exist
                    }
                    seq++;
                }
                retried = producer.retriedRequests();
            } catch (Throwable t) {
                crashed = t;
            }
        }
    }

    /** At most one broker is unavailable at any moment (down or isolated) so a majority always exists. */
    private static void chaos(TestCluster c, long seed, long durationMs, List<String> events) {
        Random rnd = new Random(seed);
        long end = System.nanoTime() + durationMs * 1_000_000L;
        int down = -1;
        int isolated = -1;
        int[] link = null;
        while (System.nanoTime() < end) {
            LockSupport.parkNanos((500 + rnd.nextInt(1000)) * 1_000_000L);
            try {
                if (down > 0) {
                    if (rnd.nextInt(10) < 7) {
                        events.add("restart " + down);
                        c.restart(down);
                        down = -1;
                    }
                } else if (isolated > 0) {
                    if (rnd.nextInt(10) < 6) {
                        events.add("heal isolation of " + isolated);
                        c.heal();
                        isolated = -1;
                    }
                } else if (link != null) {
                    events.add("unblock link " + link[0] + "<->" + link[1]);
                    c.servers[link[0]].unblockPeer(link[1]);
                    c.servers[link[1]].unblockPeer(link[0]);
                    link = null;
                } else {
                    int victim = 1 + rnd.nextInt(c.n);
                    switch (rnd.nextInt(4)) {
                        case 0, 1 -> {
                            events.add("crash " + victim);
                            c.crash(victim);
                            down = victim;
                        }
                        case 2 -> {
                            events.add("isolate " + victim);
                            c.isolate(victim);
                            isolated = victim;
                        }
                        default -> {
                            int other = 1 + (victim % c.n);
                            events.add("block link " + victim + "<->" + other);
                            c.servers[victim].blockPeer(other);
                            c.servers[other].blockPeer(victim);
                            link = new int[]{victim, other};
                        }
                    }
                }
            } catch (Exception e) {
                events.add("chaos action failed: " + e);
            }
        }
        // heal everything
        c.heal();
        if (link != null) {
            c.servers[link[0]].unblockPeer(link[1]);
            c.servers[link[1]].unblockPeer(link[0]);
        }
        if (down > 0) {
            try {
                c.restart(down);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void run(Path dir, long seed, long durationMs) throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        try (TestCluster c = new TestCluster(dir, 3, false, "--produce-timeout-ms=4000")) {
            try {
                ClusterIntegrationTest.createTopic(c, "chaos", PARTITIONS);
                AtomicBoolean stop = new AtomicBoolean();
                List<ProducerLoop> loops = new ArrayList<>();
                List<Thread> threads = new ArrayList<>();
                for (int i = 0; i < 2; i++) {
                    ProducerLoop loop = new ProducerLoop(i, c, stop);
                    loops.add(loop);
                    Thread t = new Thread(loop, "chaos-producer-" + i);
                    t.setDaemon(true);
                    t.start();
                    threads.add(t);
                }
                chaos(c, seed, durationMs, events);
                // calm period: producers keep going against a healthy cluster, so the final audit includes post-chaos records
                int calmTarget = loops.stream().mapToInt(l -> l.acked.size()).sum() + 20;
                Await.until(60_000, "20 more acknowledgements after healing", () -> loops.stream().mapToInt(l -> l.acked.size()).sum() >= calmTarget);
                stop.set(true);
                for (Thread t : threads) t.join(60_000);
                for (ProducerLoop l : loops) assertNull(l.crashed, "producer thread crashed: " + l.crashed);

                for (int i = 1; i <= c.n; i++) c.restart(i);                   // no-op for the ones that are up
                c.awaitController();
                c.awaitTopicEverywhere("chaos", PARTITIONS);
                for (int p = 0; p < PARTITIONS; p++) c.awaitLeader("chaos", p);
                for (int p = 0; p < PARTITIONS; p++) ClusterIntegrationTest.assertReplicasIdentical(c, "chaos", p);

                // the audit
                Map<Integer, List<LogRecord>> stored = ClusterIntegrationTest.readAll(c, "chaos");
                Map<String, int[]> positionOfValue = new HashMap<>();          // value -> {partition, index of FIRST copy}
                int storedTotal = 0;
                for (var e : stored.entrySet()) {
                    long expectedOffset = 0;
                    for (int idx = 0; idx < e.getValue().size(); idx++) {
                        LogRecord r = e.getValue().get(idx);
                        assertEquals(expectedOffset++, r.offset(), "partition " + e.getKey() + " offsets must be contiguous");
                        positionOfValue.putIfAbsent(new String(r.value()), new int[]{e.getKey(), idx});
                        storedTotal++;
                    }
                }
                int ackedTotal = 0;
                for (ProducerLoop l : loops) {
                    Map<String, int[]> lastPosition = new HashMap<>();
                    for (Ack a : l.acked) {
                        ackedTotal++;
                        int[] pos = positionOfValue.get(a.value());
                        assertNotNull(pos, "acknowledged record " + a.value() + " is MISSING");
                        int[] previous = lastPosition.put(a.key(), pos);
                        if (previous != null) {
                            assertEquals(previous[0], pos[0], "key " + a.key() + " moved between partitions");
                            assertTrue(pos[1] > previous[1], "acknowledged records of key " + a.key() + " are out of order at " + a.value());
                        }
                    }
                }
                System.out.printf("chaos seed=%d: %d acknowledged, %d stored (%d extra = unacknowledged or retried), %d events: %s%n",
                        seed, ackedTotal, storedTotal, storedTotal - ackedTotal, events.size(), events);
                assertTrue(ackedTotal > 20, "the producers made hardly any progress: " + ackedTotal);
            } catch (Throwable t) {
                System.err.println("CHAOS FAILURE with seed " + seed + "; events: " + events + "\n" + c.describe());
                throw t;
            }
        }
    }

    private static long seed() {
        long configured = Long.getLong("cluster.seed", 0);
        return configured != 0 ? configured : System.nanoTime() & 0xFFFFFFFFL;
    }

    @Test
    void shortChaosRunKeepsEveryAcknowledgedRecord(@TempDir Path dir) throws Exception {
        run(dir, seed(), 10_000);
    }

    @Test
    @Tag("slow")
    void sixtySecondChaosRunKeepsEveryAcknowledgedRecord(@TempDir Path dir) throws Exception {
        run(dir, seed(), 60_000);
    }
}
