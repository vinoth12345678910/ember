package com.ember.broker;

import com.ember.client.*;
import com.ember.protocol.DescribeGroupResponse;
import com.ember.protocol.PartitionOffset;
import com.ember.protocol.PartitionRef;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Random single failures (brokers, links, consumers, heartbeats) under continuous production AND consumption.
 * -Dcluster.seed=N picks the fault schedule (0 = a fresh seed, printed on failure).
 */
class GroupChaosTest {

    private static final int PARTITIONS = 6;
    private static final int SESSION_MS = 2000;
    private static final int HEARTBEAT_MS = 400;

    private static final class ProducerLoop implements Runnable {
        final int id;
        final TestCluster cluster;
        final AtomicBoolean stop;
        final Set<String> acked = Collections.synchronizedSet(new TreeSet<>());
        volatile Throwable crashed;

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
                    String value = "p" + id + "-" + seq;
                    try {
                        producer.send("chaos", ("key-" + (seq % 40)).getBytes(), value.getBytes());
                        acked.add(value);
                    } catch (EmberException | IOException e) {
                        // not acknowledged: it may or may not exist
                    }
                    seq++;
                    LockSupport.parkNanos(2_000_000L);               // a steady stream, not a flood: the consumers must keep up
                }
            } catch (Throwable t) {
                crashed = t;
            }
        }
    }

    /**
     * Samples committed offsets, then end offsets: committed must never exceed the end. The end is taken ONLY from partition
     * leaders that are READY (they have applied everything committed), as the maximum over such leaders; a lagging follower
     * or a not-yet-ready leader would show a smaller end and produce a false alarm. A suspected violation must persist when
     * sampled again a moment later (a deposed leader that has not noticed yet can briefly look ready with an old end).
     */
    private static final class CommitMonitor implements Runnable {
        final TestCluster cluster;
        final AtomicBoolean stop;
        final List<String> violations = new CopyOnWriteArrayList<>();
        volatile int samples;

        CommitMonitor(TestCluster cluster, AtomicBoolean stop) {
            this.cluster = cluster;
            this.stop = stop;
        }

        /** The largest applied end among READY leaders of the partition, or -1 if there is none right now. */
        private long readyEnd(int partition) {
            long end = -1;
            for (int i = 1; i <= cluster.n; i++) {
                BrokerServer server = cluster.servers[i];
                if (server == null) continue;
                var topic = server.replicaManager().topic("chaos");
                var replica = topic == null ? null : topic.replica(partition);
                if (replica != null && replica.group().isLeaderReady()) {
                    end = Math.max(end, replica.dataLog().nextOffset());
                }
            }
            return end;
        }

        @Override
        public void run() {
            try (ClusterClient admin = new ClusterClient(ClusterIntegrationTest.cfg(cluster).withRetries(1, 50, 100).withTimeouts(1_000, 2_000))) {
                while (!stop.get()) {
                    try {
                        List<PartitionOffset> committed = admin.describeGroup("g").offsets();     // first the committed offsets ...
                        boolean sampled = false;
                        for (PartitionOffset o : committed) {
                            long end = readyEnd(o.partition());                                // ... then the ends, which only grow
                            for (int again = 0; end >= 0 && o.offset() > end && again < 4; again++) {
                                LockSupport.parkNanos(300_000_000L);
                                end = Math.max(end, readyEnd(o.partition()));
                            }
                            if (end >= 0) {
                                sampled = true;
                                if (o.offset() > end) {
                                    violations.add("partition " + o.partition() + " committed " + o.offset() + " > end " + end + " (persisted for >1s)");
                                }
                            }
                        }
                        if (sampled) samples++;
                    } catch (Exception e) {
                        // the coordinator is moving: skip this sample
                    }
                    LockSupport.parkNanos(250_000_000L);
                }
            } catch (IOException ignored) {
                // closing the monitor's own connections at the end
            }
        }
    }

    private GroupMember newMember(TestCluster c, int serial) {
        return new GroupMember(c, "g", "m" + serial, SESSION_MS, HEARTBEAT_MS, true, 200, "chaos");
    }

    private void run(Path dir, long seed, long durationMs) throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        try (TestCluster c = new TestCluster(dir, 3, false, "--produce-timeout-ms=4000")) {
            List<GroupMember> everyone = new ArrayList<>();              // also the dead: what they received counts as delivered
            try {
                ClusterIntegrationTest.createTopic(c, "chaos", PARTITIONS);
                AtomicBoolean stopProducers = new AtomicBoolean();
                AtomicBoolean stopMonitor = new AtomicBoolean();
                List<ProducerLoop> loops = new ArrayList<>();
                List<Thread> threads = new ArrayList<>();
                for (int i = 0; i < 2; i++) {
                    ProducerLoop loop = new ProducerLoop(i, c, stopProducers);
                    loops.add(loop);
                    Thread t = new Thread(loop, "group-chaos-producer-" + i);
                    t.setDaemon(true);
                    t.start();
                    threads.add(t);
                }
                CommitMonitor monitor = new CommitMonitor(c, stopMonitor);
                Thread monitorThread = new Thread(monitor, "group-chaos-monitor");
                monitorThread.setDaemon(true);
                monitorThread.start();

                int serial = 0;
                List<GroupMember> live = new ArrayList<>();
                for (int i = 0; i < 3; i++) {
                    GroupMember m = newMember(c, serial++);
                    live.add(m);
                    everyone.add(m);
                }

                // ---------------- the chaos
                Random rnd = new Random(seed);
                long end = System.nanoTime() + durationMs * 1_000_000L;
                int down = -1;
                int[] link = null;
                GroupMember paused = null;
                long resumeAt = 0;
                while (System.nanoTime() < end) {
                    LockSupport.parkNanos((500 + rnd.nextInt(1000)) * 1_000_000L);
                    try {
                        if (paused != null && System.nanoTime() >= resumeAt) {
                            events.add("resume heartbeats of " + paused.name);
                            paused.consumer.pauseHeartbeatsForTesting(false);
                            paused = null;
                        }
                        if (down > 0 && rnd.nextInt(10) < 7) {
                            events.add("restart broker " + down);
                            c.restart(down);
                            down = -1;
                            continue;
                        }
                        if (link != null && rnd.nextInt(10) < 6) {
                            events.add("unblock link " + link[0] + "<->" + link[1]);
                            c.servers[link[0]].unblockPeer(link[1]);
                            c.servers[link[1]].unblockPeer(link[0]);
                            link = null;
                            continue;
                        }
                        switch (rnd.nextInt(4)) {
                            case 0 -> {
                                if (down < 0 && link == null) {
                                    down = 1 + rnd.nextInt(c.n);
                                    events.add("crash broker " + down);
                                    c.crash(down);
                                }
                            }
                            case 1 -> {
                                if (down < 0 && link == null) {
                                    int a = 1 + rnd.nextInt(c.n);
                                    int b = 1 + (a % c.n);
                                    events.add("block link " + a + "<->" + b);
                                    c.servers[a].blockPeer(b);
                                    c.servers[b].blockPeer(a);
                                    link = new int[]{a, b};
                                }
                            }
                            case 2 -> {
                                GroupMember victim = live.get(rnd.nextInt(live.size()));
                                if (victim != paused) {
                                    events.add("kill consumer " + victim.name + ", start a replacement");
                                    victim.crash();
                                    live.remove(victim);
                                    GroupMember fresh = newMember(c, serial++);
                                    live.add(fresh);
                                    everyone.add(fresh);
                                }
                            }
                            default -> {
                                if (paused == null) {
                                    paused = live.get(rnd.nextInt(live.size()));
                                    events.add("pause heartbeats of " + paused.name + " beyond its session");
                                    paused.consumer.pauseHeartbeatsForTesting(true);
                                    resumeAt = System.nanoTime() + (long) (SESSION_MS * 2.6) * 1_000_000L;
                                }
                            }
                        }
                    } catch (Exception e) {
                        events.add("chaos action failed: " + e);
                    }
                }

                // ---------------- heal everything
                events.add("HEAL");
                c.heal();
                if (link != null) {
                    c.servers[link[0]].unblockPeer(link[1]);
                    c.servers[link[1]].unblockPeer(link[0]);
                }
                if (down > 0) c.restart(down);
                if (paused != null) paused.consumer.pauseHeartbeatsForTesting(false);
                stopProducers.set(true);
                for (Thread t : threads) t.join(60_000);
                for (ProducerLoop l : loops) assertNull(l.crashed, "producer thread crashed: " + l.crashed);
                c.awaitController();
                c.awaitCoordinator();
                for (int p = 0; p < PARTITIONS; p++) c.awaitLeader("chaos", p);

                Set<String> acked = new TreeSet<>();
                loops.forEach(l -> acked.addAll(l.acked));
                GroupMember[] survivors = live.toArray(new GroupMember[0]);

                // ---------------- the group settles and drains
                DescribeGroupResponse finalState = GroupScenariosTest.awaitStable(c, PARTITIONS, survivors);
                Await.until(90_000, "every acknowledged record delivered at least once (" + acked.size() + " acked)\n" + c.describe(),
                        () -> GroupScenariosTest.union(everyone.toArray(new GroupMember[0])).containsAll(acked));
                try (ClusterClient admin = new ClusterClient(ClusterIntegrationTest.cfg(c))) {
                    Await.until(90_000, "lag 0: committed offsets equal the log end offsets\n" + c.describe(), () -> {
                        try {
                            long committed = GroupScenariosTest.committedTotal(admin, "g", "chaos");
                            long ends = 0;
                            for (PartitionView v : admin.describe("chaos")) ends += v.logEndOffset();
                            return committed == ends;
                        } catch (Exception e) {
                            return false;
                        }
                    });
                }
                stopMonitor.set(true);
                monitorThread.join(10_000);
                try (ClusterClient admin = new ClusterClient(ClusterIntegrationTest.cfg(c))) {            // after healing the check is exact
                    List<PartitionView> ends = admin.describe("chaos");
                    for (PartitionOffset o : admin.describeGroup("g").offsets()) {
                        assertTrue(o.offset() <= ends.get(o.partition()).logEndOffset(), "final committed offset beyond the log end of partition " + o.partition());
                    }
                }

                // ---------------- the audit
                assertTrue(monitor.violations.isEmpty(), "committed offsets beyond the log end: " + monitor.violations);
                assertTrue(monitor.samples > 0, "the commit monitor never got a sample");

                Set<String> distinct = new TreeSet<>();
                long deliveries = 0;
                for (GroupMember m : everyone) {
                    Set<String> seenThisJoin = new HashSet<>();
                    for (GroupMember.Delivery d : m.deliveries) {
                        deliveries++;
                        distinct.add(d.value());
                        assertTrue(seenThisJoin.add(d.joinCount() + "/" + d.topic() + "-" + d.partition() + "@" + d.offset()),
                                "member " + m.name + " received " + d.topic() + "-" + d.partition() + "@" + d.offset() + " twice without rejoining");
                    }
                }
                assertTrue(distinct.containsAll(acked));
                DescribeGroupResponse after = finalState;
                Set<PartitionRef> owned = new TreeSet<>();
                after.members().forEach(m -> owned.addAll(m.assignment()));
                assertEquals(PARTITIONS, owned.size(), "every partition owned exactly once in the final generation");
                c.awaitGroupStateIdentical();

                System.out.printf("group chaos seed=%d: %d acknowledged, %d distinct delivered, %d deliveries (duplicate rate %.2f%%), "
                                + "%d members used, final generation %d, %d commit samples, %d events: %s%n",
                        seed, acked.size(), distinct.size(), deliveries, 100.0 * (deliveries - distinct.size()) / Math.max(1, deliveries),
                        everyone.size(), finalState.generation(), monitor.samples, events.size(), events);
                assertTrue(acked.size() > 20, "the producers made hardly any progress: " + acked.size());
                for (GroupMember m : survivors) m.close();
            } catch (Throwable t) {
                System.err.println("GROUP CHAOS FAILURE with seed " + seed + "; events: " + events + "\n" + c.describe());
                for (GroupMember m : everyone) {
                    System.err.println("  member " + m.name + ": received=" + m.received.size() + " errors=" + m.errors + " last=" + m.lastError);
                }
                throw t;
            } finally {
                for (GroupMember m : everyone) {
                    try {
                        m.crash();
                    } catch (Exception ignored) {
                        // already stopped
                    }
                }
            }
        }
    }

    private static long seed() {
        long configured = Long.getLong("cluster.seed", 0);
        return configured != 0 ? configured : System.nanoTime() & 0xFFFFFFFFL;
    }

    @Test
    void shortGroupChaosRunLosesNothingAndKeepsOwnershipExclusive(@TempDir Path dir) throws Exception {
        run(dir, seed(), 10_000);
    }

    @Test
    @Tag("slow")
    void sixtySecondGroupChaosRunLosesNothingAndKeepsOwnershipExclusive(@TempDir Path dir) throws Exception {
        run(dir, seed(), 60_000);
    }
}
