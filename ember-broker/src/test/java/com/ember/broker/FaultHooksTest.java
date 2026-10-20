package com.ember.broker;

import com.ember.client.EmberClient;
import com.ember.protocol.ProducerRecord;
import com.ember.replication.ConfigurableFaults;
import com.ember.replication.FaultInjector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Fault hooks are OFF by default and each one works when switched on. */
class FaultHooksTest {

    private static List<ProducerRecord> one(String v) {
        return List.of(new ProducerRecord(null, v.getBytes()));
    }

    @Test
    void aNormalBrokerRunsWithTheDoNothingInjectorAndNothingFires(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC())) {
            assertSame(FaultInjector.NONE, server.faults(), "a real broker never gets an injector by itself");
        }
        assertFalse(FaultInjector.NONE.active());
        assertEquals(0, FaultInjector.NONE.fsyncDelayNanos());
        assertFalse(FaultInjector.NONE.dropPeerMessage(1));
        assertEquals(0, FaultInjector.NONE.peerDelayMillis(1));
        FaultInjector.NONE.awaitNotPaused();                                 // returns at once
        ConfigurableFaults fresh = new ConfigurableFaults();
        assertFalse(fresh.active(), "even the configurable one starts with nothing injected");
        assertEquals(0, fresh.droppedMessages());
        fresh.slowFsync(5);
        assertTrue(fresh.active());
        fresh.clear();
        assertFalse(fresh.active(), "clear() switches everything off");
    }

    @Test
    void dropProbabilityOneDropsEverythingToPeersAndZeroDropsNothing(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "t", 1);
            int leader = c.awaitLeader("t", 0);
            c.faults[leader].dropMessages(0.0, 1);
            assertFalse(c.faults[leader].active(), "probability 0 injects nothing");
            c.faults[leader].dropMessages(1.0, 1);
            Await.until(10_000, "heartbeats to be dropped by the hook", () -> c.faults[leader].droppedMessages() > 5);
            c.faults[leader].clear();                                           // no more drops: the two counts can now be compared exactly
            Await.until(10_000, "the peer metrics to account for every message the hook dropped", () -> {
                try {
                    double dropped = 0;
                    for (int peer = 1; peer <= 3; peer++) {
                        if (peer != leader) dropped += Http.value(c.adminPort(leader), "ember_peer_messages_dropped_total", "peer", Integer.toString(peer));
                    }
                    return dropped >= c.faults[leader].droppedMessages();
                } catch (Exception e) {
                    return false;
                }
            });
            c.faults[leader].clear();
        }
    }

    @Test
    void aDelayOnTheLeaderDelaysEveryAcknowledgedProduceByAtLeastThatMuch(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "t", 1);
            int leader = c.awaitLeader("t", 0);
            try (EmberClient client = new EmberClient("localhost", c.port(leader))) {
                client.produce("t", 0, one("warm-up"));
                c.faults[leader].delayMessages(100);      // below the 150 ms minimum election timeout, so heartbeats still arrive in time
                long start = System.nanoTime();
                client.produce("t", 0, one("delayed"));
                long millis = (System.nanoTime() - start) / 1_000_000;
                assertTrue(millis >= 90, "the AppendEntries to followers were delayed 100 ms, but the produce took " + millis + " ms");
                c.faults[leader].clear();
                assertTrue(c.faults[leader].delayedMessages() > 0);
            }
        }
    }

    @Test
    void slowFsyncRaisesTheFsyncHistogramByAtLeastTheInjectedTime(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, true)) {
            ClusterIntegrationTest.createTopic(c, "t", 1);
            int leader = c.awaitLeader("t", 0);
            double sumBefore = Http.value(c.adminPort(leader), "ember_raft_fsync_seconds_sum", "group", "t-0");
            c.faults[leader].slowFsync(40);
            try (EmberClient client = new EmberClient("localhost", c.port(leader))) {
                for (int i = 0; i < 5; i++) client.produce("t", 0, one("v" + i));
            }
            double sumAfter = Http.value(c.adminPort(leader), "ember_raft_fsync_seconds_sum", "group", "t-0");
            assertTrue(sumAfter - sumBefore >= 5 * 0.040 * 0.9, "5 produces, each at least one 40 ms fsync: histogram sum rose only " + (sumAfter - sumBefore) + " s");
            c.faults[leader].clear();
        }
    }

    @Test
    void aPausedFollowerStopsApplyingWhileTheOthersCommitAndCatchesUpWhenResumed(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "t", 1);
            int leader = c.awaitLeader("t", 0);
            int follower = leader == 1 ? 2 : 1;
            try (EmberClient client = new EmberClient("localhost", c.port(leader))) {
                for (int i = 0; i < 10; i++) client.produce("t", 0, one("a" + i));
                Await.until(20_000, "the follower to hold the first 10 records", () -> c.servers[follower].replicaManager().logFor("t", 0).nextOffset() == 10);
                c.faults[follower].pause();
                for (int i = 0; i < 10; i++) client.produce("t", 0, one("b" + i));       // the leader + the other follower are a majority
                assertEquals(20, c.servers[leader].replicaManager().logFor("t", 0).nextOffset());
                assertEquals(10, c.servers[follower].replicaManager().logFor("t", 0).nextOffset(), "the frozen follower applied nothing");
                c.faults[follower].resume();
                Await.until(30_000, "the resumed follower to catch up\n" + c.describe(), () -> c.servers[follower].replicaManager().logFor("t", 0).nextOffset() == 20);
            }
        }
    }

    @Test
    void aPausedLeaderIsReplacedAndTheClusterKeepsCommitting(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "t", 1);
            int old = c.awaitLeader("t", 0);
            c.faults[old].pause();
            // a frozen leader still REPORTS itself as leader (its published state is frozen too), so look at the others only
            int[] found = {-1};
            Await.until(30_000, "a new leader among the unpaused brokers\n" + c.describe(), () -> {
                for (int i = 1; i <= 3; i++) {
                    if (i != old && c.servers[i].replicaManager().topic("t").replica(0).group().isLeaderReady()) {
                        found[0] = i;
                        return true;
                    }
                }
                return false;
            });
            int next = found[0];
            try (EmberClient client = new EmberClient("localhost", c.port(next))) {
                client.produce("t", 0, one("after-pause"));
            }
            CompletableFuture<Void> resumed = CompletableFuture.runAsync(() -> c.faults[old].resume());
            resumed.get(10, TimeUnit.SECONDS);
            Await.until(30_000, "the old leader to catch up and agree\n" + c.describe(), () -> c.servers[old].replicaManager().logFor("t", 0).nextOffset() == 1);
            Await.until(20_000, "the old leader to step down after resuming\n" + c.describe(),
                    () -> !c.servers[old].replicaManager().topic("t").replica(0).group().isLeader());
        }
    }
}
