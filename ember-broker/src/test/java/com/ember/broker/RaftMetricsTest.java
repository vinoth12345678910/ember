package com.ember.broker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** The Raft and peer gauges must reflect reality, not just exist. */
class RaftMetricsTest {

    @Test
    void afterTheLeaderDiesTheNewLeaderExportsRoleTwoAndTheLeaderChangesGoUp(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "t", 1);
            int old = c.awaitLeader("t", 0);
            double[] changesBefore = new double[4];
            for (int i = 1; i <= 3; i++) {
                if (i != old) changesBefore[i] = Http.value(c.adminPort(i), "ember_raft_leader_changes_total", "group", "t-0");
            }
            c.crash(old);
            Await.until(30_000, "a new leader\n" + c.describe(), () -> c.leaderOf("t", 0) > 0 && c.leaderOf("t", 0) != old);
            int next = c.leaderOf("t", 0);
            Await.until(10_000, "the new leader to export raft_role 2", () -> {
                try {
                    return Http.value(c.adminPort(next), "ember_raft_role", "group", "t-0") == 2.0;
                } catch (Exception e) {
                    return false;
                }
            });
            List<Http.Sample> s = Http.scrape(c.adminPort(next));
            assertTrue(Http.value(s, "ember_raft_leader_changes_total", "group", "t-0") >= changesBefore[next] + 1,
                    "the leader changed, so the counter went up");
            assertTrue(Http.value(s, "ember_raft_elections_started_total", "group", "t-0") >= 1, "the winner started an election");
            for (int i = 1; i <= 3; i++) {
                if (i == old || i == next) continue;
                assertEquals(0.0, Http.value(c.adminPort(i), "ember_raft_role", "group", "t-0"), "the follower exports role 0");
            }
        }
    }

    @Test
    void peerConnectedGoesToZeroWhenTheLinkIsBlockedAndBackToOneAfterHealing(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            // block a link on the LEADER: only a leader sends regularly to every peer (heartbeats), a follower mostly just answers its leader
            int leader = c.awaitController();
            int blocked = leader == 1 ? 2 : 1;
            int other = 6 - leader - blocked;
            Await.until(20_000, "the leader to see its peers connected", () -> {
                try {
                    return Http.value(c.adminPort(leader), "ember_peer_connected", "peer", Integer.toString(blocked)) == 1.0
                            && Http.value(c.adminPort(leader), "ember_peer_connected", "peer", Integer.toString(other)) == 1.0;
                } catch (Exception e) {
                    return false;
                }
            });
            c.servers[leader].blockPeer(blocked);
            assertEquals(0.0, Http.value(c.adminPort(leader), "ember_peer_connected", "peer", Integer.toString(blocked)));
            assertEquals(1.0, Http.value(c.adminPort(leader), "ember_peer_connected", "peer", Integer.toString(other)), "the other link is unaffected");
            double droppedBefore = Http.value(c.adminPort(leader), "ember_peer_messages_dropped_total", "peer", Integer.toString(blocked));
            Await.until(10_000, "heartbeats towards the blocked peer to be dropped and counted", () -> {
                try {
                    return Http.value(c.adminPort(leader), "ember_peer_messages_dropped_total", "peer", Integer.toString(blocked)) > droppedBefore;
                } catch (Exception e) {
                    return false;
                }
            });
            c.servers[leader].unblockPeer(blocked);
            assertEquals(1.0, Http.value(c.adminPort(leader), "ember_peer_connected", "peer", Integer.toString(blocked)));
            assertTrue(Http.value(c.adminPort(leader), "ember_peer_messages_sent_total", "peer", Integer.toString(blocked)) > 0);
        }
    }

    @Test
    void anIsolatedLeaderCountsItsCheckQuorumStepDown(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "t", 1);
            int leader = c.awaitLeader("t", 0);
            double before = Http.value(c.adminPort(leader), "ember_raft_checkquorum_stepdowns_total", "group", "t-0");
            c.isolate(leader);
            Await.until(20_000, "the isolated leader to step down (CheckQuorum) and count it\n" + c.describe(), () -> {
                try {
                    return Http.value(c.adminPort(leader), "ember_raft_checkquorum_stepdowns_total", "group", "t-0") > before;
                } catch (Exception e) {
                    return false;
                }
            });
            assertEquals(0.0, Http.value(c.adminPort(leader), "ember_raft_role", "group", "t-0"), "it is a follower now");
            c.heal();
        }
    }

    @Test
    void theFsyncHistogramCountsRealFsyncsWhenFsyncIsOn(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, true)) {
            ClusterIntegrationTest.createTopic(c, "t", 1);
            int leader = c.awaitLeader("t", 0);
            double before = Http.value(c.adminPort(leader), "ember_raft_fsync_total", "group", "t-0");
            EmberGroupConsumerTest.produce(c, "t", 0, 5);
            double after = Http.value(c.adminPort(leader), "ember_raft_fsync_total", "group", "t-0");
            assertTrue(after >= before + 5, "every acknowledged produce needed at least one fsync on the leader: " + before + " -> " + after);
            assertEquals(after, Http.value(c.adminPort(leader), "ember_raft_fsync_seconds_count", "group", "t-0"));
        }
        try (TestCluster c = new TestCluster(dir.resolve("nofsync"), 1, false)) {
            ClusterIntegrationTest.createTopic(c, "t", 1);
            EmberGroupConsumerTest.produce(c, "t", 0, 5);
            assertEquals(0.0, Http.value(c.adminPort(1), "ember_raft_fsync_total", "group", "t-0"), "no fsync happens with --unsafe-no-raft-fsync");
        }
    }
}
