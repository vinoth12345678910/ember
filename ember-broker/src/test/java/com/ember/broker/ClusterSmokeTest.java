package com.ember.broker;

import com.ember.client.EmberClient;
import com.ember.client.EmberException;
import com.ember.protocol.*;
import com.ember.raft.RaftMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Three real brokers in one JVM: the replicated request path, with the single-connection EmberClient. */
class ClusterSmokeTest {

    private static ProducerRecord rec(String k, String v) {
        return new ProducerRecord(k == null ? null : k.getBytes(), v.getBytes());
    }

    private static int follower(TestCluster c, int leader) {
        return leader == 1 ? 2 : 1;
    }

    @Test
    void threeBrokersElectAControllerAndATopicAppearsOnAllOfThem(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            int controller = c.awaitController();
            try (EmberClient client = new EmberClient("localhost", c.port(controller))) {
                assertEquals(3, client.createTopic("orders", 3));
            }
            c.awaitTopicEverywhere("orders", 3);
            for (int p = 0; p < 3; p++) c.awaitLeader("orders", p);

            for (int i = 1; i <= 3; i++) {
                try (RawClient raw = new RawClient(c.port(i))) {
                    raw.send(1, (short) 2, new MetadataRequest(List.of()));
                    ClusterMetadataResponse m = (ClusterMetadataResponse) raw.read(ApiKey.METADATA, (short) 2).body();
                    assertEquals(3, m.brokers().size());
                    assertEquals(List.of(1, 2, 3), m.brokers().stream().map(ClusterMetadataResponse.Broker::nodeId).toList());
                    assertEquals(c.port(2), m.brokers().get(1).port());
                    assertEquals(controller, m.controllerId());
                    assertEquals(1, m.topics().size());
                    assertEquals("orders", m.topics().get(0).name());
                    assertEquals(3, m.topics().get(0).partitions().size());
                    for (var p : m.topics().get(0).partitions()) {
                        assertEquals(List.of(1, 2, 3), p.replicas());
                    }
                }
            }
            int leaders = 0;
            try (RawClient raw = new RawClient(c.port(controller))) {
                raw.send(1, (short) 2, new MetadataRequest(List.of("orders")));
                ClusterMetadataResponse m = (ClusterMetadataResponse) raw.read(ApiKey.METADATA, (short) 2).body();
                for (var p : m.topics().get(0).partitions()) {
                    assertTrue(p.leaderId() >= 1 && p.leaderId() <= 3, "the controller knows every leader");
                    leaders++;
                }
            }
            assertEquals(3, leaders);
        }
    }

    @Test
    void produceAndFetchGoToTheLeaderAndFollowersRefuse(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            int controller = c.awaitController();
            try (EmberClient client = new EmberClient("localhost", c.port(controller))) {
                client.createTopic("t", 1);
            }
            c.awaitTopicEverywhere("t", 1);
            int leader = c.awaitLeader("t", 0);
            int other = follower(c, leader);
            Await.until(10_000, "the follower to know the leader", () -> c.servers[other].replicaManager().topic("t").replica(0).group().leaderId() == leader);

            try (EmberClient lead = new EmberClient("localhost", c.port(leader)); EmberClient foll = new EmberClient("localhost", c.port(other))) {
                assertEquals(0, lead.produce("t", 0, List.of(rec("k", "a"), rec(null, "b"))));
                assertEquals(2, lead.produce("t", 0, List.of(rec("k", "c"))));
                var fetched = lead.fetch("t", 0, 0, 10_000);
                assertEquals(3, fetched.records().size());
                assertEquals(3, fetched.highWatermark(), "the high watermark is the committed end");

                assertEquals(ErrorCode.NOT_LEADER_OR_FOLLOWER, assertThrows(EmberException.class, () -> foll.produce("t", 0, List.of(rec(null, "x")))).errorCode());
                assertEquals(ErrorCode.NOT_LEADER_OR_FOLLOWER, assertThrows(EmberException.class, () -> foll.fetch("t", 0, 0, 100)).errorCode());
            }
            // every replica ends up with the identical data log
            Await.until(10_000, "all replicas to hold 3 records", () -> {
                try {
                    return c.summary(1, "t", 0).size() == 3 && c.summary(2, "t", 0).size() == 3 && c.summary(3, "t", 0).size() == 3;
                } catch (Exception e) {
                    return false;
                }
            });
            assertEquals(c.summary(leader, "t", 0), c.summary(other, "t", 0));
            assertArrayEquals(c.dataLogBytes(1, "t", 0), c.dataLogBytes(2, "t", 0));
            assertArrayEquals(c.dataLogBytes(2, "t", 0), c.dataLogBytes(3, "t", 0));
        }
    }

    @Test
    void createTopicOnANonControllerIsRedirectedWithNotController(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            int controller = c.awaitController();
            int other = follower(c, controller);
            try (EmberClient client = new EmberClient("localhost", c.port(other))) {
                assertEquals(ErrorCode.NOT_CONTROLLER, assertThrows(EmberException.class, () -> client.createTopic("x", 1)).errorCode());
            }
        }
    }

    @Test
    void autoCreateOnProduceWorksOnlyOnTheController(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false, "--default-partitions=2")) {
            int controller = c.awaitController();
            int other = follower(c, controller);
            try (EmberClient onOther = new EmberClient("localhost", c.port(other))) {
                assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, assertThrows(EmberException.class, () -> onOther.produce("auto", 0, List.of(rec(null, "x")))).errorCode());
            }
            try (EmberClient onController = new EmberClient("localhost", c.port(controller))) {
                try {
                    onController.produce("auto", 0, List.of(rec(null, "x")));      // may race the partition's own election
                } catch (EmberException expectedSometimes) {
                    assertTrue(expectedSometimes.errorCode() == ErrorCode.NOT_LEADER_OR_FOLLOWER
                            || expectedSometimes.errorCode() == ErrorCode.LEADER_NOT_AVAILABLE, "got " + expectedSometimes.errorCode());
                }
            }
            c.awaitTopicEverywhere("auto", 2);
            int leader = c.awaitLeader("auto", 0);
            try (EmberClient onLeader = new EmberClient("localhost", c.port(leader))) {
                assertTrue(onLeader.produce("auto", 0, List.of(rec(null, "y"))) >= 0);
            }
        }
    }

    @Test
    void repeatedProducesToANonLeaderAreRefusedImmediatelyEveryTime(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            int controller = c.awaitController();
            try (EmberClient client = new EmberClient("localhost", c.port(controller))) {
                client.createTopic("t", 1);
            }
            c.awaitTopicEverywhere("t", 1);
            int leader = c.awaitLeader("t", 0);
            int other = follower(c, leader);
            Await.until(10_000, "the follower to know the leader", () -> c.servers[other].replicaManager().topic("t").replica(0).group().leaderId() == leader);
            try (EmberClient foll = new EmberClient("localhost", c.port(other))) {
                long start = System.nanoTime();
                for (int i = 0; i < 300; i++) {
                    assertEquals(ErrorCode.NOT_LEADER_OR_FOLLOWER, assertThrows(EmberException.class, () -> foll.produce("t", 0, List.of(rec(null, "x")))).errorCode());
                }
                assertTrue((System.nanoTime() - start) / 1_000_000 < 5_000, "refusals must be immediate, not a wait for the produce timeout");
            }
        }
    }

    @Test
    void reservedTopicNamesAreRejectedAndUnknownRaftGroupsAreDroppedQuietly(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            int controller = c.awaitController();
            try (EmberClient client = new EmberClient("localhost", c.port(controller))) {
                assertEquals(ErrorCode.INVALID_REQUEST, assertThrows(EmberException.class, () -> client.createTopic("__meta", 1)).errorCode());
                assertEquals(ErrorCode.INVALID_REQUEST, assertThrows(EmberException.class, () -> client.produce("__hidden", 0, List.of(rec(null, "x")))).errorCode());
            }
            try (RawClient raw = new RawClient(c.port(controller))) {
                raw.send(0, new RaftMessageRequest("nosuchtopic", 7, 2, new RaftMessage.RequestVote(5, 2, 0, 0)));   // no reply is ever sent
                raw.send(1, new MetadataRequest(List.of()));
                assertEquals(ErrorCode.NONE, raw.read(ApiKey.METADATA).errorCode(), "the connection and the broker are fine");
            }
        }
    }

    @Test
    void aPeerMessageFromABlockedPeerIsIgnoredAndTheLinkCanBeHealed(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            int controller = c.awaitController();
            c.isolate(controller);
            Await.until(15_000, "the isolated controller to step down (CheckQuorum)", () -> !c.servers[controller].replicaManager().metaGroup().isLeader());
            Await.until(15_000, "the other two to elect a new controller", () -> c.controller() > 0 && c.controller() != controller);
            c.heal();
            Await.until(15_000, "the old controller to learn the new leader", () -> c.servers[controller].replicaManager().metaGroup().leaderId() == c.controller());
        }
    }
}
