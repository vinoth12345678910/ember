package com.ember.broker;

import com.ember.protocol.*;
import com.ember.replication.GroupStateMachine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** The seven group APIs through the real Netty pipeline, with raw frames. */
class GroupApisOverTheWireTest {

    private static JoinGroupRequest join(String member, String client, int session, String... topics) {
        return new JoinGroupRequest("g", member, client, session, List.of(topics));
    }

    private static ResponseCodec.Decoded call(RawClient c, int corr, Request r) throws Exception {
        c.send(corr, r);
        return c.read(r.apiKey());
    }

    private static void createTopic(RawClient c, String name, int partitions) throws Exception {
        assertEquals(ErrorCode.NONE, call(c, 1, new CreateTopicRequest(name, partitions)).errorCode());
    }

    @Test
    void theWholeLifecycleAgainstASingleBroker(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC()); RawClient c = new RawClient(server.port())) {
            createTopic(c, "orders", 4);

            FindCoordinatorResponse found = (FindCoordinatorResponse) call(c, 2, new FindCoordinatorRequest("g")).body();
            assertEquals(server.port(), found.port());

            JoinGroupResponse joined = (JoinGroupResponse) call(c, 3, join("", "app", 5000, "orders")).body();
            assertEquals(1, joined.generation());
            assertTrue(joined.memberId().startsWith("app-"));
            assertEquals(4, joined.assignment().size());

            assertEquals(ErrorCode.NONE, call(c, 4, new HeartbeatRequest("g", joined.memberId(), 1)).errorCode());
            assertEquals(ErrorCode.ILLEGAL_GENERATION, call(c, 5, new HeartbeatRequest("g", joined.memberId(), 2)).errorCode());

            assertEquals(ErrorCode.NONE, call(c, 6, new OffsetCommitRequest("g", joined.memberId(), 1,
                    List.of(new PartitionOffset("orders", 0, 17), new PartitionOffset("orders", 3, 4)))).errorCode());
            OffsetFetchResponse fetched = (OffsetFetchResponse) call(c, 7, new OffsetFetchRequest("g",
                    List.of(new PartitionRef("orders", 0), new PartitionRef("orders", 1), new PartitionRef("orders", 3)))).body();
            assertEquals(List.of(new PartitionOffset("orders", 0, 17), new PartitionOffset("orders", 1, -1), new PartitionOffset("orders", 3, 4)), fetched.offsets());

            DescribeGroupResponse described = (DescribeGroupResponse) call(c, 8, new DescribeGroupRequest("g")).body();
            assertEquals(DescribeGroupResponse.STABLE, described.state());
            assertEquals(1, described.members().size());
            assertEquals(2, described.offsets().size());

            assertEquals(ErrorCode.NONE, call(c, 9, new LeaveGroupRequest("g", joined.memberId())).errorCode());
            assertEquals(ErrorCode.UNKNOWN_MEMBER_ID, call(c, 10, new HeartbeatRequest("g", joined.memberId(), 2)).errorCode());
            assertEquals(DescribeGroupResponse.EMPTY, ((DescribeGroupResponse) call(c, 11, new DescribeGroupRequest("g")).body()).state());
            assertEquals(DescribeGroupResponse.UNKNOWN, ((DescribeGroupResponse) call(c, 12, new DescribeGroupRequest("never")).body()).state());
            assertEquals(17, ((OffsetFetchResponse) call(c, 13, new OffsetFetchRequest("g", List.of(new PartitionRef("orders", 0)))).body()).offsets().get(0).offset(),
                    "the offsets of an empty group are kept");
        }
    }

    @Test
    void sessionTimeoutsOutsideTheBrokersRangeAreRejected(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir, "--group-max-session-ms=2000"), Clock.systemUTC());
             RawClient c = new RawClient(server.port())) {
            assertEquals(ErrorCode.INVALID_SESSION_TIMEOUT, call(c, 1, join("", "a", 50, "t")).errorCode());
            assertEquals(ErrorCode.INVALID_SESSION_TIMEOUT, call(c, 2, join("", "a", 2001, "t")).errorCode());
            assertEquals(ErrorCode.NONE, call(c, 3, join("", "a", 2000, "t")).errorCode());
        }
    }

    @Test
    void followersPointToTheCoordinatorAndRefuseGroupRequests(@TempDir Path dir) throws Exception {
        try (TestCluster cluster = new TestCluster(dir, 3, false)) {
            int coordinator = cluster.awaitCoordinator();
            for (int broker = 1; broker <= 3; broker++) {
                final int i = broker;
                Await.until(10_000, "broker " + i + " to know the coordinator", () -> {
                    try (RawClient c = new RawClient(cluster.port(i))) {
                        return call(c, 1, new FindCoordinatorRequest("g")).errorCode() == ErrorCode.NONE;
                    } catch (Exception e) {
                        return false;
                    }
                });
                try (RawClient c = new RawClient(cluster.port(i))) {
                    FindCoordinatorResponse found = (FindCoordinatorResponse) call(c, 1, new FindCoordinatorRequest("g")).body();
                    assertEquals(coordinator, found.nodeId(), "every broker names the same coordinator");
                    assertEquals(cluster.port(coordinator), found.port());
                    if (i != coordinator) {
                        assertEquals(ErrorCode.NOT_COORDINATOR, call(c, 2, join("", "a", 5000, "t")).errorCode());
                        assertEquals(ErrorCode.NOT_COORDINATOR, call(c, 3, new HeartbeatRequest("g", "m", 1)).errorCode());
                        assertEquals(ErrorCode.NOT_COORDINATOR, call(c, 4, new DescribeGroupRequest("g")).errorCode());
                    }
                }
            }
        }
    }

    @Test
    void groupStateIsReplicatedToEveryBroker(@TempDir Path dir) throws Exception {
        try (TestCluster cluster = new TestCluster(dir, 3, false)) {
            int coordinator = cluster.awaitCoordinator();
            try (RawClient c = new RawClient(cluster.port(coordinator))) {
                createTopicViaController(cluster, "orders", 3);
                JoinGroupResponse joined = (JoinGroupResponse) call(c, 2, join("", "app", 30_000, "orders")).body();
                assertEquals(ErrorCode.NONE, call(c, 3, new OffsetCommitRequest("g", joined.memberId(), 1, List.of(new PartitionOffset("orders", 1, 9)))).errorCode());
            }
            Map<String, GroupStateMachine.GroupData> snapshot = cluster.awaitGroupStateIdentical();
            assertEquals(9L, snapshot.get("g").offsets().get(new PartitionRef("orders", 1)));
            assertEquals(1, snapshot.get("g").members().size());
        }
    }

    private static void createTopicViaController(TestCluster cluster, String name, int partitions) throws Exception {
        int controller = cluster.awaitController();
        try (RawClient c = new RawClient(cluster.port(controller))) {
            assertEquals(ErrorCode.NONE, call(c, 1, new CreateTopicRequest(name, partitions)).errorCode());
        }
        cluster.awaitTopicEverywhere(name, partitions);
    }

    @Test
    void aSilentMemberIsRemovedAfterItsSessionTimeout(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC()); RawClient c = new RawClient(server.port())) {
            createTopic(c, "orders", 2);
            JoinGroupResponse joined = (JoinGroupResponse) call(c, 2, join("", "app", 200, "orders")).body();
            assertEquals(1, joined.generation());
            Await.until(10_000, "the silent member to be expired by the coordinator", () -> {
                var g = server.replicaManager().groupState().group("g");
                return g != null && g.members().isEmpty();
            });
            assertEquals(2, server.replicaManager().groupState().group("g").generation(), "expiry cost exactly one generation");
            assertEquals(ErrorCode.UNKNOWN_MEMBER_ID, call(c, 3, new HeartbeatRequest("g", joined.memberId(), 2)).errorCode());
        }
    }

    @Test
    void aCoordinatorCrashKeepsMembersGenerationAndOffsets(@TempDir Path dir) throws Exception {
        try (TestCluster cluster = new TestCluster(dir, 3, false)) {
            int coordinator = cluster.awaitCoordinator();
            createTopicViaController(cluster, "orders", 3);
            JoinGroupResponse joined;
            try (RawClient c = new RawClient(cluster.port(coordinator))) {
                joined = (JoinGroupResponse) call(c, 2, join("", "app", 30_000, "orders")).body();
                assertEquals(ErrorCode.NONE, call(c, 3, new OffsetCommitRequest("g", joined.memberId(), 1, List.of(new PartitionOffset("orders", 0, 5)))).errorCode());
            }
            cluster.awaitGroupStateIdentical();
            cluster.crash(coordinator);
            Await.until(20_000, "a new coordinator", () -> cluster.coordinator() > 0);
            int next = cluster.coordinator();
            assertNotEquals(coordinator, next);
            try (RawClient c = new RawClient(cluster.port(next))) {
                assertEquals(ErrorCode.NONE, call(c, 4, new HeartbeatRequest("g", joined.memberId(), 1)).errorCode(),
                        "same member id, same generation: the replicated state was enough");
                OffsetFetchResponse fetched = (OffsetFetchResponse) call(c, 5, new OffsetFetchRequest("g", List.of(new PartitionRef("orders", 0)))).body();
                assertEquals(5, fetched.offsets().get(0).offset());
                assertEquals(ErrorCode.NONE, call(c, 6, new OffsetCommitRequest("g", joined.memberId(), 1, List.of(new PartitionOffset("orders", 0, 8)))).errorCode());
            }
        }
    }
}
