package com.ember.broker;

import com.ember.client.*;
import com.ember.protocol.DescribeGroupResponse;
import com.ember.protocol.ErrorCode;
import com.ember.protocol.PartitionOffset;
import com.ember.protocol.PartitionRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/** One scenario per promise of consumer groups. Every wait polls a condition and prints the cluster state on failure. */
class GroupScenariosTest {

    // ------------------------------------------------------------------ helpers

    private static GroupMember member(TestCluster c, String name, boolean autoCommit, String... topics) {
        return new GroupMember(c, "g", name, 3000, 500, autoCommit, 200, topics);
    }

    private static Set<String> values(int from, int to) {
        Set<String> out = new TreeSet<>();
        for (int i = from; i < to; i++) out.add("v" + i);
        return out;
    }

    static Set<String> union(GroupMember... members) {
        Set<String> out = new TreeSet<>();
        for (GroupMember m : members) out.addAll(m.values());
        return out;
    }

    static void awaitDelivered(TestCluster c, Set<String> expected, GroupMember... members) {
        Await.until(60_000, "all " + expected.size() + " records delivered to the group at least once\n" + c.describe(), () -> union(members).containsAll(expected));
    }

    static ClusterClient admin(TestCluster c) {
        return new ClusterClient(ClusterIntegrationTest.cfg(c));
    }

    static long committedTotal(ClusterClient admin, String group, String topic) throws Exception {
        long total = 0;
        for (PartitionOffset o : admin.describeGroup(group).offsets()) {
            if (o.topic().equals(topic)) total += o.offset();
        }
        return total;
    }

    private static void awaitCommittedTotal(TestCluster c, String topic, long expected) throws Exception {
        try (ClusterClient admin = admin(c)) {
            Await.until(40_000, "committed offsets of " + topic + " to add up to " + expected + "\n" + c.describe(), () -> {
                try {
                    return committedTotal(admin, "g", topic) == expected;
                } catch (Exception e) {
                    return false;
                }
            });
        }
    }

    /**
     * Waits until the group is settled and audits ownership: the coordinator says STABLE with exactly these members, every
     * partition is owned by exactly one of them, and each consumer's own view (generation, assignment) equals the coordinator's.
     */
    static DescribeGroupResponse awaitStable(TestCluster c, int partitions, GroupMember... members) throws Exception {
        DescribeGroupResponse[] settled = new DescribeGroupResponse[1];
        try (ClusterClient admin = admin(c)) {
            Await.until(60_000, "a stable group of " + members.length + " members owning " + partitions + " partitions\n" + c.describe(), () -> {
                try {
                    DescribeGroupResponse d = admin.describeGroup("g");
                    if (d.state() != DescribeGroupResponse.STABLE || d.members().size() != members.length) return false;
                    Set<PartitionRef> owned = new TreeSet<>();
                    int sum = 0;
                    for (DescribeGroupResponse.Member m : d.members()) {
                        owned.addAll(m.assignment());
                        sum += m.assignment().size();
                    }
                    if (sum != partitions || owned.size() != partitions) return false;       // overlap or a hole
                    for (GroupMember gm : members) {
                        DescribeGroupResponse.Member described = null;
                        for (DescribeGroupResponse.Member m : d.members()) if (m.memberId().equals(gm.consumer.memberId())) described = m;
                        if (described == null || gm.consumer.generation() != d.generation()
                                || !gm.consumer.assignment().equals(new TreeSet<>(described.assignment()))) return false;
                    }
                    settled[0] = d;
                    return true;
                } catch (Exception e) {
                    return false;
                }
            });
        }
        return settled[0];
    }

    // ------------------------------------------------------------------ rebalancing

    private void rebalanceScenario(TestCluster c) throws Exception {
        ClusterIntegrationTest.createTopic(c, "orders", 6);
        try (GroupMember a = member(c, "a", true, "orders"); GroupMember b = member(c, "b", true, "orders"); GroupMember cc = member(c, "c", true, "orders")) {
            DescribeGroupResponse three = awaitStable(c, 6, a, b, cc);
            for (DescribeGroupResponse.Member m : three.members()) assertEquals(2, m.assignment().size(), "each of three owns two of six");

            EmberGroupConsumerTest.produce(c, "orders", 0, 120);
            awaitDelivered(c, values(0, 120), a, b, cc);

            try (GroupMember d = member(c, "d", true, "orders")) {
                DescribeGroupResponse four = awaitStable(c, 6, a, b, cc, d);
                assertTrue(four.generation() > three.generation(), "a join starts a new generation");
                List<Integer> sizes = new ArrayList<>();
                four.members().forEach(m -> sizes.add(m.assignment().size()));
                Collections.sort(sizes);
                assertEquals(List.of(1, 1, 2, 2), sizes, "six partitions over four members");
            }                                                                       // d closes: commit + leave
            DescribeGroupResponse after = awaitStable(c, 6, a, b, cc);
            for (DescribeGroupResponse.Member m : after.members()) assertEquals(2, m.assignment().size());

            EmberGroupConsumerTest.produce(c, "orders", 120, 180);
            awaitDelivered(c, values(0, 180), a, b, cc);
            Map<String, ?> snapshot = c.awaitGroupStateIdentical();
            assertTrue(snapshot.containsKey("g"));
        }
    }

    @Test
    void threeConsumersSixPartitionsThenAFourthJoinsThenOneLeaves(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            rebalanceScenario(c);
        }
    }

    @Test
    void theSameRebalancesWorkInSingleBrokerMode(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 1, false)) {
            rebalanceScenario(c);
        }
    }

    // ------------------------------------------------------------------ consumer failure

    @Test
    void aCrashedConsumersPartitionsMoveAndItsUncommittedRecordsAreDeliveredAgain(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "orders", 6);
            GroupMember doomed = member(c, "doomed", false, "orders");       // never commits: everything it reads is "unfinished work"
            try (GroupMember b = member(c, "b", true, "orders"); GroupMember cc = member(c, "c", true, "orders")) {
                awaitStable(c, 6, doomed, b, cc);
                EmberGroupConsumerTest.produce(c, "orders", 0, 120);
                awaitDelivered(c, values(0, 120), doomed, b, cc);
                Set<String> readByDoomed = doomed.values();
                assertFalse(readByDoomed.isEmpty());

                doomed.crash();                                                 // no commit, no leave
                DescribeGroupResponse after = awaitStable(c, 6, b, cc);          // after the session timeout
                for (DescribeGroupResponse.Member m : after.members()) assertEquals(3, m.assignment().size());

                Await.until(60_000, "the survivors to re-deliver everything the crashed consumer had read but never committed\n" + c.describe(),
                        () -> union(b, cc).containsAll(readByDoomed));
                assertTrue(union(b, cc).containsAll(values(0, 120)), "nothing was missed");
            }
        }
    }

    @Test
    void aZombieConsumerIsFencedAndCannotOverwriteTheNewOwnersOffsets(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "orders", 4);
            EmberGroupConsumerTest.produce(c, "orders", 0, 40);
            GroupConsumerConfig zombieConfig = EmberGroupConsumerTest.groupConfig(c, "g").withClientId("zombie").withSession(5000, 800).withAutoCommit(false, 1000);
            try (EmberGroupConsumer zombie = new EmberGroupConsumer(zombieConfig)) {
                zombie.subscribe("orders");
                assertEquals(40, EmberGroupConsumerTest.drain(zombie, 40, 30_000).size());
                assertEquals(1, zombie.generation());

                zombie.pauseHeartbeatsForTesting(true);                         // paused / partitioned / just slow
                try (GroupMember replacement = member(c, "new-owner", true, "orders")) {
                    try (ClusterClient admin = admin(c)) {
                        Await.until(30_000, "the replacement to join (generation 2)\n" + c.describe(), () -> {
                            try {
                                DescribeGroupResponse d = admin.describeGroup("g");
                                return d.generation() == 2 && d.members().size() == 2;
                            } catch (Exception e) {
                                return false;
                            }
                        });
                        Set<PartitionRef> ownedByNew = new TreeSet<>();
                        for (DescribeGroupResponse.Member m : admin.describeGroup("g").members()) {
                            if (m.clientId().equals("new-owner")) ownedByNew.addAll(m.assignment());
                        }
                        assertEquals(2, ownedByNew.size());
                        // keys spread unevenly over partitions, so wait for "committed == what the new owner received" in generation 2
                        Await.until(20_000, "the new owner to commit everything it read\n" + c.describe(), () -> {
                            try {
                                return admin.describeGroup("g").generation() == 2 && !replacement.received.isEmpty()
                                        && committedTotal(admin, "g", "orders") == replacement.received.size();
                            } catch (Exception e) {
                                return false;
                            }
                        });
                        List<PartitionOffset> before = admin.describeGroup("g").offsets();

                        CommitFailedException fenced = assertThrows(CommitFailedException.class, zombie::commitSync);
                        assertEquals(ErrorCode.ILLEGAL_GENERATION, fenced.errorCode(), "the zombie still believes in generation 1");
                        assertEquals(before, admin.describeGroup("g").offsets(), "the zombie changed nothing");
                    }
                    zombie.pauseHeartbeatsForTesting(false);
                    Await.until(30_000, "the zombie to rejoin on its next poll\n" + c.describe(), () -> {
                        try {
                            zombie.poll(Duration.ofMillis(100));
                            return zombie.generation() >= 2 && !zombie.assignment().isEmpty();
                        } catch (Exception e) {
                            return false;
                        }
                    });
                }
            }
        }
    }

    // ------------------------------------------------------------------ broker failure

    @Test
    void whenTheCoordinatorBrokerDiesTheMembersKeepTheirIdsAndNothingIsLost(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "orders", 6);
            try (GroupMember a = new GroupMember(c, "g", "a", 4000, 800, true, 200, "orders");
                 GroupMember b = new GroupMember(c, "g", "b", 4000, 800, true, 200, "orders");
                 GroupMember cc = new GroupMember(c, "g", "c", 4000, 800, true, 200, "orders")) {
                awaitStable(c, 6, a, b, cc);
                String[] ids = {a.consumer.memberId(), b.consumer.memberId(), cc.consumer.memberId()};
                EmberGroupConsumerTest.produce(c, "orders", 0, 60);
                awaitDelivered(c, values(0, 60), a, b, cc);
                awaitCommittedTotal(c, "orders", 60);

                int coordinator = c.awaitCoordinator();
                c.crash(coordinator);
                Await.until(30_000, "a new coordinator\n" + c.describe(), () -> c.coordinator() > 0 && c.coordinator() != coordinator);

                EmberGroupConsumerTest.produce(c, "orders", 60, 120);            // the partitions led by the dead broker move too
                awaitDelivered(c, values(0, 120), a, b, cc);
                awaitCommittedTotal(c, "orders", 120);                           // commits succeed against the new coordinator
                assertArrayEquals(ids, new String[]{a.consumer.memberId(), b.consumer.memberId(), cc.consumer.memberId()},
                        "the same members: group membership survived the coordinator failover");
                awaitStable(c, 6, a, b, cc);
            }
        }
    }

    @Test
    void aWholeClusterRestartKeepsGroupsGenerationsAndOffsets(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "orders", 3);
            try (GroupMember a = new GroupMember(c, "g", "a", 6000, 1000, true, 200, "orders")) {
                awaitStable(c, 3, a);
                EmberGroupConsumerTest.produce(c, "orders", 0, 45);
                awaitDelivered(c, values(0, 45), a);
                awaitCommittedTotal(c, "orders", 45);
                int generationBefore;
                try (ClusterClient admin = admin(c)) {
                    generationBefore = admin.describeGroup("g").generation();
                }

                for (int i = 1; i <= 3; i++) c.stop(i);
                for (int i = 1; i <= 3; i++) c.restart(i);
                c.awaitController();
                c.awaitCoordinator();
                c.awaitTopicEverywhere("orders", 3);
                for (int p = 0; p < 3; p++) c.awaitLeader("orders", p);

                try (ClusterClient admin = admin(c)) {
                    Await.until(30_000, "the group state to be replayed after the restart\n" + c.describe(), () -> {
                        try {
                            DescribeGroupResponse d = admin.describeGroup("g");
                            return d.generation() >= generationBefore && committedTotal(admin, "g", "orders") == 45;
                        } catch (Exception e) {
                            return false;
                        }
                    });
                }
                EmberGroupConsumerTest.produce(c, "orders", 45, 90);
                awaitDelivered(c, values(0, 90), a);                             // the member rejoined if its session ran out, and went on
                awaitCommittedTotal(c, "orders", 90);
                assertEquals(90, new TreeSet<>(a.values()).size());
                awaitStable(c, 3, a);
                c.awaitGroupStateIdentical();
            }
        }
    }

    // ------------------------------------------------------------------ retention

    @Test
    void aCommittedOffsetBelowTheLogStartFollowsTheResetPolicy(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false, "--retention-bytes=6000")) {
            ClusterIntegrationTest.createTopic(c, "log", 1);
            EmberGroupConsumerTest.produce(c, "log", 0, 10);
            try (EmberGroupConsumer first = new EmberGroupConsumer(EmberGroupConsumerTest.groupConfig(c, "g"))) {
                first.subscribe("log");
                assertEquals(10, EmberGroupConsumerTest.drain(first, 10, 30_000).size());
                first.commitSync();                                              // committed offset 10
            }
            EmberGroupConsumerTest.produce(c, "log", 10, 700);
            long now = System.currentTimeMillis();
            for (int i = 1; i <= 3; i++) c.servers[i].replicaManager().runMaintenance(now);   // retention by size removes old segments
            long logStart;
            try (ClusterClient admin = admin(c)) {
                logStart = admin.describe("log").get(0).logStartOffset();
            }
            assertTrue(logStart > 10, "retention must have removed the committed position (logStart=" + logStart + ")");

            try (EmberGroupConsumer earliest = new EmberGroupConsumer(EmberGroupConsumerTest.groupConfig(c, "g"))) {
                earliest.subscribe("log");
                List<GroupRecord> got = EmberGroupConsumerTest.drain(earliest, 1, 30_000);
                assertEquals(logStart, got.get(0).offset(), "earliest = the oldest record that still exists");
            }
            try (EmberGroupConsumer strict = new EmberGroupConsumer(
                    EmberGroupConsumerTest.groupConfig(c, "g").withOffsetReset(GroupConsumerConfig.OffsetReset.NONE))) {
                strict.subscribe("log");
                EmberException e = assertThrows(EmberException.class, () -> strict.poll(Duration.ofMillis(500)));
                assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, e.errorCode());
            }
            try (EmberGroupConsumer latest = new EmberGroupConsumer(
                    EmberGroupConsumerTest.groupConfig(c, "g").withOffsetReset(GroupConsumerConfig.OffsetReset.LATEST))) {
                latest.subscribe("log");
                assertTrue(latest.poll(Duration.ofMillis(500)).isEmpty(), "latest = nothing old");
                assertEquals(700, latest.position("log", 0));
            }
        }
    }

    @Test
    void consumersThatSubscribeBeforeTheTopicExistsGetItsPartitionsOnceItIsCreated(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            c.awaitCoordinator();
            try (GroupMember a = member(c, "a", true, "late"); GroupMember b = member(c, "b", true, "late")) {
                Await.until(20_000, "both consumers to have joined an (empty) group\n" + c.describe(), () -> a.consumer.generation() > 0 && b.consumer.generation() > 0);
                assertTrue(a.consumer.assignment().isEmpty());
                ClusterIntegrationTest.createTopic(c, "late", 4);
                EmberGroupConsumerTest.produce(c, "late", 0, 40);
                awaitStable(c, 4, a, b);                                          // the coordinator noticed the topic by itself
                awaitDelivered(c, values(0, 40), a, b);
            }
        }
    }
}
