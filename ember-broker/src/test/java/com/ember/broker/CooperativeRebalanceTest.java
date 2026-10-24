package com.ember.broker;

import com.ember.client.GroupRecord;
import com.ember.protocol.DescribeGroupResponse;
import com.ember.protocol.PartitionRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Eager vs cooperative rebalancing on real brokers. The commit interval is made longer than the test so the ONLY offsets the
 * group ever stores come from a rebalance itself: a record is delivered twice exactly when a rebalance threw away progress.
 */
class CooperativeRebalanceTest {

    private static final int COMMIT_NEVER_MS = 10 * 60_000;

    private static GroupMember member(TestCluster c, String name, boolean cooperative) {
        return new GroupMember(c, "g", name, 3000, 300, true, COMMIT_NEVER_MS, cooperative, "orders");
    }

    /** records delivered more than once, summed over all members */
    private static int duplicates(GroupMember... members) {
        Map<String, Integer> seen = new HashMap<>();
        for (GroupMember m : members) {
            for (GroupRecord r : m.received) seen.merge(r.topic() + "-" + r.partition() + "@" + r.offset(), 1, Integer::sum);
        }
        int dup = 0;
        for (int n : seen.values()) dup += n - 1;
        return dup;
    }

    private static int run(Path dir, boolean cooperative) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false, "--group-rebalance=" + (cooperative ? "cooperative" : "eager"))) {
            ClusterIntegrationTest.createTopic(c, "orders", 6);
            try (GroupMember a = member(c, "a", cooperative); GroupMember b = member(c, "b", cooperative)) {
                GroupScenariosTest.awaitStable(c, 6, a, b);
                EmberGroupConsumerTest.produce(c, "orders", 0, 240);
                Set<String> first = new TreeSet<>();
                for (int i = 0; i < 240; i++) first.add("v" + i);
                GroupScenariosTest.awaitDelivered(c, first, a, b);

                // a third member joins: this is the rebalance under test
                try (GroupMember d = member(c, "d", cooperative)) {
                    DescribeGroupResponse three = GroupScenariosTest.awaitStable(c, 6, a, b, d);
                    for (DescribeGroupResponse.Member m : three.members()) assertEquals(2, m.assignment().size());
                    EmberGroupConsumerTest.produce(c, "orders", 240, 480);
                    Set<String> all = new TreeSet<>();
                    for (int i = 0; i < 480; i++) all.add("v" + i);
                    GroupScenariosTest.awaitDelivered(c, all, a, b, d);
                    Thread.sleep(500);                                   // let a late duplicate show up
                    return duplicates(a, b, d);
                }
            }
        }
    }

    @Test
    void anEagerRebalanceRepeatsEverythingThatWasNotCommittedYet(@TempDir Path dir) throws Exception {
        int dup = run(dir, false);
        System.out.println("eager: " + dup + " records delivered twice after one join");
        assertTrue(dup >= 100, "the eager protocol drops every position and starts again from the (empty) committed offsets: " + dup);
    }

    @Test
    void aCooperativeRebalanceRepeatsNothing(@TempDir Path dir) throws Exception {
        int dup = run(dir, true);
        System.out.println("cooperative: " + dup + " records delivered twice after one join");
        assertEquals(0, dup, "kept partitions are never interrupted, revoked ones are committed before the handoff");
    }

    @Test
    void everyPartitionKeepsExactlyOneOwnerWhileMembersComeAndGo(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false, "--group-rebalance=cooperative")) {
            ClusterIntegrationTest.createTopic(c, "orders", 6);
            try (GroupMember a = member(c, "a", true)) {
                GroupScenariosTest.awaitStable(c, 6, a);
                try (GroupMember b = member(c, "b", true); GroupMember d = member(c, "d", true)) {
                    GroupScenariosTest.awaitStable(c, 6, a, b, d);
                    EmberGroupConsumerTest.produce(c, "orders", 0, 120);
                    Set<String> all = new TreeSet<>();
                    for (int i = 0; i < 120; i++) all.add("v" + i);
                    GroupScenariosTest.awaitDelivered(c, all, a, b, d);
                    b.crash();                                           // dies without leaving: the session just expires
                    GroupScenariosTest.awaitStable(c, 6, a, d);
                }
            }
        }
    }
}
