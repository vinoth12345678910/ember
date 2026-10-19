package com.ember.broker;

import com.ember.client.*;
import com.ember.protocol.DescribeGroupResponse;
import com.ember.protocol.PartitionRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/** The group consumer against a real 3-broker cluster. Rebalance / crash / failover scenarios are in GroupScenariosTest. */
class EmberGroupConsumerTest {

    static GroupConsumerConfig groupConfig(TestCluster c, String group) {
        return GroupConsumerConfig.of(List.of(c.bootstrap().split(",")), group)
                .withClient(ClusterIntegrationTest.cfg(c))
                .withSession(3000, 500)
                .withAutoCommit(false, 5000);
    }

    static void produce(TestCluster c, String topic, int from, int to) throws Exception {
        try (EmberProducer producer = new EmberProducer(ClusterIntegrationTest.cfg(c))) {
            for (int i = from; i < to; i++) producer.send(topic, ("k" + i).getBytes(), ("v" + i).getBytes());
        }
    }

    /** Polls until `count` records arrived (more is a failure of the test's expectation, fewer a timeout). */
    static List<GroupRecord> drain(EmberGroupConsumer consumer, int count, long timeoutMs) {
        List<GroupRecord> all = new ArrayList<>();
        Await.until(timeoutMs, count + " records (got " + all.size() + ")", () -> {
            try {
                all.addAll(consumer.poll(Duration.ofMillis(100)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return all.size() >= count;
        });
        return all;
    }

    @Test
    void oneConsumerReadsEverythingCommitsAndANewOneResumesAfterIt(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "orders", 6);
            produce(c, "orders", 0, 120);

            try (EmberGroupConsumer consumer = new EmberGroupConsumer(groupConfig(c, "g"))) {
                consumer.subscribe("orders");
                List<GroupRecord> first = drain(consumer, 120, 30_000);
                assertEquals(120, first.size());
                assertEquals(6, consumer.assignment().size());
                assertEquals(1, consumer.generation());
                Set<String> values = new HashSet<>();
                first.forEach(r -> values.add(new String(r.value())));
                assertEquals(120, values.size(), "every record exactly once");
                consumer.commitSync();
                long total = 0;
                for (PartitionRef p : consumer.assignment()) total += consumer.position(p.topic(), p.partition());
                assertEquals(120, total);
            }

            produce(c, "orders", 120, 150);
            try (EmberGroupConsumer again = new EmberGroupConsumer(groupConfig(c, "g"))) {
                again.subscribe("orders");
                List<GroupRecord> second = drain(again, 30, 30_000);
                assertEquals(30, second.size(), "only the records produced after the commit");
                for (GroupRecord r : second) {
                    int n = Integer.parseInt(new String(r.value()).substring(1));
                    assertTrue(n >= 120, "record " + n + " was committed and must not be read again");
                }
                assertTrue(again.poll(Duration.ofMillis(300)).isEmpty());
            }
        }
    }

    @Test
    void closeCommitsWhenAutoCommitIsOnAndLeavesTheGroup(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "orders", 3);
            produce(c, "orders", 0, 30);
            try (EmberGroupConsumer consumer = new EmberGroupConsumer(groupConfig(c, "g").withAutoCommit(true, 3_600_000))) {
                consumer.subscribe("orders");
                drain(consumer, 30, 30_000);
            }
            try (ClusterClient admin = new ClusterClient(ClusterIntegrationTest.cfg(c))) {
                DescribeGroupResponse d = admin.describeGroup("g");
                assertEquals(DescribeGroupResponse.EMPTY, d.state(), "close() left the group");
                long committed = 0;
                for (var o : d.offsets()) committed += o.offset();
                assertEquals(30, committed, "close() committed the auto-commit positions");
            }
        }
    }

    @Test
    void aTopicThatDoesNotExistGivesAnEmptyAssignmentAndNoCrash(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            try (EmberGroupConsumer consumer = new EmberGroupConsumer(groupConfig(c, "g"))) {
                consumer.subscribe("not-yet");
                assertTrue(consumer.poll(Duration.ofMillis(300)).isEmpty());
                assertTrue(consumer.assignment().isEmpty());
                assertEquals(1, consumer.generation());
            }
        }
    }

    @Test
    void withoutACommittedOffsetTheResetPolicyDecidesWhereToStart(@TempDir Path dir) throws Exception {
        try (TestCluster c = new TestCluster(dir, 3, false)) {
            ClusterIntegrationTest.createTopic(c, "orders", 2);
            produce(c, "orders", 0, 20);
            try (EmberGroupConsumer latest = new EmberGroupConsumer(groupConfig(c, "late").withOffsetReset(GroupConsumerConfig.OffsetReset.LATEST))) {
                latest.subscribe("orders");
                assertTrue(latest.poll(Duration.ofMillis(300)).isEmpty(), "latest skips what is already there");
                produce(c, "orders", 20, 25);
                assertEquals(5, drain(latest, 5, 20_000).size());
            }
            try (EmberGroupConsumer none = new EmberGroupConsumer(groupConfig(c, "strict").withOffsetReset(GroupConsumerConfig.OffsetReset.NONE))) {
                none.subscribe("orders");
                EmberException e = assertThrows(EmberException.class, () -> none.poll(Duration.ofMillis(300)));
                assertEquals(com.ember.protocol.ErrorCode.OFFSET_OUT_OF_RANGE, e.errorCode());
            }
        }
    }
}
