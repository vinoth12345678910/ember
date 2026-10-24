package com.ember.replication;

import com.ember.protocol.ErrorCode;
import com.ember.protocol.QueueHandle;
import com.ember.protocol.QueueMessage;
import com.ember.raft.LogEntry;
import com.ember.storage.LogRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class AckCoordinatorTest {

    /** Applies each proposal straight to the state machine, like a one-node Raft group that commits instantly. */
    static final class FakeLog implements GroupLog {
        final AckStateMachine state;
        boolean leader = true, ready = true;
        int leaderId = 1;
        long index;
        final List<byte[]> proposals = new ArrayList<>();
        Runnable beforeNextApply;                                  // lets a test sneak a competing command in, to force a stale claim

        FakeLog(AckStateMachine state) {
            this.state = state;
        }

        public boolean isLeader() { return leader; }
        public boolean isLeaderReady() { return ready; }
        public int leaderId() { return leaderId; }
        public long term() { return 1; }

        public CompletableFuture<Object> propose(byte[] command, long timeoutMs) {
            if (beforeNextApply != null) {
                Runnable r = beforeNextApply;
                beforeNextApply = null;
                r.run();
            }
            proposals.add(command);
            return CompletableFuture.completedFuture(state.apply(++index, LogEntry.command(1, command)));
        }
    }

    static final class FakeData implements AckCoordinator.QueueData {
        final Map<Integer, List<LogRecord>> partitions = new HashMap<>();
        final Map<Integer, Long> start = new HashMap<>();
        int partitionCount = 2;

        void add(int partition, int count, String prefix) {
            List<LogRecord> l = partitions.computeIfAbsent(partition, p -> new ArrayList<>());
            for (int i = 0; i < count; i++) {
                long offset = start.getOrDefault(partition, 0L) + l.size();
                l.add(new LogRecord(offset, 1000 + offset, ("k" + offset).getBytes(), (prefix + partition + ":" + offset).getBytes()));
            }
        }

        public int partitionCount(String topic) { return topic.equals("t") ? partitionCount : -1; }
        public long logStart(String topic, int p) { return start.getOrDefault(p, 0L); }
        public long logEnd(String topic, int p) { return logStart(topic, p) + partitions.getOrDefault(p, List.of()).size(); }
        public LogRecord read(String topic, int p, long offset) {
            long i = offset - logStart(topic, p);
            List<LogRecord> l = partitions.getOrDefault(p, List.of());
            return i >= 0 && i < l.size() ? l.get((int) i) : null;
        }
    }

    AckStateMachine state;
    FakeLog log;
    FakeData data;
    AckCoordinator coordinator;
    long now;

    @BeforeEach
    void setUp() {
        state = new AckStateMachine();
        log = new FakeLog(state);
        data = new FakeData();
        now = 1_000_000;
        coordinator = new AckCoordinator("test", log, state, data, () -> now, new AckCoordinator.Config(30_000, 1000, 5, 1000));
    }

    private static String text(QueueMessage m) {
        return new String(m.value());
    }

    @Test
    void claimReturnsTheRecordsInlineAndNeverTheSameMessageTwice() {
        data.add(0, 4, "a");
        data.add(1, 3, "b");
        assertEquals(ErrorCode.NONE, coordinator.create("s", "t", true, 0).error());
        AckCoordinator.ClaimResult c1 = coordinator.claim("s", "w1", 5, 0, 1 << 20);
        AckCoordinator.ClaimResult c2 = coordinator.claim("s", "w2", 5, 0, 1 << 20);
        assertEquals(ErrorCode.NONE, c1.error());
        assertEquals(5, c1.messages().size());
        assertEquals(2, c2.messages().size(), "only 7 messages exist");
        Set<String> all = new HashSet<>();
        for (QueueMessage m : c1.messages()) assertTrue(all.add(text(m)));
        for (QueueMessage m : c2.messages()) assertTrue(all.add(text(m)));
        assertEquals(7, all.size());
        assertTrue(c1.messages().stream().allMatch(m -> m.attempt() == 1));
        assertEquals(0, coordinator.claim("s", "w3", 5, 0, 1 << 20).messages().size(), "nothing left");
        assertEquals("k0", new String(c1.messages().stream().filter(m -> m.partition() == 0 && m.offset() == 0).findFirst().orElseThrow().key()));
    }

    @Test
    void ackedMessagesAreGoneAndUnackedOnesComeBackAfterTheLeaseExpires() {
        data.add(0, 3, "a");
        data.partitionCount = 1;
        coordinator.create("s", "t", true, 10_000);
        List<QueueMessage> got = coordinator.claim("s", "w1", 10, 0, 1 << 20).messages();
        assertEquals(3, got.size());
        AckCoordinator.SettleResult ack = coordinator.settle(AckCommands.ACK, "s", "w1", 0, List.of(got.get(0).handle()));
        assertEquals(1, ack.settled());

        now += 9_999;
        coordinator.expireLeases();
        assertEquals(0, coordinator.claim("s", "w2", 10, 0, 1 << 20).messages().size(), "leases are still valid");
        now += 1;
        coordinator.expireLeases();
        List<QueueMessage> again = coordinator.claim("s", "w2", 10, 0, 1 << 20).messages();
        assertEquals(2, again.size(), "the acked one must not come back");
        assertTrue(again.stream().allMatch(m -> m.attempt() == 2));
        assertEquals(Set.of(1L, 2L), new HashSet<>(again.stream().map(QueueMessage::offset).toList()));

        // the old worker's late ack is fenced, the new worker's counts
        assertEquals(2, coordinator.settle(AckCommands.ACK, "s", "w1", 0, List.of(got.get(1).handle(), got.get(2).handle())).rejected());
        assertEquals(2, coordinator.settle(AckCommands.ACK, "s", "w2", 0, again.stream().map(QueueMessage::handle).toList()).settled());
        assertEquals(3, coordinator.describe("s").body().partitions().get(0).floor());
    }

    @Test
    void releaseMakesAMessageAvailableAtOnceAndExtendKeepsItAlive() {
        data.add(0, 2, "a");
        data.partitionCount = 1;
        coordinator.create("s", "t", true, 10_000);
        List<QueueMessage> got = coordinator.claim("s", "w1", 10, 0, 1 << 20).messages();
        assertEquals(1, coordinator.settle(AckCommands.RELEASE, "s", "w1", 0, List.of(got.get(0).handle())).settled());
        List<QueueMessage> back = coordinator.claim("s", "w2", 10, 0, 1 << 20).messages();
        assertEquals(1, back.size());
        assertEquals(0, back.get(0).offset());
        assertEquals(2, back.get(0).attempt());

        now += 9_000;
        assertEquals(1, coordinator.settle(AckCommands.EXTEND, "s", "w1", 60_000, List.of(got.get(1).handle())).settled());
        now += 5_000;                                                      // past the original 10 s, inside the extended 60 s
        coordinator.expireLeases();
        var part = coordinator.describe("s").body().partitions().get(0);
        assertEquals(1, part.inFlight(), "w1's extended lease on offset 1 is alive");
        assertEquals(1, part.redeliver(), "w2's 10 s lease on offset 0 ran out");
    }

    @Test
    void aClaimThatRacedWithAnotherIsReplannedAndStillSucceeds() {
        data.add(0, 6, "a");
        data.partitionCount = 1;
        coordinator.create("s", "t", true, 0);
        // between planning and applying, another claimant (e.g. an earlier leader's in-flight proposal) takes offsets 0..1
        log.beforeNextApply = () -> state.apply(++log.index, LogEntry.command(1,
                AckCommands.encode(new AckCommands.Claim("s", List.of(new AckCommands.Allotment(0, "rival", now + 1000, 0, 2, List.of()))))));
        List<QueueMessage> got = coordinator.claim("s", "w1", 3, 0, 1 << 20).messages();
        assertEquals(3, got.size());
        assertEquals(List.of(2L, 3L, 4L), got.stream().map(QueueMessage::offset).toList(), "re-planned after the rival took 0..1");
    }

    @Test
    void aSubscriptionStartingAtTheEndOnlySeesNewMessages() {
        data.add(0, 5, "old");
        data.partitionCount = 1;
        coordinator.create("late", "t", false, 0);
        assertEquals(0, coordinator.claim("late", "w", 10, 0, 1 << 20).messages().size());
        data.add(0, 2, "new");
        List<QueueMessage> got = coordinator.claim("late", "w", 10, 0, 1 << 20).messages();
        assertEquals(List.of(5L, 6L), got.stream().map(QueueMessage::offset).toList());
    }

    @Test
    void maxBytesBoundsTheBatchButAtLeastOneMessageIsAlwaysReturned() {
        data.add(0, 10, "a");
        data.partitionCount = 1;
        coordinator.create("s", "t", true, 0);
        int got = coordinator.claim("s", "w", 100, 0, 1).messages().size();
        assertEquals(1, got, "a tiny byte budget still delivers one message");
        int more = coordinator.claim("s", "w", 100, 0, 100).messages().size();
        assertTrue(more >= 1 && more < 9, "100 bytes fit only a few ~40 byte messages, got " + more);
    }

    @Test
    void messagesRemovedByRetentionAreSkippedAndFinished() {
        data.add(0, 5, "a");
        data.partitionCount = 1;
        coordinator.create("s", "t", true, 0);
        List<QueueMessage> first = coordinator.claim("s", "w1", 2, 10, 1 << 20).messages();      // 10 ms lease
        assertEquals(2, first.size());
        now += 100;
        coordinator.expireLeases();                                      // 0 and 1 wait for redelivery...
        data.start.put(0, 3L);                                           // ...but retention deleted offsets 0..2
        data.partitions.put(0, new ArrayList<>(data.partitions.get(0).subList(3, 5)));
        List<QueueMessage> got = coordinator.claim("s", "w2", 10, 0, 1 << 20).messages();
        assertEquals(List.of(3L, 4L), got.stream().map(QueueMessage::offset).toList(), "gone messages are not delivered");
        AckStateMachine.PartitionView v = state.view("s").partitions().get(0);
        assertEquals(0, v.redeliver(), "they did not stay in the redeliver set for ever");
        assertEquals(3, v.floor(), "0..2 are finished");
    }

    @Test
    void onlyTheReadyLeaderServesAndErrorsTellTheClientWhatToDo() {
        data.add(0, 1, "a");
        log.leader = false;
        assertEquals(ErrorCode.NOT_QUEUE_LEADER, coordinator.claim("s", "w", 1, 0, 10).error());
        log.leaderId = -1;
        assertEquals(ErrorCode.QUEUE_LEADER_NOT_AVAILABLE, coordinator.create("s", "t", true, 0).error());
        log.leader = true;
        log.ready = false;
        assertEquals(ErrorCode.QUEUE_LEADER_NOT_AVAILABLE, coordinator.settle(AckCommands.ACK, "s", "w", 0, List.of()).error());
        log.ready = true;
        assertEquals(ErrorCode.UNKNOWN_SUBSCRIPTION, coordinator.claim("s", "w", 1, 0, 10).error());
        assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, coordinator.create("s", "missing", true, 0).error());
        assertEquals(ErrorCode.NONE, coordinator.create("s", "t", true, 0).error());
        assertEquals(ErrorCode.SUBSCRIPTION_ALREADY_EXISTS, coordinator.create("s", "t", true, 0).error());
        assertEquals(ErrorCode.UNKNOWN_SUBSCRIPTION, coordinator.describe("nope").error());
    }

    @Test
    void expiryTicksProposeNothingWhenNothingIsDue() {
        data.add(0, 2, "a");
        data.partitionCount = 1;
        coordinator.create("s", "t", true, 10_000);
        coordinator.claim("s", "w", 2, 0, 1 << 20);
        int before = log.proposals.size();
        coordinator.expireLeases();
        assertEquals(before, log.proposals.size());
        log.leader = false;
        now += 100_000;
        coordinator.expireLeases();
        assertEquals(before, log.proposals.size(), "a follower never proposes expiries");
    }

    @Test
    void aNackedMessageIsNotHandedOutAgainUntilItsBackoffIsOver() {
        data.add(0, 2, "a");
        data.partitionCount = 1;
        coordinator.create("s", "t", true, 0, 3, 1000, 4000);
        List<QueueMessage> got = coordinator.claim("s", "w1", 10, 0, 1 << 20).messages();
        assertEquals(2, got.size());
        assertEquals(1, coordinator.nack("s", "w1", "bank timeout", List.of(got.get(0).handle())).settled());
        assertEquals(1, coordinator.settle(AckCommands.ACK, "s", "w1", 0, List.of(got.get(1).handle())).settled());

        assertTrue(coordinator.claim("s", "w2", 10, 0, 1 << 20).messages().isEmpty(), "still backing off");
        now += 1_300;                                                      // 1000 ms base + at most 25% jitter
        List<QueueMessage> retry = coordinator.claim("s", "w2", 10, 0, 1 << 20).messages();
        assertEquals(1, retry.size());
        assertEquals(2, retry.get(0).attempt());
        assertEquals(0, retry.get(0).offset());
    }

    @Test
    void aMessageThatKeepsFailingEndsInTheDeadLetterListWithItsContentAndAdminsCanRetryOrDiscardIt() {
        data.add(0, 3, "a");
        data.partitionCount = 1;
        coordinator.create("s", "t", true, 0, 2, 100, 100);
        List<QueueMessage> got = coordinator.claim("s", "w", 10, 0, 1 << 20).messages();
        coordinator.nack("s", "w", "poison", List.of(got.get(1).handle()));
        coordinator.settle(AckCommands.ACK, "s", "w", 0, List.of(got.get(0).handle(), got.get(2).handle()));
        now += 500;
        List<QueueMessage> again = coordinator.claim("s", "w", 10, 0, 1 << 20).messages();
        assertEquals(1, again.size());
        coordinator.nack("s", "w", "still poison", List.of(again.get(0).handle()));

        AckCoordinator.DeadListResult dead = coordinator.deadList("s", 10);
        assertEquals(ErrorCode.NONE, dead.error());
        assertEquals(1, dead.total());
        assertEquals("still poison", dead.items().get(0).reason());
        assertEquals(1, dead.items().get(0).offset());
        assertEquals("a0:1", new String(dead.items().get(0).value()), "the content comes from the partition log");
        var d = coordinator.describe("s").body();
        assertEquals(1, d.partitions().get(0).dead());
        assertEquals(2, d.maxAttempts());
        assertEquals(100, d.backoffBaseMs());

        assertEquals(1, coordinator.deadAction(AckCommands.DEAD_RETRY, "s", List.of(new QueueHandle(0, 1, 0))).settled());
        assertEquals(0, coordinator.deadList("s", 10).total());
        List<QueueMessage> retried = coordinator.claim("s", "w2", 10, 0, 1 << 20).messages();
        assertEquals(1, retried.size());
        assertEquals(3, retried.get(0).attempt(), "attempt numbers keep growing so old acks stay fenced");
        coordinator.nack("s", "w2", "x", List.of(retried.get(0).handle()));
        now += 500;
        coordinator.claim("s", "w2", 10, 0, 1 << 20);
        coordinator.nack("s", "w2", "x", List.of(new QueueHandle(0, 1, 4)));
        assertEquals(1, coordinator.deadList("s", 10).total(), "it died again after a fresh budget of 2 failures");
        assertEquals(1, coordinator.deadAction(AckCommands.DEAD_DISCARD, "s", List.of(new QueueHandle(0, 1, 0))).settled());
        assertEquals(0, coordinator.deadList("s", 10).total());
        assertEquals(3, coordinator.describe("s").body().partitions().get(0).floor(), "discarded = finished, the floor moves on");
    }

    @Test
    void deadLetterCallsAnswerLikeTheOtherOperations() {
        assertEquals(ErrorCode.UNKNOWN_SUBSCRIPTION, coordinator.deadList("nope", 5).error());
        assertEquals(ErrorCode.UNKNOWN_SUBSCRIPTION, coordinator.nack("nope", "w", "r", List.of(new QueueHandle(0, 0, 1))).error());
        assertEquals(ErrorCode.UNKNOWN_SUBSCRIPTION, coordinator.deadAction(AckCommands.DEAD_RETRY, "nope", List.of(new QueueHandle(0, 0, 0))).error());
        log.leader = false;
        assertEquals(ErrorCode.NOT_QUEUE_LEADER, coordinator.deadList("nope", 5).error());
        assertEquals(ErrorCode.NOT_QUEUE_LEADER, coordinator.nack("nope", "w", "r", List.of()).error());
    }
}
