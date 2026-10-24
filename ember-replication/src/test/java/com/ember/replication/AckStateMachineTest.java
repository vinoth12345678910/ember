package com.ember.replication;

import com.ember.protocol.QueueHandle;
import com.ember.raft.LogEntry;
import org.junit.jupiter.api.Test;

import java.util.*;

import static com.ember.replication.AckStateMachine.Result.*;
import static org.junit.jupiter.api.Assertions.*;

class AckStateMachineTest {

    private final AckStateMachine sm = new AckStateMachine();
    private long index = 0;

    private Object apply(byte[] command) {
        return sm.apply(++index, LogEntry.command(1, command));
    }

    private Object create(String sub, long... starts) {
        List<Long> s = new ArrayList<>();
        for (long x : starts) s.add(x);
        return apply(AckCommands.encode(new AckCommands.Create(sub, "t", 30_000, s)));
    }

    private static AckCommands.Allotment fresh(int partition, String worker, long deadline, long from, int count) {
        return new AckCommands.Allotment(partition, worker, deadline, from, count, List.of());
    }

    @SuppressWarnings("unchecked")
    private List<AckStateMachine.Result> claim(String sub, AckCommands.Allotment... a) {
        return (List<AckStateMachine.Result>) apply(AckCommands.encode(new AckCommands.Claim(sub, List.of(a))));
    }

    private AckStateMachine.SettleOutcome settle(byte type, String sub, String worker, long deadline, QueueHandle... h) {
        return (AckStateMachine.SettleOutcome) apply(AckCommands.encode(new AckCommands.Settle(type, sub, worker, deadline, List.of(h))));
    }

    private AckStateMachine.SettleOutcome ack(String sub, String worker, QueueHandle... h) {
        return settle(AckCommands.ACK, sub, worker, 0, h);
    }

    private AckStateMachine.SettleOutcome expire(String sub, long now, QueueHandle... h) {
        return (AckStateMachine.SettleOutcome) apply(AckCommands.encode(new AckCommands.Expire(sub, now, List.of(h))));
    }

    private static QueueHandle h(int partition, long offset, int attempt) {
        return new QueueHandle(partition, offset, attempt);
    }

    private AckStateMachine.PartitionView p(String sub, int partition) {
        return sm.view(sub).partitions().get(partition);
    }

    @Test
    void creatingASubscriptionTwiceOrWithBadInputIsRefused() {
        assertEquals(OK, create("s", 0, 0));
        assertEquals(ALREADY_EXISTS, create("s", 0, 0));
        assertEquals(INVALID, create("empty"));
        assertEquals(INVALID, create("neg", -1));
        assertEquals(2, sm.view("s").partitions().size());
        assertNull(sm.view("nope"));
    }

    @Test
    void claimThenAckAdvancesTheFloorOverContiguousFinishedOffsets() {
        create("s", 0);
        assertEquals(List.of(OK), claim("s", fresh(0, "w1", 1000, 0, 5)));
        assertEquals(5, p("s", 0).next());
        assertEquals(5, p("s", 0).inFlight());
        assertEquals(0, p("s", 0).floor());

        assertEquals(new AckStateMachine.SettleOutcome(2, 0), ack("s", "w1", h(0, 2, 1), h(0, 3, 1)));     // out of order: floor stays
        assertEquals(0, p("s", 0).floor());
        assertEquals(2, p("s", 0).doneAbove());
        assertEquals(new AckStateMachine.SettleOutcome(2, 0), ack("s", "w1", h(0, 0, 1), h(0, 1, 1)));
        assertEquals(4, p("s", 0).floor(), "0..3 are finished now");
        assertEquals(0, p("s", 0).doneAbove());
        assertEquals(1, p("s", 0).inFlight());
        ack("s", "w1", h(0, 4, 1));
        assertEquals(5, p("s", 0).floor());
        assertEquals(0, p("s", 0).inFlight());
    }

    @Test
    void anAckIsIdempotentAndCannotBeForgedByAnotherWorkerOrAttempt() {
        create("s", 0);
        claim("s", fresh(0, "w1", 1000, 0, 2));
        assertEquals(new AckStateMachine.SettleOutcome(0, 1), ack("s", "w2", h(0, 0, 1)), "w2 does not hold offset 0");
        assertEquals(new AckStateMachine.SettleOutcome(0, 1), ack("s", "w1", h(0, 0, 7)), "wrong attempt");
        assertEquals(new AckStateMachine.SettleOutcome(1, 0), ack("s", "w1", h(0, 0, 1)));
        assertEquals(new AckStateMachine.SettleOutcome(1, 0), ack("s", "w1", h(0, 0, 1)), "second ack of a finished offset is a no-op success");
        assertEquals(new AckStateMachine.SettleOutcome(0, 1), ack("s", "w1", h(5, 0, 1)), "no such partition");
        assertEquals(new AckStateMachine.SettleOutcome(0, 1), ack("s", "w1", h(0, 99, 1)), "never claimed");
    }

    @Test
    void anExpiredLeaseIsRedeliveredWithTheNextAttemptAndTheOldWorkerIsFenced() {
        create("s", 0);
        claim("s", fresh(0, "w1", 1000, 0, 1));
        assertEquals(new AckStateMachine.SettleOutcome(0, 1), expire("s", 999, h(0, 0, 1)), "not due yet");
        assertEquals(new AckStateMachine.SettleOutcome(1, 0), expire("s", 1000, h(0, 0, 1)));
        assertEquals(List.of(new AckStateMachine.Redelivery(0, 1)), sm.redeliveries("s", 0, 10));
        assertEquals(1, p("s", 0).redeliver());
        assertEquals(0, p("s", 0).inFlight());

        // w2 takes it: attempt 2
        assertEquals(List.of(OK), claim("s", new AckCommands.Allotment(0, "w2", 5000, 0, 0, List.of(0L))));
        assertEquals(0, p("s", 0).redeliver());
        // the late ack of w1 (attempt 1) is rejected, w2's (attempt 2) counts
        assertEquals(new AckStateMachine.SettleOutcome(0, 1), ack("s", "w1", h(0, 0, 1)));
        assertEquals(0, p("s", 0).floor());
        assertEquals(new AckStateMachine.SettleOutcome(1, 0), ack("s", "w2", h(0, 0, 2)));
        assertEquals(1, p("s", 0).floor());
    }

    @Test
    void aLateAckStillCountsIfNobodyElseHasTakenTheMessageYet() {
        create("s", 0);
        claim("s", fresh(0, "w1", 1000, 0, 1));
        expire("s", 2000, h(0, 0, 1));                                   // expired, waiting in the redeliver set
        assertEquals(new AckStateMachine.SettleOutcome(1, 0), ack("s", "w1", h(0, 0, 1)), "the work was done: no needless duplicate");
        assertEquals(0, sm.redeliveries("s", 0, 10).size());
        assertEquals(1, p("s", 0).floor());
    }

    @Test
    void anExpiryForAnAlreadyAckedOrRenewedLeaseDoesNothing() {
        create("s", 0);
        claim("s", fresh(0, "w1", 1000, 0, 2));
        ack("s", "w1", h(0, 0, 1));
        settle(AckCommands.EXTEND, "s", "w1", 9000, h(0, 1, 1));
        assertEquals(new AckStateMachine.SettleOutcome(0, 2), expire("s", 5000, h(0, 0, 1), h(0, 1, 1)));
        assertEquals(1, p("s", 0).inFlight());
        assertEquals(0, p("s", 0).redeliver());
    }

    @Test
    void releaseGivesAMessageBackAndExtendOnlyMovesTheDeadlineForward() {
        create("s", 0);
        claim("s", fresh(0, "w1", 1000, 0, 2));
        assertEquals(new AckStateMachine.SettleOutcome(1, 0), settle(AckCommands.RELEASE, "s", "w1", 0, h(0, 0, 1)));
        assertEquals(new AckStateMachine.SettleOutcome(1, 0), settle(AckCommands.RELEASE, "s", "w1", 0, h(0, 0, 1)), "idempotent");
        assertEquals(new AckStateMachine.SettleOutcome(0, 1), settle(AckCommands.RELEASE, "s", "w2", 0, h(0, 1, 1)), "not the holder");
        assertEquals(List.of(new AckStateMachine.Redelivery(0, 1)), sm.redeliveries("s", 0, 10));

        assertEquals(new AckStateMachine.SettleOutcome(1, 0), settle(AckCommands.EXTEND, "s", "w1", 9000, h(0, 1, 1)));
        assertEquals(new AckStateMachine.SettleOutcome(1, 0), settle(AckCommands.EXTEND, "s", "w1", 10, h(0, 1, 1)), "a shorter deadline is ignored, still accepted");
        assertTrue(sm.expiredLeases(8999, 10).isEmpty(), "the shorter deadline must not have replaced 9000");
        assertEquals(1, sm.expiredLeases(9000, 10).get("s").size());
    }

    @Test
    void aStaleClaimIsRejectedWholeAndChangesNothing() {
        create("s", 0);
        claim("s", fresh(0, "w1", 1000, 0, 3));
        // a leader that planned against old state tries to hand out 0..1 again and a redelivery that does not exist
        assertEquals(List.of(STALE), claim("s", fresh(0, "w2", 1000, 1, 2)));
        assertEquals(List.of(STALE), claim("s", new AckCommands.Allotment(0, "w2", 1000, 3, 2, List.of(77L))), "the fresh part must not be applied either");
        assertEquals(3, p("s", 0).next());
        assertEquals(3, p("s", 0).inFlight());
        assertEquals(List.of(OK, STALE), claim("s", fresh(0, "w3", 1000, 3, 1), fresh(0, "w4", 1000, 3, 1)), "allotments are independent, in order");
    }

    @Test
    void skippedOffsetsThatRetentionRemovedAreMarkedFinished() {
        create("s", 0);
        assertEquals(List.of(OK), claim("s", fresh(0, "w1", 1000, 10, 2)));        // the log starts at 10: 0..9 are gone
        assertEquals(12, p("s", 0).next());
        assertEquals(10, p("s", 0).floor(), "0..9 finished, 10..11 in flight");
        ack("s", "w1", h(0, 10, 1), h(0, 11, 1));
        assertEquals(12, p("s", 0).floor());
    }

    @Test
    void manyWorkersOnOnePartitionGetDisjointOffsets() {
        create("s", 0);
        long next = 0;
        Set<Long> seen = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            assertEquals(List.of(OK), claim("s", fresh(0, "w" + i, 1000, next, 5)));
            for (long o = next; o < next + 5; o++) assertTrue(seen.add(o));
            next += 5;
        }
        assertEquals(100, p("s", 0).inFlight());
    }

    @Test
    void unknownSubscriptionsAndMalformedCommandsAreAnswered() {
        assertEquals(UNKNOWN_SUBSCRIPTION, apply(AckCommands.encode(new AckCommands.Claim("nope", List.of(fresh(0, "w", 1, 0, 1))))));
        assertEquals(UNKNOWN_SUBSCRIPTION, apply(AckCommands.encode(new AckCommands.Settle(AckCommands.ACK, "nope", "w", 0, List.of()))));
        assertEquals(UNKNOWN_SUBSCRIPTION, apply(AckCommands.encode(new AckCommands.Delete("nope"))));
        assertEquals(INVALID, apply(new byte[]{AckCommands.CLAIM, 0, 5}));
        create("s", 0);
        assertEquals(List.of(INVALID), claim("s", fresh(3, "w", 1000, 0, 1)), "no such partition");
        assertEquals(List.of(INVALID), claim("s", fresh(0, "", 1000, 0, 1)), "empty worker");
        assertEquals(OK, apply(AckCommands.encode(new AckCommands.Delete("s"))));
        assertNull(sm.view("s"));
    }

    @Test
    void replayingTheSameLogOnAnotherReplicaGivesTheSameState() {
        List<byte[]> log = new ArrayList<>();
        log.add(AckCommands.encode(new AckCommands.Create("s", "t", 30_000, List.of(0L, 0L))));
        Random random = new Random(42);
        long[] next = {0, 0};
        for (int i = 0; i < 300; i++) {
            int part = random.nextInt(2);
            int n = 1 + random.nextInt(4);
            log.add(AckCommands.encode(new AckCommands.Claim("s", List.of(fresh(part, "w" + random.nextInt(3), 100 + i, next[part], n)))));
            next[part] += n;
            for (int k = 0; k < 3; k++) {
                long off = random.nextInt((int) next[part]);
                log.add(AckCommands.encode(new AckCommands.Settle(random.nextBoolean() ? AckCommands.ACK : AckCommands.RELEASE, "s", "w" + random.nextInt(3), 0,
                        List.of(h(part, off, 1 + random.nextInt(2))))));
            }
            if (i % 10 == 0) {
                log.add(AckCommands.encode(new AckCommands.Expire("s", i * 2L, List.of(h(part, random.nextInt((int) next[part]), 1)))));
            }
        }
        AckStateMachine a = new AckStateMachine(), b = new AckStateMachine();
        long i = 0;
        for (byte[] c : log) {
            Object ra = a.apply(++i, LogEntry.command(1, c));
            Object rb = b.apply(i, LogEntry.command(1, c));
            assertEquals(String.valueOf(ra), String.valueOf(rb));
        }
        assertEquals(a.fingerprint(), b.fingerprint());
        assertFalse(a.fingerprint().isEmpty());
    }

    @Test
    void commandCodecRoundTripsAndRejectsGarbage() {
        AckCommands.Claim c = new AckCommands.Claim("s", List.of(new AckCommands.Allotment(1, "wörker", 99, 5, 3, List.of(1L, 2L))));
        assertEquals(c, AckCommands.decodeClaim(AckCommands.encode(c)));
        AckCommands.Settle s = new AckCommands.Settle(AckCommands.EXTEND, "s", "w", 77, List.of(h(0, 1, 2)));
        assertEquals(s, AckCommands.decodeSettle(AckCommands.encode(s)));
        AckCommands.Expire e = new AckCommands.Expire("s", 5, List.of(h(0, 1, 2)));
        assertEquals(e, AckCommands.decodeExpire(AckCommands.encode(e)));
        AckCommands.Create cr = new AckCommands.Create("s", "t", 1000, List.of(0L, 7L));
        assertEquals(cr, AckCommands.decodeCreate(AckCommands.encode(cr)));
        assertThrows(IllegalArgumentException.class, () -> AckCommands.decodeClaim(new byte[]{AckCommands.CLAIM, 0, 1, 's', 0x7f, 0, 0, 0}));
        byte[] trailing = Arrays.copyOf(AckCommands.encode(new AckCommands.Delete("s")), 10);
        assertThrows(IllegalArgumentException.class, () -> AckCommands.decodeDelete(trailing));
    }
}
