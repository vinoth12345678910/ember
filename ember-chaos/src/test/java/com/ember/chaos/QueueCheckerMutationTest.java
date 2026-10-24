package com.ember.chaos;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.ember.chaos.H.row;
import static org.junit.jupiter.api.Assertions.*;

/**
 * The ack-queue properties (P10-P15) get the same treatment as P1-P9: a known-good history must report nothing, and each planted bug must be
 * caught with its own code. A checker that cannot fail proves nothing.
 *
 * The good history: 6 messages on 2 partitions. w1 claims four, acks two, nacks the poison one (p1-3) and dies holding p1-2. w2 gets p1-2 again
 * (attempt 2) and the poison message again, plus the two fresh ones; w1's late ack is refused (fenced). The poison message fails three times
 * and is dead-lettered; the operator retries it and it succeeds. Everything ends finished, and the three replicas agree.
 */
class QueueCheckerMutationTest {

    private static final Object[][] FINAL_BEFORE_FIX = {
            {0, 3L, 3L, 3L, 0, 0, 0L, 0},                       // partition 0: offsets 0..2 all finished
            {1, 1L, 3L, 3L, 0, 0, 1L, 1}};                      // partition 1: offset 0 finished, offset 1 dead, offset 2 finished above it
    private static final Object[][] FINAL_AFTER_FIX = {
            {0, 3L, 3L, 3L, 0, 0, 0L, 0},
            {1, 3L, 3L, 3L, 0, 0, 0L, 0}};

    private static H goodHistoryBuilder(Consumer<H> afterQueueOps) {
        H h = new H();
        String[] values = {"p1-0", "p1-1", "p1-2", "p1-3", "p1-4", "p1-5"};
        Object[][][] logs = {{row(0, "k0", "p1-0"), row(1, "k2", "p1-2"), row(2, "k4", "p1-4")}, {row(0, "k1", "p1-1"), row(1, "k3", "p1-3"), row(2, "k5", "p1-5")}};
        for (int i = 0; i < 6; i++) {
            h.produce("p1", "t", i % 2, "k" + i, values[i], Outcome.OK, i / 2);
        }
        h.claim("w1", new Object[]{0, 0L, 1, "p1-0"}, new Object[]{0, 1L, 1, "p1-2"}, new Object[]{1, 0L, 1, "p1-1"}, new Object[]{1, 1L, 1, "p1-3"});
        h.settle("w1", "ack", 0, 0, 1, Outcome.OK, 1, 0);
        h.settle("w1", "ack", 1, 0, 1, Outcome.OK, 1, 0);
        h.settle("w1", "nack", 1, 1, 1, Outcome.OK, 1, 0);                                  // poison: attempt 1 failed; w1 then "dies" holding p1-2
        h.claim("w2", new Object[]{0, 1L, 2, "p1-2"}, new Object[]{1, 1L, 2, "p1-3"}, new Object[]{0, 2L, 1, "p1-4"}, new Object[]{1, 2L, 1, "p1-5"});
        h.settle("w1", "ack", 0, 1, 1, Outcome.OK, 0, 1);                                   // w1's late ack: fenced, refused
        h.settle("w2", "ack", 0, 1, 2, Outcome.OK, 1, 0);
        h.settle("w2", "nack", 1, 1, 2, Outcome.OK, 1, 0);
        h.settle("w2", "ack", 0, 2, 1, Outcome.OK, 1, 0);
        h.settle("w2", "ack", 1, 2, 1, Outcome.OK, 1, 0);
        h.claim("w2", new Object[]{1, 1L, 3, "p1-3"});
        h.settle("w2", "nack", 1, 1, 3, Outcome.OK, 1, 0);                                  // third failure: dead-lettered
        h.queueFinal(false, 3, FINAL_BEFORE_FIX, new Object[][]{{1, 1L, 3L, "p1-3"}}, 1);
        h.claim("w2", new Object[]{1, 1L, 4, "p1-3"});                                      // after the operator's fix and retry
        h.settle("w2", "ack", 1, 1, 4, Outcome.OK, 1, 0);
        h.queueFinal(true, 3, FINAL_AFTER_FIX, new Object[][]{}, 0);
        for (int p = 0; p < 2; p++) h.read("final", "t", p, true, 0, 3, logs[p]);
        for (int r = 1; r <= 3; r++) h.replicaAck(r, 777);
        if (afterQueueOps != null) afterQueueOps.accept(h);
        return h;
    }

    private static History good() {
        return goodHistoryBuilder(null).history();
    }

    private static History mutate(java.util.function.Function<List<HistoryEvent>, List<HistoryEvent>> change) {
        return new History(change.apply(new ArrayList<>(good().events())));
    }

    private static HistoryEvent withData(HistoryEvent e, Map<String, Object> data) {
        return new HistoryEvent(e.timeNanos(), e.kind(), e.opId(), e.process(), e.type(), e.outcome(), data);
    }

    /** Replaces the data of the n-th event of this kind/type/process-agnostic match. */
    private static History editNth(String kind, String type, int n, Consumer<Map<String, Object>> edit) {
        return mutate(ev -> {
            int seen = 0;
            for (int i = 0; i < ev.size(); i++) {
                HistoryEvent e = ev.get(i);
                if (e.kind().equals(kind) && e.type().equals(type) && seen++ == n) {
                    Map<String, Object> d = new LinkedHashMap<>(e.data());
                    edit.accept(d);
                    ev.set(i, withData(e, d));
                    return ev;
                }
            }
            throw new AssertionError("no such event");
        });
    }

    private static void assertCaught(History bad, String code) {
        Checker.Result r = Checker.check(bad);
        assertTrue(r.has(code), "expected " + code + " but got " + r.violations());
    }

    @Test
    void theUntouchedHistoryReportsNothingAndCountsWhatHappened() {
        Checker.Result r = Checker.check(good());
        assertTrue(r.ok(), r.violations().toString());
        assertEquals(4, r.stats().get("queue.claims"));
        assertEquals(10, r.stats().get("queue.deliveries"));
        assertEquals(4, r.stats().get("queue.redeliveries"));
        assertEquals(6, r.stats().get("queue.acks.accepted"));
        assertEquals(1, r.stats().get("queue.rejected"));
        assertEquals(1, r.stats().get("queue.dead"));
    }

    @Test
    void q1DeliveringAnAcknowledgedMessageAgainIsCaught() {
        History bad = goodHistoryBuilder(h -> { }).history();
        H h = goodHistoryBuilder(null);
        h.claim("w3", new Object[]{0, 0L, 2, "p1-0"});                      // p1-0 was acked long ago
        assertCaught(h.history(), "P10_REDELIVERED_AFTER_ACK");
        assertTrue(Checker.check(bad).ok());
    }

    @Test
    void q2HandingOutTheSameAttemptTwiceIsCaught() {
        H h = goodHistoryBuilder(null);
        h.claim("w3", new Object[]{1, 2L, 1, "p1-5"});                      // attempt 1 of p1-5 already went to w2
        assertCaught(h.history(), "P11_ATTEMPT_REUSED");
    }

    @Test
    void q3AcceptingAStaleSettleWhileAnotherWorkerHoldsTheMessageIsCaught() {
        // w1's late ack of attempt 1 was refused in the good history; here the broker accepted it although attempt 2 was already out
        int lateAckIndex = 3;                                               // the 4th settle: ack 0:1 attempt 1 by w1
        History bad = editNth("complete", "settle", lateAckIndex, d -> {
            d.put("settled", 1L);
            d.put("rejected", 0L);
        });
        assertCaught(bad, "P12_FENCED_SETTLE_ACCEPTED");
        // a stale nack or release that was accepted is just as bad
        H h = goodHistoryBuilder(null);
        h.settle("w1", "release", 1, 1, 1, Outcome.OK, 1, 0);               // attempt 1 of p1-3 is long gone
        assertCaught(h.history(), "P12_FENCED_SETTLE_ACCEPTED");
    }

    @Test
    void q3bAcknowledgingSomethingAlreadyFinishedByAnotherAttemptIsAHarmlessNoOp() {
        H h = goodHistoryBuilder(null);
        h.settle("w1", "ack", 0, 1, 1, Outcome.OK, 1, 0);                   // 0:1 was finished by w2's ack of attempt 2: accepting the repeat is by design
        assertTrue(Checker.check(h.history()).ok(), Checker.check(h.history()).violations().toString());
    }

    @Test
    void q4MessagesLeftClaimedOrWaitingAreCaughtAsStuck() {
        History stuck = editNth("complete", "queue-final", 0, d -> d.put("partitions", List.of(List.of(0, 3L, 3L, 3L, 0, 0, 0L, 0), List.of(1, 1L, 3L, 3L, 2, 0, 1L, 1))));
        assertCaught(stuck, "P13_STUCK");
        History waiting = editNth("complete", "queue-final", 0, d -> d.put("partitions", List.of(List.of(0, 3L, 3L, 3L, 0, 0, 0L, 0), List.of(1, 1L, 3L, 3L, 0, 1, 1L, 1))));
        assertCaught(waiting, "P13_STUCK");
    }

    @Test
    void q5MessagesNeverHandedOutOrUnaccountedForAreCaught() {
        History unclaimed = editNth("complete", "queue-final", 0, d -> d.put("partitions", List.of(List.of(0, 2L, 2L, 3L, 0, 0, 0L, 0), List.of(1, 1L, 3L, 3L, 0, 0, 1L, 1))));
        assertCaught(unclaimed, "P13_UNCLAIMED");
        History lost = editNth("complete", "queue-final", 0, d -> d.put("partitions", List.of(List.of(0, 3L, 3L, 3L, 0, 0, 0L, 0), List.of(1, 0L, 3L, 3L, 0, 0, 1L, 1))));   // 3 above the floor, only 2 explained
        assertCaught(lost, "P13_UNACCOUNTED");
    }

    @Test
    void q6ADeadLetterThatSurvivesTheFixAndRetryIsCaught() {
        History bad = editNth("complete", "queue-final", 1, d -> d.put("partitions", List.of(List.of(0, 3L, 3L, 3L, 0, 0, 0L, 0), List.of(1, 1L, 3L, 3L, 0, 0, 1L, 1))));
        assertCaught(bad, "P13_DEAD_AFTER_FIX");
    }

    @Test
    void q7MissingFinalStateIsCaught() {
        History bad = mutate(ev -> {
            ev.removeIf(e -> e.type().equals("queue-final"));
            return ev;
        });
        assertCaught(bad, "P13_NO_FINAL_STATE");
    }

    @Test
    void q8InconsistentDeadLetterListsAreCaught() {
        assertCaught(editNth("complete", "queue-final", 0, d -> d.put("deadTotal", 2L)), "P14_DEAD_COUNT");
        assertCaught(editNth("complete", "queue-final", 0, d -> d.put("dead", List.of(List.of(1, 1L, 1L, "p1-3")))), "P14_DEAD_EARLY");
        assertCaught(editNth("complete", "queue-final", 0, d -> d.put("dead", List.of(List.of(1, 1L, 3L, "something-else")))), "P14_DEAD_CONTENT");
    }

    @Test
    void q9DeliveringSomethingTheLogDoesNotHoldIsCaught() {
        H h = goodHistoryBuilder(null);
        h.claim("w3", new Object[]{0, 2L, 7, "p1-1"});                      // offset 0:2 holds p1-4, not p1-1
        assertCaught(h.history(), "P15_PHANTOM_DELIVERY");
        H g = goodHistoryBuilder(null);
        g.claim("w3", new Object[]{1, 2L, 9, "never-produced"});
        assertCaught(g.history(), "P15_PHANTOM_DELIVERY");
    }

    @Test
    void q10ReplicasThatDisagreeAboutTheAckStateAreCaught() {
        H h = goodHistoryBuilder(null);
        h.replicaAck(4, 999);
        assertCaught(h.history(), "P9_ACK_STATE_DIVERGENCE");
    }

    @Test
    void q11UnknownOutcomesAreNotHeldAgainstTheBroker() {
        H h = goodHistoryBuilder(null);
        h.settle("w3", "ack", 0, 2, 1, Outcome.INFO, 0, 0);                  // timed out: it may or may not have been applied, either way no violation
        assertTrue(Checker.check(h.history()).ok(), Checker.check(h.history()).violations().toString());
    }
}
