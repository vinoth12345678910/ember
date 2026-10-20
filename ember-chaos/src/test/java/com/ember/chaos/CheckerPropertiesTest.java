package com.ember.chaos;

import org.junit.jupiter.api.Test;

import static com.ember.chaos.H.row;
import static org.junit.jupiter.api.Assertions.*;

/** Each property P1..P9 on small handwritten histories: a good one reports nothing, a bad one reports the right code. */
class CheckerPropertiesTest {

    private static Checker.Result check(H h) {
        return Checker.check(h.history());
    }

    private static void assertCodes(Checker.Result r, String... codes) {
        for (String c : codes) assertTrue(r.has(c), "expected " + c + " but got " + r.violations());
    }

    // ---- P1

    @Test
    void p1AcknowledgedWriteMustBeInTheFinalReadAtItsOffset() {
        H good = new H();
        good.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        good.produce("p1", "t", 0, "k", "v1", Outcome.OK, 1);
        good.read("reader", "t", 0, true, 0, 2, row(0, "k", "v0"), row(1, "k", "v1"));
        assertTrue(check(good).ok(), check(good).violations().toString());

        H lost = new H();
        lost.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        lost.produce("p1", "t", 0, "k", "v1", Outcome.OK, 1);
        lost.read("reader", "t", 0, true, 0, 2, row(0, "k", "v0"));
        assertCodes(check(lost), "P1_LOST_ACK");

        H wrongRecord = new H();
        wrongRecord.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        wrongRecord.produce("p2", "t", 0, "k2", "other", Outcome.INFO, -1);
        wrongRecord.read("reader", "t", 0, true, 0, 1, row(0, "k2", "other"));
        assertCodes(check(wrongRecord), "P1_LOST_ACK");

        H noFinal = new H();
        noFinal.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        assertCodes(check(noFinal), "P1_NO_FINAL_READ");
    }

    @Test
    void p1UnknownOutcomesMayExistOrNot() {
        H h = new H();
        h.produce("p1", "t", 0, "k", "maybe-stored", Outcome.INFO, -1);
        h.produce("p1", "t", 0, "k", "maybe-not", Outcome.INFO, -1);
        h.read("reader", "t", 0, true, 0, 1, row(0, "k", "maybe-stored"));
        assertTrue(check(h).ok(), check(h).violations().toString());
    }

    @Test
    void p1RetentionMayRemoveOldAcknowledgedRecords() {
        H h = new H();
        h.produce("p1", "t", 0, "k", "old", Outcome.OK, 0);
        h.produce("p1", "t", 0, "k", "new", Outcome.OK, 5);
        h.read("reader", "t", 0, true, 3, 6, row(5, "k", "new"));
        assertTrue(check(h).ok(), "offset 0 is below the log start 3: retention removed it");
    }

    // ---- P2

    @Test
    void p2TwoReadsMustAgreeAboutAnOffset() {
        H good = new H();
        good.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        good.read("r1", "t", 0, false, 0, 1, row(0, "k", "v0"));
        good.read("r2", "t", 0, true, 0, 1, row(0, "k", "v0"));
        assertTrue(check(good).ok());

        H bad = new H();
        bad.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        bad.produce("p1", "t", 0, "k", "v1", Outcome.INFO, -1);
        bad.read("r1", "t", 0, false, 0, 1, row(0, "k", "v0"));
        bad.read("r2", "t", 0, true, 0, 1, row(0, "k", "v1"));
        assertCodes(check(bad), "P2_DIVERGENCE");
    }

    @Test
    void p2AGroupDeliveryMustMatchWhatReadsSaw() {
        H bad = new H();
        bad.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        bad.produce("p1", "t", 0, "k", "v1", Outcome.OK, 1);
        bad.read("r", "t", 0, true, 0, 2, row(0, "k", "v0"), row(1, "k", "v1"));
        bad.poll("c1", 1, new Object[]{"t", 0, 0L, "v1"}, new Object[]{"t", 0, 1L, "v1"});
        assertCodes(check(bad), "P2_DIVERGENCE");
    }

    // ---- P3

    @Test
    void p3EveryValueReadWasSentAndAFailedWriteIsNeverVisible() {
        H good = new H();
        good.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        good.read("r", "t", 0, true, 0, 1, row(0, "k", "v0"));
        assertTrue(check(good).ok());

        H phantom = new H();
        phantom.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        phantom.read("r", "t", 0, true, 0, 2, row(0, "k", "v0"), row(1, "k", "invented"));
        assertCodes(check(phantom), "P3_PHANTOM");

        H failed = new H();
        failed.produce("p1", "t", 0, "k", "refused", Outcome.FAIL, -1);
        failed.read("r", "t", 0, true, 0, 1, row(0, "k", "refused"));
        assertCodes(check(failed), "P3_FAILED_WRITE_VISIBLE");
    }

    // ---- P4

    @Test
    void p4OffsetsAreUniqueAndIncreasePerProducerAndPartition() {
        H good = new H();
        good.produce("p1", "t", 0, "a", "v0", Outcome.OK, 0);
        good.produce("p2", "t", 0, "b", "v1", Outcome.OK, 1);
        good.produce("p1", "t", 0, "a", "v2", Outcome.OK, 2);
        good.read("r", "t", 0, true, 0, 3, row(0, "a", "v0"), row(1, "b", "v1"), row(2, "a", "v2"));
        assertTrue(check(good).ok());

        H dup = new H();
        dup.produce("p1", "t", 0, "a", "v0", Outcome.OK, 0);
        dup.produce("p2", "t", 0, "b", "v1", Outcome.OK, 0);
        dup.read("r", "t", 0, true, 0, 1, row(0, "a", "v0"));
        assertCodes(check(dup), "P4_DUPLICATE_OFFSET");

        H backwards = new H();
        backwards.produce("p1", "t", 0, "a", "v0", Outcome.OK, 5);
        backwards.produce("p1", "t", 0, "a", "v1", Outcome.OK, 3);
        backwards.read("r", "t", 0, true, 0, 6, row(3, "a", "v1"), row(5, "a", "v0"));
        assertCodes(check(backwards), "P4_NOT_INCREASING");

        H retried = new H();
        retried.produce("p1", "t", 0, "a", "v0", Outcome.OK, 5);
        retried.produceRetried("p1", "t", 0, "a", "v1", 3);                 // a retried send is exempt from the monotonic rule
        retried.read("r", "t", 0, true, 0, 6, row(3, "a", "v1"), row(5, "a", "v0"));
        assertFalse(check(retried).has("P4_NOT_INCREASING"));
    }

    // ---- P5

    @Test
    void p5AKeysAcknowledgedValuesAppearInSendOrder() {
        H good = new H();
        good.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        good.produce("p1", "t", 0, "k", "v1", Outcome.OK, 1);
        good.read("r", "t", 0, true, 0, 2, row(0, "k", "v0"), row(1, "k", "v1"));
        assertTrue(check(good).ok());

        H swapped = new H();
        swapped.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        swapped.produce("p1", "t", 0, "k", "v1", Outcome.OK, 1);
        swapped.read("r", "t", 0, true, 0, 2, row(0, "k", "v1"), row(1, "k", "v0"));
        assertCodes(check(swapped), "P5_KEY_ORDER");

        H moved = new H();
        moved.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        moved.produce("p1", "t", 1, "k", "v1", Outcome.OK, 0);
        moved.read("r", "t", 0, true, 0, 1, row(0, "k", "v0"));
        moved.read("r", "t", 1, true, 0, 1, row(0, "k", "v1"));
        assertCodes(check(moved), "P5_KEY_ORDER");
    }

    // ---- P6

    @Test
    void p6ALaterReadNeverShowsAShorterLogThanAnEarlierOneByTheSameReader() {
        H good = new H();
        good.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        good.read("r", "t", 0, false, 0, 1, row(0, "k", "v0"));
        good.read("r", "t", 0, true, 0, 1, row(0, "k", "v0"));
        assertTrue(check(good).ok());

        H shrink = new H();
        shrink.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        shrink.produce("p1", "t", 0, "k", "v1", Outcome.OK, 1);
        shrink.read("r", "t", 0, false, 0, 2, row(0, "k", "v0"), row(1, "k", "v1"));
        shrink.read("r", "t", 0, false, 0, 1, row(0, "k", "v0"));
        shrink.read("other", "t", 0, true, 0, 2, row(0, "k", "v0"), row(1, "k", "v1"));
        assertCodes(check(shrink), "P6_SHRINK");

        H retention = new H();
        retention.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        retention.produce("p1", "t", 0, "k", "v1", Outcome.OK, 1);
        retention.read("r", "t", 0, false, 0, 2, row(0, "k", "v0"), row(1, "k", "v1"));
        retention.read("r", "t", 0, true, 2, 4);                               // retention advanced the start past everything seen
        assertFalse(check(retention).has("P6_SHRINK"));
    }

    // ---- P7

    @Test
    void p7EveryAcknowledgedRecordReachesTheGroupAndNothingInventedIsDelivered() {
        H good = new H();
        good.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        good.produce("p1", "t", 0, "k", "v1", Outcome.OK, 1);
        good.read("r", "t", 0, true, 0, 2, row(0, "k", "v0"), row(1, "k", "v1"));
        good.poll("c1", 1, new Object[]{"t", 0, 0L, "v0"}, new Object[]{"t", 0, 1L, "v1"});
        good.poll("c2", 2, new Object[]{"t", 0, 1L, "v1"});                      // a duplicate is allowed
        Checker.Result r = check(good);
        assertTrue(r.ok(), r.violations().toString());
        assertEquals(3, r.stats().get("deliveries"));
        assertEquals(2, r.stats().get("deliveries.distinct"));
        assertEquals(1, r.stats().get("deliveries.duplicates"));

        H missing = new H();
        missing.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        missing.produce("p1", "t", 0, "k", "v1", Outcome.OK, 1);
        missing.read("r", "t", 0, true, 0, 2, row(0, "k", "v0"), row(1, "k", "v1"));
        missing.poll("c1", 1, new Object[]{"t", 0, 0L, "v0"});
        assertCodes(check(missing), "P7_NOT_DELIVERED");

        H phantom = new H();
        phantom.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        phantom.read("r", "t", 0, true, 0, 1, row(0, "k", "v0"));
        phantom.poll("c1", 1, new Object[]{"t", 0, 0L, "v0"}, new Object[]{"t", 0, 1L, "ghost"});
        assertCodes(check(phantom), "P7_PHANTOM_DELIVERY");
    }

    // ---- P8

    @Test
    void p8CommitsStayInsideTheLogAndNoZombieCommitIsAccepted() {
        H good = new H();
        good.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        good.read("r", "t", 0, true, 0, 1, row(0, "k", "v0"));
        good.poll("c1", 1, new Object[]{"t", 0, 0L, "v0"});
        good.commit("m1", 1, Outcome.OK, new Object[]{"t", 0, 1L});
        assertTrue(check(good).ok(), check(good).violations().toString());

        H beyond = new H();
        beyond.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        beyond.read("r", "t", 0, true, 0, 1, row(0, "k", "v0"));
        beyond.poll("c1", 1, new Object[]{"t", 0, 0L, "v0"});
        beyond.commit("m1", 1, Outcome.OK, new Object[]{"t", 0, 7L});
        assertCodes(check(beyond), "P8_COMMIT_BEYOND_END");

        H zombie = new H();
        zombie.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        zombie.read("r", "t", 0, true, 0, 1, row(0, "k", "v0"));
        zombie.poll("c1", 1, new Object[]{"t", 0, 0L, "v0"});
        zombie.poll("c2", 2);                                                  // somebody already lives in generation 2
        zombie.commit("m1", 1, Outcome.OK, new Object[]{"t", 0, 1L});          // ... yet a generation-1 commit was acknowledged
        assertCodes(check(zombie), "P8_ZOMBIE_COMMIT");

        H refused = new H();
        refused.produce("p1", "t", 0, "k", "v0", Outcome.OK, 0);
        refused.read("r", "t", 0, true, 0, 1, row(0, "k", "v0"));
        refused.poll("c1", 1, new Object[]{"t", 0, 0L, "v0"});
        refused.poll("c2", 2);
        refused.commit("m1", 1, Outcome.FAIL, new Object[]{"t", 0, 1L});      // correctly refused: no violation
        assertTrue(check(refused).ok());
    }

    // ---- P9

    @Test
    void p9ReplicasMustAgree() {
        H good = new H();
        for (int r = 1; r <= 3; r++) {
            good.replicaData("t-0", r, 0, 10, 12345);
            good.replicaRaft("t-0", r, 50, 999, "1:1,30:2");
            good.replicaState("groupstate", r, 77);
            good.replicaState("metastate", r, 88);
        }
        assertTrue(check(good).ok(), check(good).violations().toString());
        assertEquals(4, check(good).stats().get("replica.checks"));

        H data = new H();
        data.replicaData("t-0", 1, 0, 10, 12345);
        data.replicaData("t-0", 2, 0, 10, 12345);
        data.replicaData("t-0", 3, 0, 10, 12346);                               // one byte differs
        assertCodes(check(data), "P9_DATA_DIVERGENCE");

        H raft = new H();
        raft.replicaRaft("t-0", 1, 50, 999, "1:1");
        raft.replicaRaft("t-0", 2, 50, 998, "1:1");
        assertCodes(check(raft), "P9_RAFT_DIVERGENCE");

        H terms = new H();
        terms.replicaRaft("t-0", 1, 50, 999, "1:1");
        terms.replicaRaft("t-0", 2, 50, 999, "1:2");
        assertCodes(check(terms), "P9_RAFT_DIVERGENCE");

        H group = new H();
        group.replicaState("groupstate", 1, 1);
        group.replicaState("groupstate", 2, 2);
        assertCodes(check(group), "P9_GROUP_STATE_DIVERGENCE");

        H meta = new H();
        meta.replicaState("metastate", 1, 1);
        meta.replicaState("metastate", 2, 2);
        assertCodes(check(meta), "P9_META_STATE_DIVERGENCE");
    }

    @Test
    void anEmptyHistoryHasNothingToComplainAbout() {
        assertTrue(Checker.check(new History(java.util.List.of())).ok());
    }

    @Test
    void nemesisEventsAreCountedInTheStatistics() {
        H h = new H();
        h.rec.nemesis("crash", "broker-2", "start");
        h.rec.nemesis("crash", "broker-2", "stop");
        h.rec.nemesis("pause", "broker-1", "start");
        Checker.Result r = check(h);
        assertEquals(2, r.stats().get("nemesis.total"));
        assertEquals(1, r.stats().get("nemesis.crash"));
    }
}
