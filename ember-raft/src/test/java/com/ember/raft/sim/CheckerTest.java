package com.ember.raft.sim;

import com.ember.raft.LogEntry;
import com.ember.raft.Role;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Every checker must be able to FAIL: here each one is fed a hand-made violation. */
class CheckerTest {

    private static LogEntry e(long term, String s) {
        return LogEntry.command(term, s.getBytes());
    }

    private static SimCluster fresh() {
        return new SimCluster(SimConfig.of(1, 3));
    }

    @Test
    void stateMachineSafetyCatchesTwoDifferentEntriesAtTheSameIndex() {
        SimCluster c = fresh();
        c.checker().onCommitted(1, 1, e(1, "a"));
        InvariantViolation v = assertThrows(InvariantViolation.class, () -> c.checker().onCommitted(2, 1, e(1, "DIFFERENT")));
        assertEquals("StateMachineSafety", v.invariant());
    }

    @Test
    void stateMachineSafetyAcceptsTheSameEntryOnEveryNode() {
        SimCluster c = fresh();
        for (int node = 1; node <= 3; node++) {
            c.checker().onCommitted(node, 1, e(1, "a"));
            c.checker().onCommitted(node, 2, e(1, "b"));
        }
        assertEquals(List.of(e(1, "a"), e(1, "b")), c.checker().committedEntries());
    }

    @Test
    void applyOrderCatchesGapsAndRepeats() {
        SimCluster c = fresh();
        assertEquals("ApplyOrder", assertThrows(InvariantViolation.class, () -> c.checker().onCommitted(1, 2, e(1, "a"))).invariant());
        SimCluster d = fresh();
        d.checker().onCommitted(1, 1, e(1, "a"));
        assertEquals("ApplyOrder", assertThrows(InvariantViolation.class, () -> d.checker().onCommitted(1, 1, e(1, "a"))).invariant());
    }

    @Test
    void aRestartedNodeMayApplyFromIndexOneAgain() {
        SimCluster c = fresh();
        c.checker().onCommitted(1, 1, e(1, "a"));
        c.checker().onCommitted(1, 2, e(1, "b"));
        c.checker().onRestart(1);
        assertDoesNotThrow(() -> c.checker().onCommitted(1, 1, e(1, "a")));
        assertEquals("StateMachineSafety", assertThrows(InvariantViolation.class, () -> {
            c.checker().onRestart(2);
            c.checker().onCommitted(2, 1, e(1, "other"));
        }).invariant());
    }

    @Test
    void leaderCompletenessCatchesALeaderThatMissesACommittedEntry() {
        SimCluster c = fresh();
        c.checker().onCommitted(1, 1, e(1, "committed"));
        // node 2's log is empty: it must not be allowed to become leader
        InvariantViolation v = assertThrows(InvariantViolation.class, () -> c.checker().onRoleChange(2, Role.LEADER, 3, 2));
        assertEquals("LeaderCompleteness", v.invariant());
    }

    @Test
    void leaderCompletenessCatchesALeaderWithADifferentEntryAtACommittedIndex() {
        SimCluster c = fresh();
        c.checker().onCommitted(1, 1, e(1, "committed"));
        c.storage(3).append(List.of(e(2, "something else")));
        assertEquals("LeaderCompleteness", assertThrows(InvariantViolation.class, () -> c.checker().onRoleChange(3, Role.LEADER, 3, 3)).invariant());
    }

    @Test
    void leaderCompletenessAcceptsALeaderThatHasEverythingCommitted() {
        SimCluster c = fresh();
        c.checker().onCommitted(1, 1, e(1, "committed"));
        c.storage(2).append(List.of(e(1, "committed"), e(2, "uncommitted")));
        assertDoesNotThrow(() -> c.checker().onRoleChange(2, Role.LEADER, 3, 2));
    }

    @Test
    void logMatchingCatchesTwoLogsWithTheSameTermAtAnIndexButDifferentContent() {
        SimCluster c = fresh();
        c.storage(1).append(List.of(e(1, "a")));
        c.checker().afterEvent(1);
        c.storage(2).append(List.of(e(1, "DIFFERENT")));
        assertEquals("LogMatching", assertThrows(InvariantViolation.class, () -> c.checker().afterEvent(2)).invariant());
    }

    @Test
    void logMatchingCatchesLogsThatDifferEarlierEvenIfTheLastEntriesAgree() {
        SimCluster c = fresh();
        c.storage(1).append(List.of(e(1, "x"), e(2, "same")));
        c.checker().afterEvent(1);
        c.storage(2).append(List.of(e(1, "y"), e(2, "same")));
        assertEquals("LogMatching", assertThrows(InvariantViolation.class, () -> c.checker().afterEvent(2)).invariant());
    }

    @Test
    void logMatchingAllowsDifferentTermsAtTheSameIndex() {
        SimCluster c = fresh();
        c.storage(1).append(List.of(e(1, "a"), e(2, "old leader")));
        c.checker().afterEvent(1);
        c.storage(2).append(List.of(e(1, "a"), e(3, "new leader")));
        assertDoesNotThrow(() -> c.checker().afterEvent(2));
    }

    @Test
    void logMatchingNoticesWhenALogIsRewrittenAndCompareTheNewEntries() {
        SimCluster c = fresh();
        c.storage(1).append(List.of(e(1, "a"), e(2, "b")));
        c.checker().afterEvent(1);
        c.storage(2).append(List.of(e(1, "a"), e(2, "b")));
        c.checker().afterEvent(2);                                  // identical logs: fine
        c.storage(2).truncateFrom(2);
        c.storage(2).append(List.of(e(2, "TAMPERED")));
        assertEquals("LogMatching", assertThrows(InvariantViolation.class, () -> c.checker().afterEvent(2)).invariant());
    }

    @Test
    void leaderAppendOnlyCatchesALeaderThatDeletesItsOwnEntries() {
        SimCluster c = fresh();
        c.checker().onRoleChange(1, Role.LEADER, 5, 1);
        c.storage(1).append(List.of(e(5, "noop"), e(5, "a"), e(5, "b")));
        c.checker().afterEvent(1);                                   // the event in which it became leader: appends are fine
        c.storage(1).append(List.of(e(5, "c")));
        assertDoesNotThrow(() -> c.checker().afterEvent(1));         // pure appends are fine
        c.storage(1).truncateFrom(3);
        InvariantViolation v = assertThrows(InvariantViolation.class, () -> c.checker().afterEvent(1));
        assertEquals("LeaderAppendOnly", v.invariant());
    }

    @Test
    void leaderAppendOnlyCatchesAnOverwriteEvenWhenTheLengthStaysTheSame() {
        SimCluster c = fresh();
        c.checker().onRoleChange(1, Role.LEADER, 5, 1);
        c.storage(1).append(List.of(e(5, "a"), e(5, "b")));
        c.checker().afterEvent(1);
        c.storage(1).truncateFrom(2);
        c.storage(1).append(List.of(e(5, "REPLACED")));
        assertEquals("LeaderAppendOnly", assertThrows(InvariantViolation.class, () -> c.checker().afterEvent(1)).invariant());
    }

    @Test
    void aFollowerMayTruncateItsLogFreely() {
        SimCluster c = fresh();
        c.storage(1).append(List.of(e(1, "a"), e(2, "b")));
        c.checker().afterEvent(1);
        c.storage(1).truncateFrom(2);
        assertDoesNotThrow(() -> c.checker().afterEvent(1));
    }

    @Test
    void aLeaderThatCrashedAndCameBackIsAFollowerAgainAndMayBeTruncated() {
        SimCluster c = fresh();
        c.checker().onRoleChange(1, Role.LEADER, 5, 1);
        c.storage(1).append(List.of(e(5, "a"), e(5, "b")));
        c.checker().afterEvent(1);
        c.checker().onCrash(1);
        c.checker().onRestart(1);
        c.storage(1).truncateFrom(2);
        assertDoesNotThrow(() -> c.checker().afterEvent(1));
    }
}
