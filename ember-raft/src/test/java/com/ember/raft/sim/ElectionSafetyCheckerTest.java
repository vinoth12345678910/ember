package com.ember.raft.sim;

import com.ember.raft.Role;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** The checkers must be able to FAIL: here they are fed contradictory facts by hand. */
class ElectionSafetyCheckerTest {

    @Test
    void twoDifferentLeadersInOneTermAreAViolation() {
        SimCluster c = new SimCluster(SimConfig.of(1, 3));
        c.checker().onRoleChange(1, Role.LEADER, 50, 1);
        InvariantViolation v = assertThrows(InvariantViolation.class, () -> c.checker().onRoleChange(2, Role.LEADER, 50, 2));
        assertEquals("ElectionSafety", v.invariant());
        assertEquals(1, v.seed());
        assertTrue(v.getMessage().contains("n1") && v.getMessage().contains("n2") && v.getMessage().contains("term 50"));
    }

    @Test
    void theSameNodeAnnouncingItselfTwiceIsNotAViolationAndDifferentTermsAreFine() {
        SimCluster c = new SimCluster(SimConfig.of(1, 3));
        c.checker().onRoleChange(1, Role.LEADER, 50, 1);
        assertDoesNotThrow(() -> c.checker().onRoleChange(1, Role.LEADER, 50, 1));
        assertDoesNotThrow(() -> c.checker().onRoleChange(2, Role.LEADER, 51, 2));
        assertDoesNotThrow(() -> c.checker().onRoleChange(3, Role.CANDIDATE, 50, -1));
    }

    @Test
    void aLeaderThatCrashedStillOwnsItsTerm() {
        SimCluster c = new SimCluster(SimConfig.of(1, 3));
        c.checker().onRoleChange(1, Role.LEADER, 7, 1);
        c.checker().onCrash(1);
        assertThrows(InvariantViolation.class, () -> c.checker().onRoleChange(3, Role.LEADER, 7, 3));
    }

    @Test
    void aTermThatGoesBackwardsIsAViolation() {
        SimCluster c = new SimCluster(SimConfig.of(2, 3));
        c.storage(1).setTermAndVote(9, -1);
        c.checker().afterEvent(1);
        c.storage(1).setTermAndVote(8, -1);
        InvariantViolation v = assertThrows(InvariantViolation.class, () -> c.checker().afterEvent(1));
        assertEquals("TermMonotonic", v.invariant());
    }

    @Test
    void aLostTermBumpAfterARestartIsCaught() {
        SimCluster c = new SimCluster(SimConfig.of(3, 3));
        c.storage(2).setTermAndVote(5, -1);            // written but never synced
        c.checker().afterEvent(2);                     // the checker saw the node in term 5 ...
        c.storage(2).crash();                          // ... then power loss forgot it
        assertThrows(InvariantViolation.class, () -> c.checker().afterEvent(2));
    }

    @Test
    void theViolationReportContainsTheTraceTail() {
        SimCluster c = new SimCluster(SimConfig.of(4, 3));
        c.runFor(1_000);
        c.checker().onRoleChange(1, Role.LEADER, 99, 1);
        InvariantViolation v = assertThrows(InvariantViolation.class, () -> c.checker().onRoleChange(2, Role.LEADER, 99, 2));
        assertFalse(v.traceTail().isEmpty());
        assertTrue(v.traceTail().size() <= 60);
        assertTrue(v.getMessage().contains("last " + v.traceTail().size() + " trace lines"));
    }
}
