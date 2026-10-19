package com.ember.raft.sim;

import com.ember.raft.Role;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Step 5: scripted election scenarios (the invariant checker runs after every event of every one of them). */
class ElectionScenariosTest {

    @Test
    void aThreeNodeElectionProducesOneLeaderInTermOneAndStaysCalm() {
        SimConfig config = SimConfig.of(1, 3)
                .withElectionTimeout(1, 200, 200).withElectionTimeout(2, 5_000, 5_000).withElectionTimeout(3, 5_000, 5_000);
        SimCluster c = new SimCluster(config);
        assertTrue(c.runUntil(() -> c.currentLeader() > 0, 2_000));
        assertEquals(1, c.currentLeader());
        assertEquals(1, c.term(1));
        c.runFor(10_000);                                          // calm: no spurious election
        assertEquals(List.of(1), c.aliveLeaders());
        for (int id = 1; id <= 3; id++) {
            assertEquals(1, c.term(id), "n" + id + " must still be in term 1");
            assertEquals(1, c.node(id).leaderId());
        }
        assertEquals(List.of("CANDIDATE@1", "LEADER@1"), c.roleHistoryOf(1));
        assertEquals(java.util.Map.of(1L, 1), c.leaderHistory());
    }

    @Test
    void twoHundredElectionOnlySeedsAlwaysEndWithExactlyOneLeaderAndNoTermChurn() {
        for (long seed = 0; seed < 200; seed++) {
            int nodes = seed % 2 == 0 ? 3 : 5;
            SimCluster c = new SimCluster(SimConfig.of(seed, nodes));
            c.runFor(3_000);
            assertEquals(1, c.aliveLeaders().size(), "seed " + seed);
            long term = c.term(c.currentLeader());
            c.runFor(10_000);
            assertEquals(1, c.aliveLeaders().size(), "seed " + seed);
            for (int id = 1; id <= nodes; id++) {
                assertEquals(term, c.term(id), "seed " + seed + ": term churned on n" + id);
            }
        }
    }

    @Test
    void whenTheLeaderCrashesTheOthersElectANewOneInAHigherTermAndTheOldLeaderComesBackAsAFollower() {
        SimCluster c = new SimCluster(SimConfig.of(5, 5));
        assertTrue(c.runUntil(() -> c.currentLeader() > 0, 3_000));
        int oldLeader = c.currentLeader();
        long oldTerm = c.term(oldLeader);
        c.crash(oldLeader);
        assertTrue(c.runUntil(() -> c.currentLeader() > 0 && c.currentLeader() != oldLeader, 5_000));
        int newLeader = c.currentLeader();
        assertTrue(c.term(newLeader) > oldTerm);
        c.restart(oldLeader);
        c.runFor(3_000);
        assertEquals(Role.FOLLOWER, c.role(oldLeader));
        assertEquals(c.term(newLeader), c.term(oldLeader));
        assertEquals(List.of(newLeader), c.aliveLeaders());
    }

    @Test
    void aForcedSplitVoteResolvesInALaterTerm() {
        // 4 nodes (majority 3). Nodes 1 and 2 time out at exactly the same instant; voters 3 and 4 are wired so that
        // 3 prefers candidate 1 and 4 prefers candidate 2 -> 2 votes each, nobody wins term 1.
        SimConfig config = SimConfig.of(1, 4).withLatency(10, 10)
                .withElectionTimeout(1, 150, 150).withElectionTimeout(2, 150, 150)
                .withElectionTimeout(3, 100_000, 100_000).withElectionTimeout(4, 100_000, 100_000);
        SimCluster c = new SimCluster(config);
        c.setLinkLatency(1, 3, 10);
        c.setLinkLatency(2, 3, 30);
        c.setLinkLatency(2, 4, 10);
        c.setLinkLatency(1, 4, 30);
        c.runFor(250);                                             // the first round plays out completely
        assertTrue(c.aliveLeaders().isEmpty(), "split: nobody has a majority in term 1");
        assertEquals(1, c.term(1));
        assertEquals(1, c.term(2));
        assertEquals(1, c.term(3));
        assertEquals(1, c.term(4));
        assertEquals(1, c.node(3).votedFor());
        assertEquals(2, c.node(4).votedFor());

        c.clearLinkLatencies();                                    // random latencies break the symmetry
        c.setLinkLatency(2, 3, 25);
        c.setLinkLatency(2, 4, 25);                                // keep candidate 1 ahead on both voters
        assertTrue(c.runUntil(() -> c.currentLeader() > 0, 5_000));
        assertTrue(c.term(c.currentLeader()) >= 2, "resolved in a LATER term");
        assertEquals(1, c.aliveLeaders().size());
    }

    @Test
    void aPartitionedLeaderKeepsItsRoleWhileTheMajorityElectsASuccessorAndHealingDeposesIt() {
        SimCluster c = new SimCluster(SimConfig.of(8, 5));
        assertTrue(c.runUntil(() -> c.currentLeader() > 0, 3_000));
        int oldLeader = c.currentLeader();
        long oldTerm = c.term(oldLeader);
        int[] rest = java.util.stream.IntStream.rangeClosed(1, 5).filter(i -> i != oldLeader).toArray();
        c.partition(new int[]{oldLeader}, rest);
        assertTrue(c.runUntil(() -> {
            int l = c.currentLeader();
            return l > 0 && l != oldLeader;
        }, 5_000));
        assertEquals(Role.LEADER, c.role(oldLeader), "no CheckQuorum: the stranded leader still believes it leads");
        assertEquals(oldTerm, c.term(oldLeader));
        assertEquals(2, c.aliveLeaders().size());
        long newTerm = c.term(c.currentLeader());
        assertTrue(newTerm > oldTerm);

        c.heal();
        c.runFor(2_000);
        assertEquals(1, c.aliveLeaders().size());
        assertEquals(Role.FOLLOWER, c.role(oldLeader));
        assertEquals(newTerm, c.term(oldLeader));
    }

    @Test
    void anIsolatedFollowerKeepsIncreasingItsTermAndForcesOneMoreElectionAfterHealing() {
        SimCluster c = new SimCluster(SimConfig.of(12, 3));
        assertTrue(c.runUntil(() -> c.currentLeader() > 0, 3_000));
        int leader = c.currentLeader();
        int loner = leader == 1 ? 2 : 1;
        long term = c.term(leader);
        c.isolate(loner);
        c.runFor(5_000);
        assertTrue(c.term(loner) > term + 3, "the isolated node keeps timing out");
        c.heal();
        c.runFor(5_000);
        assertEquals(1, c.aliveLeaders().size());
        assertTrue(c.term(c.currentLeader()) > term, "plain Raft: its big term deposes the old leader (PreVote would prevent this)");
    }

    @Test
    void aSingleNodeAndATwoNodeClusterBehave() {
        SimCluster one = new SimCluster(SimConfig.of(3, 1));
        one.runFor(5_000);
        assertEquals(List.of(1), one.aliveLeaders());

        SimCluster two = new SimCluster(SimConfig.of(3, 2));
        two.runFor(5_000);
        assertEquals(1, two.aliveLeaders().size());
        two.crash(two.aliveLeaders().get(0));
        two.runFor(5_000);
        assertTrue(two.aliveLeaders().isEmpty(), "2 nodes: one dead node means no majority");
    }

    @Test
    void withCheckQuorumAnIsolatedLeaderStepsDownByItselfWithoutAnyOneElseTellingIt() {
        SimCluster c = new SimCluster(SimConfig.of(8, 5).withCheckQuorum(true));
        assertTrue(c.runUntil(() -> c.currentLeader() > 0, 3_000));
        int stranded = c.currentLeader();
        long term = c.term(stranded);
        c.isolate(stranded);
        assertTrue(c.runUntil(() -> c.role(stranded) != Role.LEADER, 2_000), "it must resign within about electionTimeoutMax + heartbeat");
        assertEquals(term, c.term(stranded), "no new term: it just stopped leading");
        assertTrue(c.runUntil(() -> {
            int l = c.currentLeader();
            return l > 0 && l != stranded;
        }, 5_000));
        c.heal();
        c.runFor(3_000);
        assertEquals(1, c.aliveLeaders().size());
    }

    @Test
    void withoutCheckQuorumTheIsolatedLeaderKeepsBelievingForever() {
        SimCluster c = new SimCluster(SimConfig.of(8, 5).withCheckQuorum(false));
        assertTrue(c.runUntil(() -> c.currentLeader() > 0, 3_000));
        int stranded = c.currentLeader();
        c.isolate(stranded);
        c.runFor(10_000);
        assertEquals(Role.LEADER, c.role(stranded));
    }
}
