package com.ember.raft.sim;

import com.ember.raft.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Step 4: the simulator itself (clock, network model, crash, determinism). */
class SimClusterTest {

    private static String fingerprint(SimCluster c) {
        StringBuilder sb = new StringBuilder();
        sb.append(c.now()).append('|').append(Long.toHexString(c.trace().hash())).append('|').append(c.trace().lineCount());
        for (int id = 1; id <= c.nodeCount(); id++) {
            sb.append('|').append(c.role(id)).append(',').append(c.term(id)).append(',').append(c.lastLogIndex(id))
                    .append(',').append(c.commitIndex(id));
        }
        return sb.toString();
    }

    private static SimCluster run(long seed, int nodes, long ms) {
        SimCluster c = new SimCluster(SimConfig.of(seed, nodes).withDrop(0.05).withDuplicate(0.02));
        c.runFor(ms);
        return c;
    }

    @Test
    void theSameSeedGivesTheSameTraceHashAndTheSameFinalState() {
        for (long seed : new long[]{1, 2, 42, 12345}) {
            SimCluster a = run(seed, 5, 8_000);
            SimCluster b = run(seed, 5, 8_000);
            assertEquals(fingerprint(a), fingerprint(b), "seed " + seed);
            assertTrue(a.trace().lineCount() > 50);
        }
    }

    @Test
    void differentSeedsGiveDifferentTraces() {
        java.util.Set<Long> hashes = new java.util.HashSet<>();
        for (long seed = 1; seed <= 20; seed++) {
            hashes.add(run(seed, 3, 3_000).trace().hash());
        }
        assertEquals(20, hashes.size());
    }

    @Test
    void fullRecordingAndRingRecordingProduceTheSameHash() {
        SimCluster ring = new SimCluster(SimConfig.of(7, 3));
        SimCluster full = new SimCluster(SimConfig.of(7, 3), java.util.Map.of(), true);
        ring.runFor(5_000);
        full.runFor(5_000);
        assertEquals(ring.trace().hash(), full.trace().hash());
        assertEquals(full.trace().lineCount(), full.trace().full().size());
        assertTrue(ring.trace().tail(60).size() <= 60);
    }

    @Test
    void theClockOnlyMovesForwardAndRunForLandsExactlyOnItsTarget() {
        SimCluster c = new SimCluster(SimConfig.of(3, 3));
        long last = 0;
        for (int i = 0; i < 300; i++) {
            c.step();
            assertTrue(c.now() >= last);
            last = c.now();
        }
        long before = c.now();
        c.runFor(1234);
        assertEquals(before + 1234, c.now());
        c.runFor(0);
        assertEquals(before + 1234, c.now());
    }

    @Test
    void runUntilStopsAsSoonAsThePredicateHolds() {
        SimCluster c = new SimCluster(SimConfig.of(5, 3));
        assertTrue(c.runUntil(() -> c.currentLeader() > 0, 5_000));
        assertTrue(c.now() < 1_000);
        assertFalse(c.runUntil(() -> false, 100));
    }

    @Test
    void aPartitionBlocksDeliveryAndHealingRestoresIt() {
        SimCluster c = new SimCluster(SimConfig.of(9, 3));
        c.partition(new int[]{1}, new int[]{2, 3});
        c.runFor(3_000);
        assertTrue(c.stats().lostToPartition > 0);
        assertEquals(List.of(), c.aliveLeaders().stream().filter(id -> id == 1).toList(), "the isolated node cannot win");
        long lostBefore = c.stats().lostToPartition;
        c.heal();
        c.runFor(3_000);
        assertEquals(lostBefore, c.stats().lostToPartition, "nothing is lost once healed");
        assertEquals(1, c.aliveLeaders().size());
    }

    @Test
    void messagesToADeadNodeAreLostAndARestartedNodeComesBackAsAFollower() {
        SimCluster c = new SimCluster(SimConfig.of(11, 3));
        c.runFor(1_000);
        int victim = c.currentLeader() == 3 ? 2 : 3;
        c.crash(victim);
        assertFalse(c.isAlive(victim));
        long lost = c.stats().lostToDeadNode;
        c.runFor(1_000);
        assertTrue(c.stats().lostToDeadNode > lost);
        c.restart(victim);
        assertTrue(c.isAlive(victim));
        assertEquals(Role.FOLLOWER, c.role(victim));
        c.runFor(2_000);
        assertEquals(1, c.aliveLeaders().size());
    }

    @Test
    void dropAndDuplicateProbabilitiesShowUpInTheStatistics() {
        SimCluster c = new SimCluster(SimConfig.of(13, 5).withDrop(0.3).withDuplicate(0.2));
        c.runFor(10_000);
        SimCluster.Stats s = c.stats();
        double dropRate = s.dropped / (double) s.sent;
        double dupRate = s.duplicated / (double) (s.sent - s.dropped);
        assertTrue(dropRate > 0.2 && dropRate < 0.4, "drop rate " + dropRate);
        assertTrue(dupRate > 0.1 && dupRate < 0.3, "duplicate rate " + dupRate);
        SimCluster calm = new SimCluster(SimConfig.of(13, 5));
        calm.runFor(10_000);
        assertEquals(0, calm.stats().dropped);
        assertEquals(0, calm.stats().duplicated);
    }

    @Test
    void latencyIsInsideTheConfiguredRange() {
        SimCluster c = new SimCluster(SimConfig.of(17, 3).withLatency(20, 20));
        // with a fixed 20 ms latency every RequestVote sent at time T arrives at T+20
        c.runFor(1_000);
        List<String> lines = c.trace().tail(2000);
        assertTrue(lines.stream().anyMatch(l -> l.contains("RequestVote(")));
        assertThrows(IllegalArgumentException.class, () -> SimConfig.of(1, 3).withLatency(10, 5));
    }

    @Test
    void aCrashLosesOnlyUnsyncedWrites() {
        SimCluster c = new SimCluster(SimConfig.of(21, 3));
        c.runFor(1_000);
        int leader = c.currentLeader();
        long termBefore = c.term(leader);
        long logBefore = c.lastLogIndex(leader);
        assertFalse(c.storage(leader).hasUnsyncedWrites(), "between events everything is synced");
        c.crash(leader);
        assertEquals(termBefore, c.term(leader));
        assertEquals(logBefore, c.lastLogIndex(leader));
    }

    @Test
    void aNodeCanLosePowerRightAfterSendingAMessage() {
        SimCluster c = new SimCluster(SimConfig.of(23, 3));
        c.crashOnNextSend(1);
        c.crashOnNextSend(2);
        c.crashOnNextSend(3);
        c.runFor(1_000);
        int dead = 0;
        for (int id = 1; id <= 3; id++) {
            if (!c.isAlive(id)) dead++;
        }
        assertTrue(dead >= 1);
        assertTrue(c.trace().tail(500).stream().anyMatch(l -> l.contains("power loss while sending")));
    }

    @Test
    void theClusterNeedsNoRealClockOrThreads() {
        long cpuBefore = System.nanoTime();
        SimCluster c = new SimCluster(SimConfig.of(1, 5));
        c.runFor(60_000);                                       // a virtual minute
        assertTrue(c.now() == 60_000);
        assertTrue((System.nanoTime() - cpuBefore) / 1_000_000 < 5_000, "a virtual minute must take well under 5 real seconds");
    }

    @Test
    void scheduledActionsRunAtTheirVirtualTime() {
        SimCluster c = new SimCluster(SimConfig.of(1, 3));
        List<Long> ran = new ArrayList<>();
        c.schedule(500, () -> ran.add(c.now()));
        c.schedule(100, () -> ran.add(c.now()));
        c.runFor(1_000);
        assertEquals(List.of(100L, 500L), ran);
    }

    @Test
    void aSingleNodeClusterElectsItselfAndNeverTimesOutAgain() {
        SimCluster c = new SimCluster(SimConfig.of(1, 1));
        c.runFor(10_000);
        assertEquals(Role.LEADER, c.role(1));
        assertEquals(1, c.term(1));
    }
}
