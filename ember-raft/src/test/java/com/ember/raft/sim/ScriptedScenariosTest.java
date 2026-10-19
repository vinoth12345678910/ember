package com.ember.raft.sim;

import com.ember.raft.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Deterministic scenarios with exact expected outcomes. All safety checkers run after every event. */
class ScriptedScenariosTest {

    private static LogEntry e(long term, String s) {
        return LogEntry.command(term, s.getBytes());
    }

    private static String text(LogEntry entry) {
        return new String(entry.command());
    }

    private static List<String> commands(List<LogEntry> log) {
        List<String> out = new ArrayList<>();
        for (LogEntry entry : log) {
            if (entry.type() == EntryType.COMMAND) out.add(text(entry));
        }
        return out;
    }

    private static void assertAllLogsEqual(SimCluster c, int... nodes) {
        for (int i = 1; i < nodes.length; i++) {
            assertEquals(c.log(nodes[0]), c.log(nodes[i]), "log of n" + nodes[i] + " differs from n" + nodes[0]);
        }
    }

    // ---------------------------------------------------------------- replication

    @Test
    void threeNodeElectionCommitsTheNoopEverywhereAndThenStaysCalmWhileCommandsReplicate() {
        SimConfig config = SimConfig.of(1, 3)
                .withElectionTimeout(1, 200, 200).withElectionTimeout(2, 5_000, 5_000).withElectionTimeout(3, 5_000, 5_000);
        SimCluster c = new SimCluster(config);
        assertTrue(c.runUntil(() -> c.currentLeader() == 1, 2_000));
        assertEquals(1, c.term(1));
        assertTrue(c.runUntil(() -> c.commitIndex(1) >= 1 && c.commitIndex(2) >= 1 && c.commitIndex(3) >= 1, 1_000));
        for (int id = 1; id <= 3; id++) {
            assertEquals(EntryType.NOOP, c.applied(id).get(0).type(), "n" + id + " applied the leader's no-op");
        }
        c.runFor(10_000);                                                    // calm
        for (int id = 1; id <= 3; id++) assertEquals(1, c.term(id));
        assertEquals(List.of("CANDIDATE@1", "LEADER@1"), c.roleHistoryOf(1));

        for (int i = 0; i < 10; i++) {
            assertTrue(c.proposeToLeader(("cmd-" + i).getBytes()).accepted());
        }
        c.runFor(1_000);
        for (int id = 1; id <= 3; id++) {
            assertEquals(11, c.commitIndex(id));
            assertEquals(c.applied(1), c.applied(id));
        }
        assertEquals(List.of("cmd-0", "cmd-1", "cmd-2", "cmd-3", "cmd-4", "cmd-5", "cmd-6", "cmd-7", "cmd-8", "cmd-9"), commands(c.applied(2)));
    }

    @Test
    void leaderCrashReelectionAndTheOldLeaderCatchesUpAsAFollower() {
        SimCluster c = new SimCluster(SimConfig.of(2, 3));
        assertTrue(c.runUntil(() -> c.currentLeader() > 0, 3_000));
        for (String s : List.of("a", "b", "c")) c.proposeToLeader(s.getBytes());
        c.runFor(1_000);
        int old = c.currentLeader();
        long oldTerm = c.term(old);
        assertEquals(4, c.commitIndex(old));

        c.crash(old);
        assertFalse(c.propose(old, "x".getBytes()).accepted());
        assertTrue(c.runUntil(() -> c.currentLeader() > 0 && c.currentLeader() != old, 5_000));
        assertTrue(c.term(c.currentLeader()) > oldTerm);
        assertTrue(c.proposeToLeader("d".getBytes()).accepted());
        c.runFor(1_000);

        c.restart(old);
        c.runFor(2_000);
        assertEquals(Role.FOLLOWER, c.role(old));
        assertAllLogsEqual(c, 1, 2, 3);
        assertEquals(List.of("a", "b", "c", "d"), commands(c.applied(old)));
        assertEquals(c.applied(1), c.applied(2));
        assertEquals(c.applied(2), c.applied(3));
    }

    @Test
    void aLeaderStuckInTheMinorityNeverCommitsAndItsEntriesVanishAfterHealing() {
        SimCluster c = new SimCluster(SimConfig.of(3, 5));
        assertTrue(c.runUntil(() -> c.currentLeader() > 0, 3_000));
        int stranded = c.currentLeader();
        assertTrue(c.proposeToLeader("before".getBytes()).accepted());
        c.runFor(1_000);
        long committedBefore = c.commitIndex(stranded);
        assertEquals(2, committedBefore);

        int partner = stranded == 1 ? 2 : 1;
        int[] majority = java.util.stream.IntStream.rangeClosed(1, 5).filter(i -> i != stranded && i != partner).toArray();
        c.partition(new int[]{stranded, partner}, majority);

        for (String s : List.of("lost-1", "lost-2", "lost-3")) {
            assertTrue(c.propose(stranded, s.getBytes()).accepted(), "the stranded leader still believes it leads");
        }
        assertTrue(c.runUntil(() -> {
            int l = c.currentLeader();
            return l > 0 && l != stranded;
        }, 5_000));
        int newLeader = c.currentLeader();
        assertTrue(c.proposeToLeader("kept".getBytes()).accepted());
        c.runFor(1_500);
        assertEquals(committedBefore, c.commitIndex(stranded), "no majority behind the stranded leader: nothing commits");
        assertTrue(c.commitIndex(newLeader) > committedBefore, "the majority side keeps making progress");
        assertTrue(commands(c.log(stranded)).containsAll(List.of("lost-1", "lost-2", "lost-3")));

        c.heal();
        c.runFor(3_000);
        assertEquals(Role.FOLLOWER, c.role(stranded));
        List<String> all = commands(c.log(stranded));
        assertEquals(List.of("before", "kept"), all, "the uncommitted entries are gone");
        assertAllLogsEqual(c, 1, 2, 3, 4, 5);
        for (int id = 1; id <= 5; id++) assertEquals(c.commitIndex(1), c.commitIndex(id));
    }

    @Test
    void aDivergentFollowerLogIsOverwrittenByTheNewLeader() {
        // n3 was the leader of term 2 and wrote two entries nobody else has. n1 and n2 know only term 1 entries
        // (the checkers would rightly reject a setup where two nodes both wrote term-2 entries: one leader per term).
        Map<Integer, InMemoryRaftStorage> disks = Map.of(
                1, new InMemoryRaftStorage(2, -1, List.of(e(1, "a"), e(1, "b"))),
                2, new InMemoryRaftStorage(2, -1, List.of(e(1, "a"), e(1, "b"))),
                3, new InMemoryRaftStorage(2, 3, List.of(e(1, "a"), e(2, "X-uncommitted"), e(2, "Y-uncommitted"))));
        SimConfig config = SimConfig.of(4, 3).withElectionTimeout(1, 200, 200)
                .withElectionTimeout(2, 5_000, 5_000).withElectionTimeout(3, 5_000, 5_000);
        SimCluster c = new SimCluster(config, disks, false);
        assertTrue(c.runUntil(() -> c.currentLeader() == 1, 3_000));
        assertEquals(3, c.term(1), "n1 campaigns in term 3 (n3's log is more up to date, so n2's vote decides)");
        c.proposeToLeader("fresh".getBytes());
        c.runFor(1_500);
        assertAllLogsEqual(c, 1, 2, 3);
        assertEquals(List.of("a", "b", "fresh"), commands(c.log(3)));
        assertFalse(c.log(3).stream().anyMatch(x -> text(x).contains("uncommitted")));
        assertEquals(4, c.commitIndex(3));
    }

    @Test
    void aDuplicateAndReorderStormKeepsEveryLogCorrect() {
        SimCluster c = new SimCluster(SimConfig.of(5, 5).withDuplicate(0.5).withLatency(1, 100));
        List<String> accepted = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            c.runFor(150);
            String cmd = "storm-" + i;
            if (c.proposeToLeader(cmd.getBytes()).accepted()) accepted.add(cmd);
        }
        assertFalse(accepted.isEmpty());
        c.setDuplicate(0.0);
        c.runFor(15_000);
        assertEquals(1, c.aliveLeaders().size());
        assertAllLogsEqual(c, 1, 2, 3, 4, 5);
        for (int id = 1; id <= 5; id++) {
            assertEquals(c.commitIndex(1), c.commitIndex(id));
            assertEquals(c.applied(1), c.applied(id));
        }
        List<String> applied = commands(c.applied(1));
        assertEquals(applied.size(), new java.util.HashSet<>(applied).size(), "no command is applied twice");
        assertTrue(c.stats().duplicated > 100, "the storm really duplicated messages: " + c.stats().duplicated);
    }

    @Test
    void aCrashLosesOnlyWhatWasNeverSynced_soEverythingAFollowerAcknowledgedSurvives() {
        SimConfig config = SimConfig.of(6, 3).withElectionTimeout(1, 200, 200)
                .withElectionTimeout(2, 5_000, 5_000).withElectionTimeout(3, 5_000, 5_000);
        SimCluster c = new SimCluster(config);
        assertTrue(c.runUntil(() -> c.commitIndex(2) >= 1, 2_000));
        c.crashOnNextSend(2);                                                // power loss right after n2's next message leaves
        for (int i = 0; i < 5; i++) c.proposeToLeader(("p" + i).getBytes());
        c.runFor(500);
        assertFalse(c.isAlive(2), "n2 lost power while sending");

        long acked = 0;
        for (String line : c.trace().tail(2000)) {
            if (line.contains("n2 -> n1 AppendEntriesResponse") && line.contains("ok match=")) {
                acked = Long.parseLong(line.substring(line.indexOf("match=") + 6, line.indexOf(')')));
            }
        }
        assertTrue(acked >= 1);
        assertTrue(c.lastLogIndex(2) >= acked, "n2 acknowledged " + acked + " but its disk only has " + c.lastLogIndex(2));
        assertFalse(c.storage(2).hasUnsyncedWrites());
        c.restart(2);
        c.runFor(1_500);
        assertAllLogsEqual(c, 1, 2, 3);
    }

    @Test
    void aSingleNodeClusterCommitsEverythingItself() {
        SimCluster c = new SimCluster(SimConfig.of(1, 1));
        c.runFor(1_000);
        for (int i = 0; i < 5; i++) assertTrue(c.proposeToLeader(("s" + i).getBytes()).accepted());
        assertEquals(6, c.commitIndex(1));
        assertEquals(6, c.applied(1).size());
    }

    // ---------------------------------------------------------------- Figure 8

    // Figure 8 of the paper: the setup and the play-by-play live in Scenarios (SimMain --scenario=figure8 shows it).

    private static SimCluster figure8(Bug bug) {
        return Scenarios.figure8Cluster(bug, 8, false);
    }

    private static void playFigure8StepsCAndD(SimCluster c) {
        c.runFor(160 - c.now());
        assertEquals(1, c.currentLeader());
        assertEquals(4, c.term(1));
        assertEquals(2, c.lastLogIndex(3), "index 2 (term 2) is now stored on n1, n2, n3 and n4: a majority");
        Scenarios.playFigure8(c);
    }

    @Test
    void figure8_withTheCorrectRuleTheOldTermEntryIsNeverCommittedByCountingAndNothingIsLost() {
        SimCluster c = figure8(Bug.NONE);
        playFigure8StepsCAndD(c);
        assertEquals(5, c.currentLeader(), "n5 wins term 5 as in the paper");
        assertEquals(5, c.term(5));
        List<LogEntry> committed = c.checker().committedEntries();
        assertFalse(committed.stream().anyMatch(x -> text(x).equals("term2-entry")),
                "the term-2 entry was never committed, so overwriting it loses nothing");
        assertTrue(committed.size() >= 3);
        assertEquals(List.of("a", "term3-entry"), commands(c.log(5)));
        assertAllLogsEqual(c, 2, 3, 4, 5);
    }

    @Test
    void figure8_withTheCommitByCountingBugTheCheckersReportALostCommittedEntry() {
        SimCluster c = figure8(Bug.COMMIT_OLD_TERM_BY_COUNTING);
        InvariantViolation v = assertThrows(InvariantViolation.class, () -> playFigure8StepsCAndD(c));
        assertTrue(v.invariant().equals("LeaderCompleteness") || v.invariant().equals("StateMachineSafety"), v.invariant());
        assertTrue(v.getMessage().contains("term=2"), "the report names the lost term-2 entry: " + v.getMessage());
        assertEquals(8, v.seed());
        assertFalse(v.traceTail().isEmpty());
    }
}
