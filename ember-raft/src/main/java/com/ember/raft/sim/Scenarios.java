package com.ember.raft.sim;

import com.ember.raft.Bug;
import com.ember.raft.InMemoryRaftStorage;
import com.ember.raft.LogEntry;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/** Named, readable scenarios for SimMain and tests. */
public final class Scenarios {

    private Scenarios() {
    }

    /** Builds the cluster a scenario needs (figure8 needs five pre-populated nodes). */
    public static SimCluster create(String name, SimConfig config, boolean fullTrace) {
        if (name.equals("figure8")) {
            return figure8Cluster(config.bug(), config.seed(), fullTrace);
        }
        if (name.equals("random")) {
            return RandomScenario.newCluster(config.seed(), config.nodeCount(), config.bug(), fullTrace);
        }
        return new SimCluster(config, Map.of(), fullTrace);
    }

    public static void run(String name, SimCluster c, long durationMs, int proposals) {
        switch (name) {
            case "election" -> election(c, durationMs, proposals);
            case "leader-crash" -> leaderCrash(c, proposals);
            case "partition" -> partition(c, proposals);
            case "figure8" -> playFigure8(c);
            case "random" -> RandomScenario.run(c, c.config().seed(), durationMs);
            default -> throw new IllegalArgumentException("unknown scenario: " + name
                    + " (use random, election, leader-crash, partition or figure8)");
        }
    }

    /** Let the cluster elect a leader, replicate some commands, and run. */
    static void election(SimCluster c, long durationMs, int proposals) {
        c.runUntil(() -> c.currentLeader() > 0, 5_000);
        propose(c, "cmd", proposals);
        c.runFor(durationMs);
    }

    /** Elect, replicate, kill the leader, watch the re-election, bring the old leader back. */
    static void leaderCrash(SimCluster c, int proposals) {
        c.runUntil(() -> c.currentLeader() > 0, 5_000);
        propose(c, "before", proposals);
        c.runFor(500);
        int old = c.currentLeader();
        c.crash(old);
        c.runUntil(() -> c.currentLeader() > 0 && c.currentLeader() != old, 5_000);
        propose(c, "after", proposals);
        c.runFor(500);
        c.restart(old);
        c.runFor(3_000);
    }

    /** The leader and one friend are cut off from the majority: the minority leader cannot commit. */
    static void partition(SimCluster c, int proposals) {
        c.runUntil(() -> c.currentLeader() > 0, 5_000);
        propose(c, "before", proposals);
        c.runFor(500);
        int old = c.currentLeader();
        int friend = old == 1 ? 2 : 1;
        int[] others = java.util.stream.IntStream.rangeClosed(1, c.nodeCount()).filter(i -> i != old && i != friend).toArray();
        c.partition(new int[]{old, friend}, others);
        for (int i = 0; i < proposals; i++) {
            c.propose(old, cmd("lost-" + i));                      // accepted, never committed
        }
        c.runUntil(() -> c.currentLeader() > 0 && c.currentLeader() != old, 5_000);
        propose(c, "majority", proposals);
        c.runFor(1_000);
        c.heal();
        c.runFor(3_000);
    }

    private static void propose(SimCluster c, String prefix, int count) {
        for (int i = 0; i < count; i++) {
            c.proposeToLeader(cmd(prefix + "-" + i));
        }
    }

    // ------------------------------------------------------------------ Figure 8 of the paper

    private static LogEntry entry(long term, String s) {
        return LogEntry.command(term, s.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Figure 8, five servers. State after the paper's steps (a) and (b):
     *   n1: term 3, log [t1, t2]   (was leader of term 2 and replicated index 2 only to n2)
     *   n2: term 2, log [t1, t2]
     *   n3, n4: term 3, log [t1]   (voted for n5 in term 3)
     *   n5: term 3, log [t1, t3]   (was leader of term 3 and wrote index 2 only locally); down at first
     * Step (c): n1 is elected leader of term 4 and replicates index 2 (term 2) to n3: a majority stores it.
     * Step (d): n1 dies. If it had called index 2 committed by counting replicas, n5 still wins term 5 and
     * overwrites it. maxEntriesPerAppend = 1 and fixed latencies make the interleaving exact.
     */
    public static SimCluster figure8Cluster(Bug bug, long seed, boolean fullTrace) {
        Map<Integer, InMemoryRaftStorage> disks = Map.of(
                1, new InMemoryRaftStorage(3, -1, List.of(entry(1, "a"), entry(2, "term2-entry"))),
                2, new InMemoryRaftStorage(2, -1, List.of(entry(1, "a"), entry(2, "term2-entry"))),
                3, new InMemoryRaftStorage(3, 5, List.of(entry(1, "a"))),
                4, new InMemoryRaftStorage(3, 5, List.of(entry(1, "a"))),
                5, new InMemoryRaftStorage(3, 5, List.of(entry(1, "a"), entry(3, "term3-entry"))));
        SimConfig config = SimConfig.of(seed, 5).withLatency(10, 10).withMaxEntriesPerAppend(1).withBug(bug)
                .withElectionTimeout(1, 100, 100)
                .withElectionTimeout(2, 100_000, 100_000).withElectionTimeout(3, 100_000, 100_000)
                .withElectionTimeout(4, 100_000, 100_000).withElectionTimeout(5, 150, 150);
        SimCluster c = new SimCluster(config, disks, fullTrace);
        c.crash(5);                                                         // n5 is away during step (c)
        return c;
    }

    public static void playFigure8(SimCluster c) {
        c.runFor(160 - c.now());                                            // n1 leads term 4; index 2 (term 2) reached n3 and n4
        c.partition(new int[]{1}, new int[]{2, 3, 4, 5});                   // cut n1 off before its no-op reaches anyone else
        c.crash(1);
        c.restart(5);                                                       // n5 comes back and campaigns
        c.runFor(1_500);
    }

    static byte[] cmd(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
