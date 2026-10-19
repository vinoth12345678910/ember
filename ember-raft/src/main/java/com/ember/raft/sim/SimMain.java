package com.ember.raft.sim;

import com.ember.raft.Bug;

import java.util.List;
import java.util.Locale;

/**
 * Learning tool: watch elections, crashes and replication.
 *   java -cp ember-raft/target/classes com.ember.raft.sim.SimMain --seed=42 --nodes=5 --scenario=leader-crash
 */
public final class SimMain {

    private SimMain() {
    }

    public static void main(String[] args) {
        long seed = 1;
        int nodes = 3;
        long durationMs = -1;                  // -1: the scenario's own default (random: the 30 s chaos phase the tests use)
        String scenario = "election";
        int proposals = 5;
        boolean quiet = false;
        Bug bug = Bug.NONE;
        for (String a : args) {
            int eq = a.indexOf('=');
            String name = eq < 0 ? a : a.substring(0, eq);
            String value = eq < 0 ? "" : a.substring(eq + 1);
            switch (name) {
                case "--seed" -> seed = Long.parseLong(value);
                case "--nodes" -> nodes = Integer.parseInt(value);
                case "--duration-ms" -> durationMs = Long.parseLong(value);
                case "--scenario" -> scenario = value;
                case "--proposals" -> proposals = Integer.parseInt(value);
                case "--quiet" -> quiet = true;
                case "--bug" -> bug = Bug.valueOf(value.toUpperCase(Locale.ROOT));
                default -> {
                    System.err.println("unknown flag " + a);
                    System.err.println("usage: SimMain [--seed=N] [--nodes=3|5] [--duration-ms=20000] "
                            + "[--scenario=random|election|leader-crash|partition|figure8] [--proposals=K] [--bug=NAME] [--quiet]");
                    System.exit(2);
                }
            }
        }
        if (scenario.equals("figure8")) {
            nodes = 5;
        }
        if (durationMs < 0) {
            durationMs = scenario.equals("random") ? RandomScenario.CHAOS_MS : 20_000;
        }
        SimConfig config = SimConfig.of(seed, nodes).withBug(bug);
        SimCluster cluster = Scenarios.create(scenario, config, true);
        InvariantViolation violation = null;
        try {
            Scenarios.run(scenario, cluster, durationMs, proposals);
        } catch (InvariantViolation v) {
            violation = v;
        }
        if (!quiet) {
            List<String> lines = cluster.trace().full();
            for (String line : lines) {
                System.out.println(line);
            }
        }
        System.out.println();
        printSummary(cluster, scenario, violation);
        if (violation != null) {
            System.out.println();
            System.out.println(violation.getMessage());
            System.exit(1);
        }
    }

    static void printSummary(SimCluster c, String scenario, InvariantViolation violation) {
        System.out.println("=== summary: scenario=" + scenario + " seed=" + c.config().seed() + " nodes=" + c.nodeCount()
                + (c.config().bug() != Bug.NONE ? " BUG=" + c.config().bug() : ""));
        System.out.println("virtual time: " + c.now() + " ms, trace lines: " + c.trace().lineCount() + ", trace hash: "
                + Long.toHexString(c.trace().hash()));
        System.out.println("leader history (term -> node): " + c.leaderHistory());
        for (int id = 1; id <= c.nodeCount(); id++) {
            System.out.println("n" + id + ": " + (c.isAlive(id) ? c.role(id) : "DEAD") + " term=" + c.term(id)
                    + " log=" + c.lastLogIndex(id) + " commit=" + c.commitIndex(id) + " roles=" + c.roleHistoryOf(id));
        }
        SimCluster.Stats s = c.stats();
        System.out.println("messages: sent=" + s.sent + " delivered=" + s.delivered + " dropped=" + s.dropped
                + " duplicated=" + s.duplicated + " lost(partition)=" + s.lostToPartition + " lost(dead node)=" + s.lostToDeadNode
                + " events=" + s.events);
        System.out.println(violation == null ? "invariants: OK" : "invariants: VIOLATED (" + violation.invariant() + ")");
    }
}
