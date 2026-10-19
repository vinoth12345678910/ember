package com.ember.raft.sim;

import com.ember.raft.Bug;
import com.ember.raft.EntryType;
import com.ember.raft.LogEntry;
import com.ember.raft.Role;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * A chaos phase of random failures, then a healing phase and a liveness check. Everything random
 * comes from the seed, so a failing seed is a reproducible test case.
 */
public final class RandomScenario {

    public static final long CHAOS_MS = 30_000;

    /** What happened in one run. */
    public record Result(long seed, int nodes, long events, long traceHash, long virtualMs) {
    }

    private RandomScenario() {
    }

    /** Phase 5: CheckQuorum is a coin flip per seed, so about half of all seeds exercise it. */
    public static boolean checkQuorumFor(long seed) {
        return new Random(seed * 31 + 17).nextBoolean();
    }

    public static SimCluster newCluster(long seed, int nodes, Bug bug, boolean fullTrace) {
        return newCluster(seed, nodes, bug, fullTrace, checkQuorumFor(seed));
    }

    public static SimCluster newCluster(long seed, int nodes, Bug bug, boolean fullTrace, boolean checkQuorum) {
        return new SimCluster(SimConfig.of(seed, nodes).withBug(bug).withCheckQuorum(checkQuorum), java.util.Map.of(), fullTrace);
    }

    /** Runs the whole scenario on a fresh cluster. Throws InvariantViolation or LivenessFailure. */
    public static Result run(long seed, int nodes, Bug bug) {
        return run(newCluster(seed, nodes, bug, false), seed, CHAOS_MS);
    }

    /** Same, with CheckQuorum forced on or off for every seed. */
    public static Result run(long seed, int nodes, Bug bug, boolean checkQuorum) {
        return run(newCluster(seed, nodes, bug, false, checkQuorum), seed, CHAOS_MS);
    }

    public static Result run(SimCluster c, long seed, long chaosMs) {
        Random rng = new Random(seed * 6364136223846793005L + 1442695040888963407L);   // separate stream from nodes and network
        int[] counter = {0};
        chaosPhase(c, rng, chaosMs, counter);
        List<String> finalCommands = healingPhase(c, seed, counter);
        assertLiveness(c, finalCommands);
        return new Result(seed, c.nodeCount(), c.stats().events, c.trace().hash(), c.now());
    }

    private static byte[] nextCommand(SimCluster c, int[] counter) {
        return ("cmd-" + c.config().seed() + "-" + counter[0]++).getBytes(StandardCharsets.UTF_8);
    }

    // ------------------------------------------------------------------ chaos

    private static void chaosPhase(SimCluster c, Random rng, long chaosMs, int[] counter) {
        int n = c.nodeCount();
        while (c.now() < chaosMs) {
            c.runFor(50 + rng.nextInt(551));
            int roll = rng.nextInt(100);
            if (roll < 40) {                                            // propose a unique command to a random node
                c.propose(1 + rng.nextInt(n), nextCommand(c, counter));
            } else if (roll < 50) {                                     // crash a random alive node (ANY number may be down)
                int victim = 1 + rng.nextInt(n);
                if (c.isAlive(victim)) {
                    switch (rng.nextInt(3)) {
                        case 0 -> c.crash(victim);
                        case 1 -> c.crashOnNextSend(victim, rng.nextBoolean() ? 1 + rng.nextInt(40) : -1);   // power loss right after its next message leaves, maybe a quick reboot
                        default -> {
                            // QUICK REBOOT: back within the message latency, so messages that were already on the
                            // wire reach the NEW incarnation. Added in Step 9: without it a node that forgets its
                            // vote could never be asked again in the same term (see docs/phase4/bug-injection.md).
                            c.crash(victim);
                            c.schedule(1 + rng.nextInt(40), () -> c.restart(victim));
                        }
                    }
                }
            } else if (roll < 65) {                                     // restart a random dead node
                List<Integer> dead = new ArrayList<>();
                for (int id = 1; id <= n; id++) {
                    if (!c.isAlive(id)) dead.add(id);
                }
                if (!dead.isEmpty()) {
                    c.restart(dead.get(rng.nextInt(dead.size())));
                }
            } else if (roll < 73) {                                     // random bipartition
                if (n >= 2) {
                    List<Integer> left = new ArrayList<>();
                    List<Integer> right = new ArrayList<>();
                    do {
                        left.clear();
                        right.clear();
                        for (int id = 1; id <= n; id++) {
                            (rng.nextBoolean() ? left : right).add(id);
                        }
                    } while (left.isEmpty() || right.isEmpty());
                    c.partition(left.stream().mapToInt(Integer::intValue).toArray(),
                            right.stream().mapToInt(Integer::intValue).toArray());
                }
            } else if (roll < 83) {                                     // heal
                c.heal();
            } else if (roll < 90) {                                     // change drop (0-25%) and duplicate (0-5%) rates
                c.setDrop(rng.nextDouble() * 0.25);
                c.setDuplicate(rng.nextDouble() * 0.05);
                // Step 9: also a "reboot right after granting a vote" chaos level (0-40%). Needed to catch a node
                // that forgets its vote: it is only dangerous in the few ms between voting and a rival's request.
                c.setRebootAfterVoteProbability(rng.nextDouble() * 0.4);
            } else if (roll < 95) {                                     // isolate one node
                c.isolate(1 + rng.nextInt(n));
            }                                                           // else: do nothing
        }
    }

    // ------------------------------------------------------------------ healing

    private static List<String> healingPhase(SimCluster c, long seed, int[] counter) {
        c.heal();
        c.clearCrashOnNextSend();
        for (int id = 1; id <= c.nodeCount(); id++) {
            c.restart(id);
        }
        c.setDrop(0);
        c.setDuplicate(0);
        c.setRebootAfterVoteProbability(0);
        c.runFor(15_000);

        List<String> finalCommands = new ArrayList<>();
        for (int k = 0; k < 5; k++) {
            byte[] cmd = nextCommand(c, counter);
            long started = c.now();
            while (!c.proposeToLeader(cmd).accepted()) {
                if (c.now() - started >= 5_000) {
                    throw new LivenessFailure("no leader accepted final command " + k + " within 5 s of virtual time (leaders: "
                            + c.aliveLeaders() + ")", seed, c.trace().tail(60));
                }
                c.runFor(100);
            }
            finalCommands.add(new String(cmd, StandardCharsets.UTF_8));
        }
        c.runFor(5_000);
        return finalCommands;
    }

    /** After healing, a working Raft cluster has one leader and identical, fully committed logs. */
    static void assertLiveness(SimCluster c, List<String> finalCommands) {
        long seed = c.config().seed();
        int n = c.nodeCount();
        for (int id = 1; id <= n; id++) {
            if (!c.isAlive(id)) {
                throw fail(c, "n" + id + " is not alive after the healing phase");
            }
        }
        List<Integer> leaders = c.aliveLeaders();
        if (leaders.size() != 1) {
            throw fail(c, "expected exactly one leader after healing, found " + leaders);
        }
        for (int id = 1; id <= n; id++) {
            if (c.role(id) != Role.LEADER && c.node(id).leaderId() != leaders.get(0)) {
                throw fail(c, "n" + id + " does not follow the leader n" + leaders.get(0) + " (it follows n" + c.node(id).leaderId() + ")");
            }
            if (!c.log(id).equals(c.log(1))) {
                throw fail(c, "log of n" + id + " (" + c.log(id).size() + " entries) differs from the log of n1 (" + c.log(1).size() + ")");
            }
            if (c.commitIndex(id) != c.commitIndex(1)) {
                throw fail(c, "commit index of n" + id + " is " + c.commitIndex(id) + " but n1 has " + c.commitIndex(1));
            }
            if (!c.applied(id).equals(c.applied(1))) {
                throw fail(c, "applied sequence of n" + id + " differs from the one of n1");
            }
        }
        if (c.commitIndex(1) != c.log(1).size()) {
            throw fail(c, "commit index " + c.commitIndex(1) + " does not cover the whole log of " + c.log(1).size() + " entries");
        }
        List<String> appliedCommands = new ArrayList<>();
        for (LogEntry e : c.applied(1)) {
            if (e.type() == EntryType.COMMAND) {
                appliedCommands.add(new String(e.command(), StandardCharsets.UTF_8));
            }
        }
        for (String cmd : finalCommands) {
            long count = appliedCommands.stream().filter(cmd::equals).count();
            if (count != 1) {
                throw fail(c, "final command " + cmd + " appears " + count + " times in the applied sequence (expected exactly once)");
            }
        }
    }

    private static LivenessFailure fail(SimCluster c, String detail) {
        return new LivenessFailure(detail, c.config().seed(), c.trace().tail(60));
    }

    // ------------------------------------------------------------------ reporting

    /** Outcome of runChecked: either ok, or the failure and where its full trace was written. */
    public record Checked(Result result, RuntimeException failure, Path traceFile) {
        public boolean ok() {
            return failure == null;
        }
    }

    /**
     * Runs one seed; on failure re-runs it with full recording (determinism makes this exact) and writes
     * target/raft-sim/seed-N.trace so the failure can be read and replayed.
     */
    public static Checked runChecked(long seed, int nodes, Bug bug, Path traceDir) {
        try {
            return new Checked(run(seed, nodes, bug), null, null);
        } catch (InvariantViolation | LivenessFailure first) {
            SimCluster again = newCluster(seed, nodes, bug, true);
            try {
                run(again, seed, CHAOS_MS);
            } catch (InvariantViolation | LivenessFailure ignored) {
                // same failure, now with every trace line recorded
            }
            Path file = traceDir == null ? null : writeTrace(traceDir, seed, nodes, bug, first, again);
            return new Checked(null, (RuntimeException) first, file);
        }
    }

    private static Path writeTrace(Path dir, long seed, int nodes, Bug bug, Throwable failure, SimCluster c) {
        try {
            Files.createDirectories(dir);
            Path file = dir.resolve("seed-" + seed + ".trace");
            List<String> lines = new ArrayList<>();
            lines.add("# seed=" + seed + " nodes=" + nodes + " bug=" + bug);
            lines.add("# replay: java -cp ember-raft/target/classes com.ember.raft.sim.SimMain --scenario=random --seed="
                    + seed + " --nodes=" + nodes + (bug != Bug.NONE ? " --bug=" + bug : ""));
            lines.add("# failure: " + failure.getMessage().split("\n")[0]);
            lines.addAll(c.trace().full());
            Files.write(file, lines);
            return file;
        } catch (IOException e) {
            return null;
        }
    }
}
