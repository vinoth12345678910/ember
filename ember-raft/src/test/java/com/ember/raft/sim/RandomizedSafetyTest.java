package com.ember.raft.sim;

import com.ember.raft.Bug;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Hundreds of random failure schedules against the REAL node code, every safety law checked after
 * every event, liveness asserted after healing. -Draft.seeds=N, -Draft.seedStart=K to change the range.
 */
class RandomizedSafetyTest {

    static final Path TRACE_DIR = Path.of("target", "raft-sim");

    static long seeds() {
        return Long.getLong("raft.seeds", 300);
    }

    static long seedStart() {
        return Long.getLong("raft.seedStart", 0);
    }

    static int nodesFor(long seed) {
        return seed % 2 == 0 ? 3 : 5;
    }

    @Test
    void safetyAndLivenessHoldOnRandomSchedulesWithTheCorrectCode() {
        List<String> failures = new ArrayList<>();
        long events = 0;
        long first = seedStart();
        long start = System.nanoTime();
        for (long seed = first; seed < first + seeds(); seed++) {
            RandomScenario.Checked checked = RandomScenario.runChecked(seed, nodesFor(seed), Bug.NONE, TRACE_DIR);
            if (checked.ok()) {
                events += checked.result().events();
            } else {
                failures.add("seed " + seed + " (" + nodesFor(seed) + " nodes): " + checked.failure().getMessage()
                        + "\nfull trace: " + checked.traceFile());
                if (failures.size() >= 3) {
                    break;
                }
            }
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        System.out.printf("RandomizedSafetyTest: %d seeds from %d, %,d events in %.1f s = %,.0f events/s, %.1f seeds/s%n",
                seeds(), first, events, seconds, events / seconds, seeds() / seconds);
        assertTrue(failures.isEmpty(), String.join("\n=====\n", failures));
    }

    @Test
    void theSameSeedGivesTheSameRunAndTheSameTraceHash() {
        for (long seed : new long[]{3, 4, 77}) {
            RandomScenario.Result a = RandomScenario.run(seed, nodesFor(seed), Bug.NONE);
            RandomScenario.Result b = RandomScenario.run(seed, nodesFor(seed), Bug.NONE);
            assertEquals(a, b, "seed " + seed + " must replay identically");
        }
        assertNotEquals(RandomScenario.run(5, 5, Bug.NONE).traceHash(), RandomScenario.run(6, 5, Bug.NONE).traceHash());
    }

    @Test
    void fullTraceRecordingDoesNotChangeTheRun() {
        SimCluster ring = RandomScenario.newCluster(9, 5, Bug.NONE, false);
        SimCluster full = RandomScenario.newCluster(9, 5, Bug.NONE, true);
        RandomScenario.Result a = RandomScenario.run(ring, 9, RandomScenario.CHAOS_MS);
        RandomScenario.Result b = RandomScenario.run(full, 9, RandomScenario.CHAOS_MS);
        assertEquals(a, b);
        assertEquals(full.trace().lineCount(), full.trace().full().size());
    }

    @Test
    @Tag("slow")
    void aLongerRunWithManyMoreSeeds() {
        List<String> failures = new ArrayList<>();
        for (long seed = 100_000; seed < 100_000 + 2_000; seed++) {
            RandomScenario.Checked checked = RandomScenario.runChecked(seed, nodesFor(seed), Bug.NONE, TRACE_DIR);
            if (!checked.ok()) {
                failures.add("seed " + seed + ": " + checked.failure().getMessage() + "\nfull trace: " + checked.traceFile());
                break;
            }
        }
        assertTrue(failures.isEmpty(), String.join("\n", failures));
    }

    @Test
    void checkQuorumForcedOnKeepsEveryLawAndLivenessOverAThousandSeeds() {
        List<String> failures = new ArrayList<>();
        for (long seed = 50_000; seed < 51_000; seed++) {
            try {
                RandomScenario.run(seed, nodesFor(seed), Bug.NONE, true);
            } catch (InvariantViolation | LivenessFailure e) {
                failures.add("seed " + seed + ": " + e.getMessage());
                if (failures.size() >= 2) break;
            }
        }
        assertTrue(failures.isEmpty(), String.join("\n=====\n", failures));
    }

    @Test
    void theCoinFlipGivesBothSettingsInAboutEqualParts() {
        int on = 0;
        for (long seed = 0; seed < 1000; seed++) {
            if (RandomScenario.checkQuorumFor(seed)) on++;
        }
        assertTrue(on > 400 && on < 600, "checkQuorum on for " + on + " of 1000 seeds");
    }
}
