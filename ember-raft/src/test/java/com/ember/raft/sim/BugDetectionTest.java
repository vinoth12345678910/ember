package com.ember.raft.sim;

import com.ember.raft.Bug;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The proof that the checkers can see: inject each bug, run random seeds, and require a violation of a sensible kind.
 */
class BugDetectionTest {

    private static final int MAX_SEEDS = 3_000;

    private record Found(int seedsNeeded, long seed, String invariant, Map<String, Integer> byInvariant, int livenessFailures) {
    }

    private static Found hunt(Bug bug) {
        Map<String, Integer> byInvariant = new TreeMap<>();
        int liveness = 0;
        int firstNeeded = -1;
        long firstSeed = -1;
        String firstInvariant = null;
        for (int i = 0; i < MAX_SEEDS; i++) {
            long seed = i;
            int nodes = seed % 2 == 0 ? 3 : 5;
            try {
                RandomScenario.run(seed, nodes, bug);
            } catch (InvariantViolation v) {
                byInvariant.merge(v.invariant(), 1, Integer::sum);
                if (firstNeeded < 0) {
                    firstNeeded = i + 1;
                    firstSeed = seed;
                    firstInvariant = v.invariant();
                }
            } catch (LivenessFailure l) {
                liveness++;
            }
        }
        return new Found(firstNeeded, firstSeed, firstInvariant, byInvariant, liveness);
    }

    private static void check(Bug bug, Set<String> sensibleKinds) {
        Found f = hunt(bug);
        System.out.printf("BUG %-28s first caught by %-20s at seed %d (after %d seeds); violations by kind over %d seeds: %s; liveness failures: %d%n",
                bug, f.invariant(), f.seed(), f.seedsNeeded(), MAX_SEEDS, f.byInvariant(), f.livenessFailures());
        assertTrue(f.seedsNeeded() > 0, bug + " was NOT caught in " + MAX_SEEDS + " seeds: make the scenarios nastier");
        // at least one seed must trip a checker of a sensible kind (other sanity checkers may fire first or as well)
        assertTrue(f.byInvariant().keySet().stream().anyMatch(sensibleKinds::contains),
                bug + " was caught, but never by a sensible checker: " + f.byInvariant());
    }

    @Test
    void voteWithoutLogCheckIsCaught() {
        check(Bug.VOTE_WITHOUT_LOG_CHECK, Set.of("LeaderCompleteness", "StateMachineSafety"));
    }

    @Test
    void forgettingTheVoteOnRestartIsCaught() {
        check(Bug.FORGET_VOTED_FOR_ON_RESTART, Set.of("ElectionSafety"));
    }

    @Test
    void acknowledgingBeforeSyncIsCaught() {
        check(Bug.ACK_BEFORE_SYNC, Set.of("LeaderCompleteness", "StateMachineSafety"));
    }

    @Test
    void truncatingOnEveryAppendIsCaught() {
        check(Bug.TRUNCATE_ON_EVERY_APPEND, Set.of("LogMatching", "StateMachineSafety", "LeaderCompleteness"));
    }

    @Test
    void commitOldTermByCountingIsCaughtByRandomSeedsOrByTheFigure8Scenario() {
        Found f = hunt(Bug.COMMIT_OLD_TERM_BY_COUNTING);
        System.out.printf("BUG %-28s random seeds: first caught by %s after %d seeds; by kind: %s%n",
                Bug.COMMIT_OLD_TERM_BY_COUNTING, f.invariant(), f.seedsNeeded(), f.byInvariant());
        boolean figure8 = false;
        try {
            SimCluster c = Scenarios.figure8Cluster(Bug.COMMIT_OLD_TERM_BY_COUNTING, 8, false);
            Scenarios.playFigure8(c);
        } catch (InvariantViolation v) {
            figure8 = true;
        }
        assertTrue(figure8, "the scripted Figure 8 schedule must always catch it");
        if (f.seedsNeeded() > 0) {
            assertTrue(Set.of("LeaderCompleteness", "StateMachineSafety").contains(f.invariant()), f.invariant());
        }
    }

    @Test
    void theCorrectCodeProducesNoViolationsOnTheSameSeeds() {
        Found f = hunt(Bug.NONE);
        assertEquals(-1, f.seedsNeeded(), "false alarm with Bug.NONE: " + f.byInvariant());
        assertEquals(0, f.livenessFailures());
        assertTrue(new LinkedHashMap<>(f.byInvariant()).isEmpty());
    }
}
