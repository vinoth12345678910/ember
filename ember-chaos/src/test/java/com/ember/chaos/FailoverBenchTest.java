package com.ember.chaos;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** The failover and catch-up measurements, run against a real in-process cluster (tiny numbers: the tool, not the benchmark). */
class FailoverBenchTest {

    @Test
    void statisticsUseTheDocumentedDefinitions() {
        FailoverBench.Stats s = FailoverBench.Stats.of(List.of(5L, 1L, 9L, 3L, 7L, 2L, 8L, 4L, 6L, 10L));
        assertEquals(10, s.n());
        assertEquals(1, s.min());
        assertEquals(6, s.median(), "the element at index n/2 of the sorted list");
        assertEquals(9, s.p90(), "ceil(0.9 * n)-th smallest");
        assertEquals(10, s.max());
        assertEquals(5.5, s.mean(), 1e-9);
        assertEquals(42, FailoverBench.Stats.of(List.of(42L)).median());
    }

    @Test
    void killingTheLeaderRepeatedlyMeasuresARecoveryEachTime(@TempDir Path dir) throws Exception {
        try (InProcessCluster cluster = new InProcessCluster(dir, 3, false)) {
            InProcessDriver driver = new InProcessDriver(cluster);
            List<String> admins = new ArrayList<>();
            for (int i = 1; i <= 3; i++) admins.add(cluster.adminAddress(i));
            List<Long> millis = FailoverBench.failover(driver, cluster.bootstrap(), admins, 3, "failover-test");
            assertEquals(3, millis.size());
            for (long m : millis) {
                assertTrue(m > 0 && m < 20_000, "recovery took " + m + " ms: " + millis);
            }
            assertTrue(cluster.awaitReady(60_000), "every killed leader was restarted");
        }
    }

    @Test
    void aBrokerThatMissedRecordsIsReadyAndCompleteAfterItsRestart(@TempDir Path dir) throws Exception {
        try (InProcessCluster cluster = new InProcessCluster(dir, 3, false)) {
            InProcessDriver driver = new InProcessDriver(cluster);
            List<String> admins = new ArrayList<>();
            for (int i = 1; i <= 3; i++) admins.add(cluster.adminAddress(i));
            FailoverBench.CatchUp result = FailoverBench.catchUp(driver, cluster.bootstrap(), admins, 3, 4_000, "catchup-test");
            assertEquals(4_000, result.records());
            assertEquals(3, result.victim());
            assertTrue(result.millis() > 0 && result.millis() < 60_000, result.toString());
            assertEquals(4_000, FailoverBench.totalEnd(admins.get(2), "catchup-test"), "the restarted broker holds every record");
        }
    }
}
