package com.ember.chaos;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.IntToDoubleFunction;

import static org.junit.jupiter.api.Assertions.*;

class SoakAnalysisTest {

    /** 10-second samples for one broker over `minutes`; each function gives the metric at sample i. */
    private static List<MetricsSampler.Row> series(int minutes, IntToDoubleFunction heapAfterGc, IntToDoubleFunction threads, IntToDoubleFunction fds) {
        List<MetricsSampler.Row> rows = new ArrayList<>();
        for (int i = 0; i < minutes * 6; i++) {
            rows.add(new MetricsSampler.Row(i * 10.0, 1, i * 10.0, 0, heapAfterGc.applyAsDouble(i), threads.applyAsDouble(i), fds.applyAsDouble(i), 0, 0, 0, 0));
        }
        return rows;
    }

    private static SoakAnalysis.Trend find(List<SoakAnalysis.Trend> trends, String metric) {
        return trends.stream().filter(t -> t.metric().equals(metric)).findFirst().orElseThrow();
    }

    @Test
    void aHealthyBrokerIsFlatEvenWithNoiseAndASawtooth() {
        Random random = new Random(1);
        List<SoakAnalysis.Trend> trends = SoakAnalysis.analyze(series(15,
                i -> 150e6 + random.nextGaussian() * 8e6,                    // live set wobbling around 150 MB
                i -> 60 + (i % 7 == 0 ? 2 : 0),                              // threads wiggle with connections
                i -> 120 + random.nextInt(6)), 120);
        assertTrue(SoakAnalysis.allFlat(trends), SoakAnalysis.table(trends));
        assertEquals(3, trends.size());
    }

    @Test
    void aClimbingThreadCountIsALeak() {
        List<SoakAnalysis.Trend> trends = SoakAnalysis.analyze(series(15, i -> 150e6, i -> 60 + i / 3, i -> 120), 120);
        assertFalse(find(trends, "threads").flat(), SoakAnalysis.table(trends));
        assertTrue(find(trends, "open_fds").flat());
        assertTrue(find(trends, "threads").slopePerHour() > 100, "about 120 more threads per hour: " + find(trends, "threads").slopePerHour());
    }

    @Test
    void aClimbingFileDescriptorCountIsALeak() {
        List<SoakAnalysis.Trend> trends = SoakAnalysis.analyze(series(15, i -> 150e6, i -> 60, i -> 100 + i), 120);
        assertFalse(find(trends, "open_fds").flat(), SoakAnalysis.table(trends));
    }

    @Test
    void aTransientHighReadingIsNotALeakButARisingFloorIs() {
        // the live set sits at ~6 MB but some readings jump to 16 MB for a while (one pool's reading not yet refreshed): not a leak
        List<SoakAnalysis.Trend> spike = SoakAnalysis.analyze(series(15, i -> i > 60 && i < 75 ? 16e6 : 6e6, i -> 60, i -> 120), 120);
        assertTrue(find(spike, "heap_after_gc").flat(), SoakAnalysis.table(spike));
        // every reading climbs: the floor rises, and that is a leak
        List<SoakAnalysis.Trend> leaking = SoakAnalysis.analyze(series(15, i -> 6e6 + i * 1e6, i -> 60, i -> 120), 120);
        assertFalse(find(leaking, "heap_after_gc").flat(), SoakAnalysis.table(leaking));
    }

    @Test
    void theHeapReadingIsShownButDoesNotDecideTheVerdictWhileThreadsAndDescriptorsDo() {
        List<SoakAnalysis.Trend> heapOnly = SoakAnalysis.analyze(series(15, i -> 6e6 + i * 1e6, i -> 60, i -> 120), 120);
        assertFalse(find(heapOnly, "heap_after_gc").flat(), "the statistic itself still reports the rise");
        assertTrue(SoakAnalysis.allFlat(heapOnly), "but the heap gauge is informational: it does not fail the soak");
        assertFalse(SoakAnalysis.allFlat(SoakAnalysis.analyze(series(15, i -> 6e6, i -> 60 + i / 2, i -> 120), 120)), "threads do");
    }

    @Test
    void aSamplesCsvCanBeReadBackAndAnalyzed(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        java.nio.file.Path csv = dir.resolve("s.csv");
        StringBuilder sb = new StringBuilder(MetricsSampler.HEADER).append('\n');
        for (int i = 0; i < 60; i++) sb.append(i * 10).append(",1,").append(i * 10).append(",0,6000000,55,100,0,0,0,0\n");
        java.nio.file.Files.writeString(csv, sb);
        List<MetricsSampler.Row> rows = SoakAnalysis.readCsv(csv);
        assertEquals(60, rows.size());
        assertEquals(55, rows.get(0).threads());
        assertTrue(SoakAnalysis.allFlat(SoakAnalysis.analyze(rows, 120)));
        java.nio.file.Files.writeString(csv, "elapsed_s,node\n1,2\n");
        assertThrows(IllegalArgumentException.class, () -> SoakAnalysis.readCsv(csv));
    }

    @Test
    void aGrowingLiveSetIsALeakButASmallDriftIsNot() {
        assertFalse(find(SoakAnalysis.analyze(series(15, i -> 100e6 + i * 5e6, i -> 60, i -> 120), 120), "heap_after_gc").flat());
        assertTrue(find(SoakAnalysis.analyze(series(15, i -> 150e6 + i * 20_000, i -> 60, i -> 120), 120), "heap_after_gc").flat(),
                "a few KB per sample is within the stated tolerance");
    }

    @Test
    void theWarmUpIsIgnoredSoStartupGrowthIsNotAccused() {
        // climbing during the first two minutes (class loading, connection pools), flat afterwards
        List<SoakAnalysis.Trend> trends = SoakAnalysis.analyze(series(15, i -> i < 12 ? 40e6 + i * 10e6 : 160e6, i -> i < 12 ? 20 + i * 4 : 68, i -> 100), 120);
        assertTrue(SoakAnalysis.allFlat(trends), SoakAnalysis.table(trends));
    }

    @Test
    void aCrashAndRestartIsNotALeakEvenThoughTheThreadCountStartsLowAgain() {
        // the nemesis restarts the broker at 600 s; its process then starts with 25 threads and needs ~90 s to grow back to 55
        List<MetricsSampler.Row> rows = new ArrayList<>();
        for (int i = 0; i < 90; i++) {
            double t = i * 10.0;
            double uptime = t < 600 ? t : t - 600;
            double threads = uptime < 100 ? 25 + uptime / 100 * 30 : 55;
            rows.add(new MetricsSampler.Row(t, 1, uptime, 0, 150e6, threads, 100, 0, 0, 0, 0));
        }
        List<SoakAnalysis.Trend> trends = SoakAnalysis.analyze(rows, 120);
        assertTrue(SoakAnalysis.allFlat(trends), SoakAnalysis.table(trends));
    }

    @Test
    void aRealLeakInsideALongLivedProcessIsStillCaughtAfterRestarts() {
        List<MetricsSampler.Row> rows = new ArrayList<>();
        for (int i = 0; i < 90; i++) {
            double t = i * 10.0;
            double uptime = t < 300 ? t : t - 300;                                    // one restart at 300 s
            rows.add(new MetricsSampler.Row(t, 1, uptime, 0, 150e6, 55 + (uptime > 120 ? (t / 10) : 0), 100, 0, 0, 0, 0));   // threads climb after warm-up
        }
        assertFalse(find(SoakAnalysis.analyze(rows, 120), "threads").flat());
    }

    @Test
    void tooFewSamplesAreReportedHonestlyNotJudged() {
        List<SoakAnalysis.Trend> trends = SoakAnalysis.analyze(series(1, i -> 1, i -> 1, i -> 1).subList(0, 5), 0);
        assertTrue(find(trends, "threads").note().contains("too few"));
        assertTrue(SoakAnalysis.table(trends).contains("too few"));
    }

    @Test
    void theSlopeOfAStraightLineIsExact() {
        List<double[]> line = new ArrayList<>();
        for (int i = 0; i < 20; i++) line.add(new double[]{i * 10.0, 5 + 0.5 * i * 10.0});       // 0.5 units per second
        assertEquals(0.5, SoakAnalysis.slope(line), 1e-9);
    }

    @Test
    void aMetricsBodyIsParsedAndPerPartitionSeriesAreSummed() {
        String body = """
                # HELP x y
                ember_process_uptime_seconds 321.5
                ember_jvm_heap_used_bytes 1000
                ember_jvm_heap_after_gc_bytes 400
                ember_jvm_threads 55
                ember_process_open_fds 130
                ember_jvm_direct_buffer_used_bytes 77
                ember_log_size_bytes{topic="a",partition="0"} 100
                ember_log_size_bytes{topic="a",partition="1"} 200
                ember_group_lag{group="g",topic="a",partition="0"} 5
                ember_group_lag{group="g",topic="a",partition="1"} 7
                ember_raft_pending_proposals{group="a-0"} 1
                ember_raft_pending_proposals{group="__meta-0"} 2
                """;
        MetricsSampler.Row r = MetricsSampler.sample(12.5, 2, body);
        assertEquals(2, r.node());
        assertEquals(321.5, r.uptimeSeconds());
        assertEquals(1000, r.heapUsed());
        assertEquals(400, r.heapAfterGc());
        assertEquals(55, r.threads());
        assertEquals(130, r.openFds());
        assertEquals(77, r.directBuffers());
        assertEquals(300, r.logBytes());
        assertEquals(12, r.groupLag());
        assertEquals(3, r.pendingProposals());
    }

    @Test
    void theSamplerScrapesARealBrokerAndWritesACsv(@TempDir Path dir) throws Exception {
        try (InProcessCluster cluster = new InProcessCluster(dir.resolve("c"), 1, false)) {
            assertTrue(cluster.awaitReady(30_000));
            Path csv = dir.resolve("samples.csv");
            MetricsSampler sampler = new MetricsSampler(List.of(cluster.adminAddress(1)), 100, csv);
            sampler.start();
            assertTrue(InProcessCluster.await(15_000, () -> sampler.rows().size() >= 3), "three samples");
            sampler.close();
            List<String> lines = Files.readAllLines(csv);
            assertEquals(MetricsSampler.HEADER, lines.get(0));
            assertTrue(lines.size() >= 4);
            MetricsSampler.Row first = sampler.rows().get(0);
            assertTrue(first.threads() > 5 && first.heapUsed() > 1_000_000 && first.openFds() > 10, first.toString());
        }
    }
}
