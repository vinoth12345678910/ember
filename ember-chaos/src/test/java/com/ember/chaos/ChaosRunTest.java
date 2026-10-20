package com.ember.chaos;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Whole-system chaos runs on real in-process brokers with every nemesis type. The seed picks the FAULT SCHEDULE (printed on
 * failure); real timing still varies. A violation means a bug in Ember or in the checker; the checker is never relaxed.
 */
class ChaosRunTest {

    private static ChaosRunner.Config config(long seed, long millis, Path history) {
        return new ChaosRunner.Config(seed, millis, 2, 1, 3, 3, Fault.parse("all"), 400, 1200, history);
    }

    private void run(Path dir, long seed, long millis) throws Exception {
        Path history = dir.resolve("history-" + seed + ".jsonl");
        ChaosRunner.Summary summary = ChaosRunner.runInProcess(config(seed, millis, history), dir.resolve("cluster"), 3, false);
        System.out.print(summary.table());
        System.out.println("schedule: " + summary.schedule());
        if (!summary.ok()) {
            System.err.println("CHAOS VIOLATIONS with seed " + seed + ":\n" + summary.table() + "schedule: " + summary.schedule()
                    + "\nhistory kept in " + history + " (re-check with --check-only)");
            java.nio.file.Files.copy(history, java.nio.file.Path.of(System.getProperty("java.io.tmpdir")).resolve("chaos-failed-" + seed + ".jsonl"),
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
        assertTrue(summary.ok(), "seed " + seed + ": " + summary.result().violations());
        assertTrue(summary.okProduces() > 50, "the producers made hardly any progress: " + summary.okProduces());
        assertTrue(summary.result().stats().getOrDefault("nemesis.total", 0L) >= 3, "the nemesis injected too few faults");
        assertTrue(summary.result().stats().get("replica.checks") >= 10, "replicas of data, raft logs, group and topic state must have been compared");
        // the history file can be checked again offline and gives the same verdict
        assertTrue(Checker.check(History.read(history)).ok());
    }

    @Test
    void seedOneTwentySeconds(@TempDir Path dir) throws Exception {
        run(dir, 20_26_01, 20_000);
    }

    @Test
    void seedTwoTwentySeconds(@TempDir Path dir) throws Exception {
        run(dir, 20_26_02, 20_000);
    }

    @Test
    void seedThreeTwentySeconds(@TempDir Path dir) throws Exception {
        run(dir, 20_26_03, 20_000);
    }

    @Test
    @Tag("slow")
    void twoMinutesSeedA(@TempDir Path dir) throws Exception {
        run(dir, Long.getLong("cluster.seed", 0) != 0 ? Long.getLong("cluster.seed") : 7_001, 120_000);
    }

    @Test
    @Tag("slow")
    void twoMinutesSeedB(@TempDir Path dir) throws Exception {
        run(dir, 7_002, 120_000);
    }

    @Test
    @Tag("slow")
    void twoMinutesSeedC(@TempDir Path dir) throws Exception {
        run(dir, 7_003, 120_000);
    }

    @Test
    void withTwoOfThreeBrokersDownNothingIsAcknowledgedAndProgressResumesAfterHealing(@TempDir Path dir) throws Exception {
        ChaosRunner.MajorityLossResult r = ChaosRunner.runMajorityLoss(dir, 1);
        System.out.print(r.summary().table());
        System.out.println("acknowledged before / during / after the outage: " + r.acksBefore() + " / " + r.acksDuring() + " / " + r.acksAfter());
        assertTrue(r.acksBefore() >= 30);
        assertEquals(0, r.acksDuring(), "no produce may be acknowledged while a majority is down");
        assertTrue(r.acksAfter() >= 30, "progress must resume after healing");
        assertTrue(r.summary().ok(), r.summary().result().violations().toString());
    }

    @Test
    void theCommandLineToolRunsChecksAndReportsViaItsExitCode(@TempDir Path dir) throws Exception {
        Path history = dir.resolve("h.jsonl");
        int code = ChaosMain.run(new String[]{"--seed=99", "--duration=4", "--nemesis=pause,delay", "--history=" + history, "--data-dir=" + dir.resolve("data"), "--min-gap-ms=300", "--max-gap-ms=700"});
        if (code != 0) {
            java.nio.file.Files.copy(history, Path.of(System.getProperty("java.io.tmpdir")).resolve("chaos-failed-cli.jsonl"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
        assertEquals(0, code);
        assertEquals(0, ChaosMain.run(new String[]{"--check-only=" + history}));
        // a corrupted history (an acknowledged write removed from the final read) must exit non-zero
        History good = History.read(history);
        java.util.List<HistoryEvent> broken = new java.util.ArrayList<>();
        boolean dropped = false;
        for (HistoryEvent e : good.events()) {
            if (!dropped && e.kind().equals("complete") && e.type().equals("read") && e.process().equals("final") && !e.rows("records").isEmpty()) {
                java.util.Map<String, Object> d = new java.util.LinkedHashMap<>(e.data());
                java.util.List<java.util.List<Object>> rows = new java.util.ArrayList<>(e.rows("records"));
                rows.remove(0);
                d.put("records", rows);
                broken.add(new HistoryEvent(e.timeNanos(), e.kind(), e.opId(), e.process(), e.type(), e.outcome(), d));
                dropped = true;
            } else {
                broken.add(e);
            }
        }
        assertTrue(dropped);
        Path bad = dir.resolve("bad.jsonl");
        new History(broken).write(bad);
        assertEquals(1, ChaosMain.run(new String[]{"--check-only=" + bad}));
        assertEquals(2, ChaosMain.run(new String[]{"--nemesis=meteor"}));
    }
}
