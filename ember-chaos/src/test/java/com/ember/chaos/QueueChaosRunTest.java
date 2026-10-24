package com.ember.chaos;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ack queues under the nemesis: workers claim, ack, nack, release and sometimes abandon messages (like a worker that dies), while brokers
 * are crashed, frozen, isolated, slowed, and the leader of "__acks" is killed on purpose. The checker then proves from the recorded
 * history alone that nothing acknowledged came back, no delivery was handed out twice, stale deliveries could not settle, nothing is stuck
 * or lost after the drain, the dead-letter list is consistent, and every replica ended with the same ack state.
 */
class QueueChaosRunTest {

    private void run(Path dir, long seed, long millis) throws Exception {
        Path history = dir.resolve("history-" + seed + ".jsonl");
        ChaosRunner.Config cfg = new ChaosRunner.Config(seed, millis, 2, 1, 1, 3, Fault.parse("all"), 400, 1200, history).withQueue(4, 120);
        ChaosRunner.Summary summary = ChaosRunner.runInProcess(cfg, dir.resolve("cluster"), 3, false);
        System.out.print(summary.table());
        System.out.println("schedule: " + summary.schedule());
        if (!summary.ok()) {
            System.err.println("QUEUE CHAOS VIOLATIONS with seed " + seed + ":\n" + summary.table() + "schedule: " + summary.schedule() + "\nhistory kept in " + history);
            java.nio.file.Files.copy(history, Path.of(System.getProperty("java.io.tmpdir")).resolve("chaos-queue-failed-" + seed + ".jsonl"),
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
        assertTrue(summary.ok(), "seed " + seed + ": " + summary.result().violations());
        var stats = summary.result().stats();
        assertTrue(summary.okProduces() > 50, "producers made hardly any progress: " + summary.okProduces());
        assertTrue(stats.getOrDefault("queue.deliveries", 0L) > 50, "the queue workers made hardly any progress: " + stats);
        assertTrue(stats.getOrDefault("queue.acks.accepted", 0L) > 30, "few acks accepted: " + stats);
        assertTrue(stats.getOrDefault("queue.redeliveries", 0L) > 0, "abandoned and failed messages must have been redelivered");
        assertTrue(stats.getOrDefault("queue.dead", 0L) > 0, "poison messages must have been dead-lettered before the fix");
        assertTrue(stats.getOrDefault("nemesis.total", 0L) >= 3, "the nemesis injected too few faults");
        assertTrue(Checker.check(History.read(history)).ok(), "the saved history must check the same way offline");
    }

    @Test
    void seedOneTwentySeconds(@TempDir Path dir) throws Exception {
        run(dir, 31_01, 20_000);
    }

    @Test
    void seedTwoTwentySeconds(@TempDir Path dir) throws Exception {
        run(dir, 31_02, 20_000);
    }

    @Test
    void seedThreeTwentySeconds(@TempDir Path dir) throws Exception {
        run(dir, 31_03, 20_000);
    }

    @Test
    @Tag("slow")
    void twoMinutesSeedA(@TempDir Path dir) throws Exception {
        run(dir, 8_101, 120_000);
    }

    @Test
    @Tag("slow")
    void twoMinutesSeedB(@TempDir Path dir) throws Exception {
        run(dir, 8_102, 120_000);
    }
}
