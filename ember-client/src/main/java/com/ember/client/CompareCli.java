package com.ember.client;

import com.ember.protocol.ProducerRecord;
import com.ember.protocol.QueueDescribeResponse;
import com.ember.protocol.QueueMessage;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * compare-bench: the same workload (a topic where every Nth message is poison, fixed work per message) consumed four ways, so the
 * difference between "track a position per partition" (a consumer group, Kafka's model) and "track every message" (an ack queue)
 * is a measurement, not a claim.
 *
 *   1. group, retry in place      a poison message is retried until it works (it never does): the partition is stuck behind it
 *   2. group, skip after 3 tries  the partition keeps moving, the poison messages are simply lost (no dead-letter queue exists)
 *   3. ack queue, same workers    as many workers as partitions: only the poison messages are set aside
 *   4. ack queue, many workers    more workers than partitions: possible because the queue does not tie a worker to a partition
 *
 * The group consumers are tuned the way an operator would (small poll batches, so they poll and heartbeat often); an untuned one that works
 * through hundreds of records between polls can be expired during a rebalance, which makes things much worse for it.
 *
 * A fifth honest option exists in real systems: build retry and dead-letter topics by hand on top of a consumer group. That works but
 * is application code (extra topics, extra consumers, ordering lost on retry); it is not measured here.
 */
final class CompareCli {

    private CompareCli() {
    }

    record Row(String mode, String label, int workers, long elapsedMs, int healthyProcessed, int healthyTotal, int poisonTotal, String poisonOutcome,
               boolean finished) {
    }

    static void run(ClientConfig cfg, Flags flags, PrintStream out) throws Exception {
        int records = flags.getInt("records", 3000);
        int partitions = flags.getInt("partitions", 6);
        int poisonEvery = flags.getInt("poison-every", 100);
        int workMs = flags.getInt("work-ms", 20);
        int capSeconds = flags.getInt("seconds", 30);
        int manyWorkers = flags.getInt("queue-workers", 24);
        if (records < 1 || partitions < 1 || poisonEvery < 2 || workMs < 0 || capSeconds < 1 || manyWorkers < 1) {
            throw new IllegalArgumentException("bad compare-bench flags");
        }
        String topic = "cmp-" + System.currentTimeMillis();
        int poison = 0;
        try (ClusterClient admin = new ClusterClient(cfg)) {
            admin.createTopic(topic, partitions);
        }
        try (EmberProducer p = EmberProducer.pipelined(cfg, 6)) {
            List<ProducerRecord> batch = new ArrayList<>();
            for (int i = 0; i < records; i++) {
                boolean bad = i % poisonEvery == 0;
                if (bad) poison++;
                batch.add(new ProducerRecord(("k" + i).getBytes(StandardCharsets.UTF_8), ((bad ? "poison-" : "ok-") + i).getBytes(StandardCharsets.UTF_8)));
                if (batch.size() == 200 || i == records - 1) {
                    p.sendBatch(topic, batch);
                    batch.clear();
                }
            }
        }
        int healthy = records - poison;
        out.printf(Locale.ROOT, "compare-bench: %d messages on %d partitions, every %dth is poison (%d), %d ms of work per message, at most %d s per mode%n",
                records, partitions, poisonEvery, poison, workMs, capSeconds);

        List<Row> rows = new ArrayList<>();
        rows.add(groupMode(cfg, topic, partitions, healthy, poison, workMs, capSeconds, false));
        rows.add(groupMode(cfg, topic, partitions, healthy, poison, workMs, capSeconds, true));
        rows.add(queueMode(cfg, topic, "ack-same", "ack queue, " + partitions + " workers (= partitions)", partitions, healthy, poison, workMs, capSeconds));
        rows.add(queueMode(cfg, topic, "ack-many", "ack queue, " + manyWorkers + " workers on the same " + partitions + " partitions", manyWorkers, healthy, poison, workMs, capSeconds));

        out.println();
        out.printf(Locale.ROOT, "%-46s %7s %9s %14s  %s%n", "how the topic is consumed", "workers", "time", "healthy done", "poison messages");
        for (Row r : rows) {
            out.printf(Locale.ROOT, "%-46s %7d %8.1fs %7d / %-5d  %s%s%n", r.label(), r.workers(), r.elapsedMs() / 1000.0, r.healthyProcessed(), r.healthyTotal(), r.poisonOutcome(),
                    r.finished() ? "" : "   [stopped at the time cap]");
        }
        String json = flags.get("json", null);
        if (json != null) {
            StringBuilder b = new StringBuilder("{\"records\":" + records + ",\"partitions\":" + partitions + ",\"poisonEvery\":" + poisonEvery + ",\"workMs\":" + workMs
                    + ",\"capSeconds\":" + capSeconds + ",\"finishedAt\":" + System.currentTimeMillis() + ",\"rows\":[");
            for (int i = 0; i < rows.size(); i++) {
                Row r = rows.get(i);
                if (i > 0) b.append(',');
                b.append("{\"mode\":\"").append(r.mode()).append("\",\"label\":\"").append(r.label().replace("\"", "'")).append("\",\"workers\":").append(r.workers())
                        .append(",\"elapsedMs\":").append(r.elapsedMs()).append(",\"healthyProcessed\":").append(r.healthyProcessed())
                        .append(",\"healthyTotal\":").append(r.healthyTotal()).append(",\"poisonTotal\":").append(r.poisonTotal())
                        .append(",\"poisonOutcome\":\"").append(r.poisonOutcome().replace("\"", "'")).append("\",\"finished\":").append(r.finished()).append('}');
            }
            Files.writeString(Path.of(json), b.append("]}").toString());
            out.println("wrote " + json);
        }
    }

    private static void work(int ms) throws InterruptedException {
        if (ms > 0) Thread.sleep(ms);
    }

    // ------------------------------------------------------------------ consumer group: a position per partition

    private static Row groupMode(ClientConfig cfg, String topic, int workers, int healthyTotal, int poisonTotal, int workMs, int capSeconds, boolean skip) throws Exception {
        String group = "cmp-group-" + (skip ? "skip-" : "retry-") + System.nanoTime();
        Set<String> healthySeen = ConcurrentHashMap.newKeySet();         // distinct messages: a rebalance replays uncommitted records, which must not be counted as progress
        Set<String> lostSeen = ConcurrentHashMap.newKeySet();
        AtomicInteger replays = new AtomicInteger();
        AtomicInteger commitFailures = new AtomicInteger();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong startAt = new AtomicLong();
        AtomicInteger assigned = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        for (int w = 0; w < workers; w++) {
            String id = "m" + w;
            pool.submit(() -> {
                GroupConsumerConfig gc = GroupConsumerConfig.of(cfg.bootstrap(), group).withClient(cfg).withClientId(id).withSession(10_000, 3_000).withAutoCommit(false, 5_000)
                        .withMaxPollBytes(512);                       // ~10 records per poll: a tuned baseline. A consumer that works through hundreds of records between polls
                                                                      // is not heartbeating after a rebalance and gets expired, which triggers more rebalances
                try (EmberGroupConsumer consumer = new EmberGroupConsumer(gc)) {
                    consumer.subscribe(topic);
                    boolean counted = false;
                    while (!stop.get()) {
                        try {
                            List<GroupRecord> batch = consumer.poll(Duration.ofMillis(300));
                            if (!counted && !consumer.assignment().isEmpty()) {
                                counted = true;
                                if (assigned.incrementAndGet() == workers) startAt.set(System.nanoTime());      // timing starts once every member owns a partition
                            }
                            for (GroupRecord r : batch) {
                                boolean bad = new String(r.value(), StandardCharsets.UTF_8).startsWith("poison-");
                                int attempts = 0;
                                while (!stop.get()) {
                                    work(workMs);
                                    if (!bad) {
                                        if (!healthySeen.add(r.partition() + ":" + r.offset())) replays.incrementAndGet();
                                        break;
                                    }
                                    attempts++;
                                    if (skip && attempts >= 3) {
                                        lostSeen.add(r.partition() + ":" + r.offset());          // gave up: no dead-letter queue, the message is gone
                                        break;
                                    }
                                    Thread.sleep(100);                                        // a failing message is retried; a position cannot move past it
                                }
                            }
                            if (!batch.isEmpty()) consumer.commitSync();
                        } catch (CommitFailedException e) {
                            commitFailures.incrementAndGet();                                 // the group rebalanced under us: carry on, the uncommitted records are replayed
                        } catch (IOException | RuntimeException e) {
                            if (stop.get()) break;
                            Thread.sleep(100);                                                // a transient error: poll again, which rejoins if needed
                        }
                    }
                } catch (Exception e) {
                    // a failed member simply stops; the totals show it
                }
            });
        }
        long waitStart = System.nanoTime();
        while (startAt.get() == 0 && System.nanoTime() - waitStart < 30_000_000_000L) Thread.sleep(50);
        long t0 = startAt.get() == 0 ? System.nanoTime() : startAt.get();
        boolean finished = false;
        while ((System.nanoTime() - t0) / 1_000_000_000L < capSeconds) {
            if (skip ? healthySeen.size() >= healthyTotal && lostSeen.size() >= poisonTotal : healthySeen.size() >= healthyTotal) {
                finished = true;
                break;
            }
            Thread.sleep(100);
        }
        long elapsed = (System.nanoTime() - t0) / 1_000_000;
        stop.set(true);
        pool.shutdown();
        pool.awaitTermination(15, TimeUnit.SECONDS);
        String replayed = (replays.get() > 0 ? "; " + replays.get() + " healthy messages ran twice (replayed after a rebalance)" : "")
                + (commitFailures.get() > 0 ? "; " + commitFailures.get() + " offset commits rejected by a rebalance" : "");
        String outcome = skip ? lostSeen.size() + " lost (skipped after 3 tries, nowhere to put them)" + replayed
                : poisonTotal + " retried for ever; every message behind them in their partition never ran" + replayed;
        return new Row(skip ? "group-skip" : "group-retry", skip ? "consumer group, skip a poison message after 3 tries" : "consumer group, retry a poison message in place",
                workers, elapsed, healthySeen.size(), healthyTotal, poisonTotal, outcome, finished);
    }

    // ------------------------------------------------------------------ ack queue: every message tracked

    private static Row queueMode(ClientConfig cfg, String topic, String mode, String label, int workers, int healthyTotal, int poisonTotal, int workMs, int capSeconds) throws Exception {
        String sub = "cmp-queue-" + mode + "-" + System.nanoTime();
        QueueConsumer.createSubscription(cfg, sub, topic, true, 30_000, 3, 100, 200);
        AtomicInteger healthyDone = new AtomicInteger();
        AtomicBoolean stop = new AtomicBoolean();
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        long t0 = System.nanoTime();
        for (int w = 0; w < workers; w++) {
            String id = "w" + w;
            pool.submit(() -> {
                try (QueueConsumer q = new QueueConsumer(cfg, sub, id)) {
                    while (!stop.get()) {
                        List<QueueMessage> got = q.claim(10);
                        if (got.isEmpty()) {
                            Thread.sleep(20);
                            continue;
                        }
                        List<QueueMessage> good = new ArrayList<>(), bad = new ArrayList<>();
                        for (QueueMessage m : got) {
                            work(workMs);
                            if (new String(m.value(), StandardCharsets.UTF_8).startsWith("poison-")) bad.add(m);
                            else good.add(m);
                        }
                        if (!bad.isEmpty()) q.nack(bad, "poison message");
                        if (!good.isEmpty()) {
                            q.ack(good);
                            healthyDone.addAndGet(good.size());
                        }
                    }
                } catch (Exception e) {
                    // a failed worker stops; the totals show it
                }
            });
        }
        boolean finished = false;
        int dead = 0;
        try (QueueConsumer observer = new QueueConsumer(cfg, sub, "observer")) {
            while ((System.nanoTime() - t0) / 1_000_000_000L < capSeconds) {
                if (healthyDone.get() >= healthyTotal) {
                    QueueDescribeResponse d = observer.describe();
                    dead = d.partitions().stream().mapToInt(QueueDescribeResponse.PartitionState::dead).sum();
                    int open = d.partitions().stream().mapToInt(p -> p.inFlight() + p.redeliver()).sum();
                    if (dead >= poisonTotal && open == 0) {
                        finished = true;
                        break;
                    }
                }
                Thread.sleep(100);
            }
        }
        long elapsed = (System.nanoTime() - t0) / 1_000_000;
        stop.set(true);
        pool.shutdown();
        pool.awaitTermination(15, TimeUnit.SECONDS);
        return new Row(mode, label, workers, elapsed, healthyDone.get(), healthyTotal, poisonTotal,
                dead + " dead-lettered after 3 tries (kept, listed, retryable by an operator)", finished);
    }
}
