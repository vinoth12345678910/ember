package com.ember.client;

import com.ember.protocol.QueueDeadListResponse;
import com.ember.protocol.QueueDescribeResponse;
import com.ember.protocol.QueueMessage;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Command-line access to ack queues: create a subscription, look at its progress, and measure claim+ack throughput. */
final class QueueCli {

    private QueueCli() {
    }

    static void run(String command, ClientConfig cfg, Flags flags, PrintStream out) throws IOException {
        String subscription = flags.get("subscription", null);
        if (subscription == null) {
            throw new IllegalArgumentException(command + " needs --subscription=NAME");
        }
        switch (command) {
            case "queue-create" -> {
                String topic = flags.get("topic", null);
                if (topic == null) {
                    throw new IllegalArgumentException("queue-create needs --topic=NAME");
                }
                int partitions = QueueConsumer.createSubscription(cfg, subscription, topic, Boolean.parseBoolean(flags.get("from-start", "true")),
                        flags.getInt("lease-ms", 0), flags.getInt("max-attempts", 0), flags.getInt("backoff-ms", 0), flags.getInt("backoff-max-ms", 0));
                out.println("subscription " + subscription + " created on " + topic + " (" + partitions + " partitions)");
            }
            case "queue-describe" -> {
                try (QueueConsumer q = new QueueConsumer(cfg, subscription, "cli")) {
                    QueueDescribeResponse d = q.describe();
                    out.println("subscription " + subscription + " on " + d.topic() + ", lease " + d.leaseMs() + " ms, dead after " + d.maxAttempts()
                            + " failed attempts, retry backoff " + d.backoffBaseMs() + ".." + d.backoffMaxMs() + " ms");
                    out.println("partition  floor  next  logEnd  inFlight  redeliver  doneAbove  dead");
                    for (QueueDescribeResponse.PartitionState p : d.partitions()) {
                        out.printf(Locale.ROOT, "%9d  %5d  %4d  %6d  %8d  %9d  %9d  %4d%n", p.partition(), p.floor(), p.next(), p.logEnd(), p.inFlight(), p.redeliver(), p.doneAbove(), p.dead());
                    }
                }
            }
            case "queue-bench" -> bench(cfg, flags, subscription, out);
            case "queue-dead", "queue-dead-retry", "queue-dead-discard" -> dead(command, cfg, flags, subscription, out);
            default -> throw new IllegalArgumentException("unknown command " + command);
        }
    }

    /** queue-dead lists; queue-dead-retry / queue-dead-discard act on --partition=P --offset=N, or on every dead message with --all. */
    private static void dead(String command, ClientConfig cfg, Flags flags, String subscription, PrintStream out) throws IOException {
        try (QueueConsumer q = new QueueConsumer(cfg, subscription, "cli")) {
            QueueDeadListResponse dead = q.deadLetters(flags.getInt("max", 100));
            if (command.equals("queue-dead")) {
                out.println(dead.total() + " dead-lettered message(s)" + (dead.total() > dead.items().size() ? ", showing " + dead.items().size() : ""));
                for (QueueDeadListResponse.DeadMessage m : dead.items()) {
                    out.printf(Locale.ROOT, "partition %d offset %d: %d failed attempts, reason: %s%n    key=%s value=%s%n", m.partition(), m.offset(), m.failures(),
                            m.reason(), m.key() == null ? "null" : new String(m.key(), java.nio.charset.StandardCharsets.UTF_8),
                            new String(m.value(), java.nio.charset.StandardCharsets.UTF_8));
                }
                return;
            }
            List<QueueDeadListResponse.DeadMessage> chosen = new ArrayList<>();
            for (QueueDeadListResponse.DeadMessage m : dead.items()) {
                if (flags.has("all") || (flags.has("partition") && flags.has("offset")
                        && m.partition() == flags.getInt("partition", -1) && m.offset() == Long.parseLong(flags.get("offset", "-1")))) {
                    chosen.add(m);
                }
            }
            if (!flags.has("all") && !(flags.has("partition") && flags.has("offset"))) {
                throw new IllegalArgumentException(command + " needs --all or --partition=P --offset=N");
            }
            var r = command.equals("queue-dead-retry") ? q.retryDead(chosen) : q.discardDead(chosen);
            out.println((command.equals("queue-dead-retry") ? "retried " : "discarded ") + r.settled() + " message(s), " + r.rejected() + " not found");
        }
    }

    private static String summary(LatencyHistogram h) {
        return String.format(Locale.ROOT, "p50 %.1f ms  p99 %.1f ms  max %.1f ms  (%d calls)", h.percentile(0.5) / 1000.0, h.percentile(0.99) / 1000.0, h.max() / 1000.0, h.count());
    }

    /** N workers claim and ack in a loop until `records` messages are acked (or nothing is left). Prints messages/s and the claim/ack latency. */
    private static void bench(ClientConfig cfg, Flags flags, String subscription, PrintStream out) throws IOException {
        int workers = flags.getInt("workers", 4);
        int claim = flags.getInt("claim", 100);
        int records = flags.getInt("records", Integer.MAX_VALUE);
        int holdMs = flags.getInt("hold-ms", 0);
        int poisonEvery = flags.getInt("poison-every", 0);                  // every Nth message (by offset) always fails, to show it does not block the rest
        AtomicLong acked = new AtomicLong();
        AtomicLong claimed = new AtomicLong();
        AtomicLong redelivered = new AtomicLong();
        AtomicLong rejected = new AtomicLong();
        AtomicLong nacked = new AtomicLong();
        LatencyHistogram claimLatency = new LatencyHistogram();
        LatencyHistogram ackLatency = new LatencyHistogram();
        long startNanos = System.nanoTime();
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        List<Future<?>> futures = new ArrayList<>();
        for (int w = 0; w < workers; w++) {
            String id = "bench-" + w;
            futures.add(pool.submit(() -> {
                int emptyInARow = 0;
                try (QueueConsumer q = new QueueConsumer(cfg, subscription, id)) {
                    while (acked.get() < records && emptyInARow < 50) {
                        long t0 = System.nanoTime();
                        List<QueueMessage> got = q.claim(claim);
                        synchronized (claimLatency) {
                            claimLatency.record((System.nanoTime() - t0) / 1_000);
                        }
                        if (got.isEmpty()) {
                            emptyInARow++;
                            Thread.sleep(20);
                            continue;
                        }
                        emptyInARow = 0;
                        claimed.addAndGet(got.size());
                        got.stream().filter(m -> m.attempt() > 1).forEach(m -> redelivered.incrementAndGet());
                        if (holdMs > 0) {
                            Thread.sleep(holdMs);
                        }
                        List<QueueMessage> good = got;
                        if (poisonEvery > 0) {
                            List<QueueMessage> bad = new ArrayList<>();
                            good = new ArrayList<>();
                            for (QueueMessage m : got) {
                                (m.offset() % poisonEvery == 0 ? bad : good).add(m);
                            }
                            if (!bad.isEmpty()) {
                                nacked.addAndGet(q.nack(bad, "simulated poison message").settled());
                            }
                        }
                        long t1 = System.nanoTime();
                        var r = q.ack(good);
                        synchronized (ackLatency) {
                            ackLatency.record((System.nanoTime() - t1) / 1_000);
                        }
                        acked.addAndGet(r.settled());
                        rejected.addAndGet(r.rejected());
                    }
                }
                return null;
            }));
        }
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                throw new IOException("worker failed: " + e.getCause(), e.getCause());
            }
        }
        pool.shutdown();
        try {
            pool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        double seconds = (System.nanoTime() - startNanos) / 1e9;
        out.printf(Locale.ROOT, "queue-bench: %d workers, claim up to %d, hold %d ms%n", workers, claim, holdMs);
        out.printf(Locale.ROOT, "acked %,d messages in %.2f s = %,.0f messages/s (claimed %,d, redelivered %,d, nacked %,d, ack rejected %,d)%n",
                acked.get(), seconds, acked.get() / seconds, claimed.get(), redelivered.get(), nacked.get(), rejected.get());
        out.println("claim latency: " + summary(claimLatency));
        out.println("ack latency:   " + summary(ackLatency));
    }
}
