package com.ember.chaos;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Runs a whole chaos experiment: workload + nemesis, heal, drain, final reads, collect replica state, check. */
public final class ChaosRunner {

    /** ratePerSecond: total produce rate (0 = unthrottled). sampleAdmins/samplesCsv: scrape /metrics every 10 s into a CSV (soak runs). */
    public record Config(long seed, long durationMs, int producers, int readers, int consumers, int partitions, Set<Fault> faults,
                         long minGapMs, long maxGapMs, Path historyFile, double ratePerSecond, List<String> sampleAdmins, Path samplesCsv, int queueWorkers, boolean cooperative) {
        public Config(long seed, long durationMs, int producers, int readers, int consumers, int partitions, Set<Fault> faults,
                      long minGapMs, long maxGapMs, Path historyFile, double ratePerSecond, List<String> sampleAdmins, Path samplesCsv) {
            this(seed, durationMs, producers, readers, consumers, partitions, faults, minGapMs, maxGapMs, historyFile, ratePerSecond, sampleAdmins, samplesCsv, 0, false);
        }

        public Config(long seed, long durationMs, int producers, int readers, int consumers, int partitions, Set<Fault> faults,
                      long minGapMs, long maxGapMs, Path historyFile) {
            this(seed, durationMs, producers, readers, consumers, partitions, faults, minGapMs, maxGapMs, historyFile, 0, List.of(), null, 0, false);
        }

        /** The same run with ack-queue workers (and a paced producer so the queue can keep up). */
        public Config withQueue(int workers, double ratePerSecond) {
            return new Config(seed, durationMs, producers, readers, consumers, partitions, faults, minGapMs, maxGapMs, historyFile, ratePerSecond, sampleAdmins, samplesCsv, workers, cooperative);
        }

        /** The same run with cooperative group rebalancing (brokers started with --group-rebalance=cooperative, consumers rejoin incrementally). */
        public Config withCooperative() {
            return new Config(seed, durationMs, producers, readers, consumers, partitions, faults, minGapMs, maxGapMs, historyFile, ratePerSecond, sampleAdmins, samplesCsv, queueWorkers, true);
        }
    }

    public record Summary(long seed, long durationMs, Checker.Result result, List<String> schedule, List<SoakAnalysis.Trend> trends, long okProduces,
                          long unknownProduces, long failedProduces, History history) {

        /** Violations or a leak trend make a run fail. */
        public boolean ok() {
            return result.ok() && SoakAnalysis.allFlat(trends);
        }

        public String table() {
            Map<String, Long> s = result.stats();
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("chaos run  seed=%d  duration=%ds%n", seed, durationMs / 1000));
            sb.append(String.format("  produce operations     : %d acknowledged, %d unknown outcome, %d failed%n", okProduces, unknownProduces, failedProduces));
            sb.append(String.format("  reads / polls / commits: %d / %d / %d%n", s.getOrDefault("reads", 0L), s.getOrDefault("polls", 0L), s.getOrDefault("commits", 0L)));
            sb.append(String.format("  faults injected        : %d", s.getOrDefault("nemesis.total", 0L)));
            s.forEach((k, v) -> {
                if (k.startsWith("nemesis.") && !k.equals("nemesis.total")) sb.append("  ").append(k.substring(8)).append('=').append(v);
            });
            sb.append('\n');
            long deliveries = s.getOrDefault("deliveries", 0L);
            sb.append(String.format("  group deliveries       : %d (%d distinct, %d duplicates = %.2f%%)%n", deliveries, s.getOrDefault("deliveries.distinct", 0L),
                    s.getOrDefault("deliveries.duplicates", 0L), deliveries == 0 ? 0.0 : 100.0 * s.getOrDefault("deliveries.duplicates", 0L) / deliveries));
            if (s.getOrDefault("queue.claims", 0L) > 0) {
                sb.append(String.format("  ack queue              : %d claims delivered %d messages (%d redelivered), %d acks accepted, %d nacks, %d releases, %d dead-lettered, %d rejected as fenced%n",
                        s.getOrDefault("queue.claims", 0L), s.getOrDefault("queue.deliveries", 0L), s.getOrDefault("queue.redeliveries", 0L), s.getOrDefault("queue.acks.accepted", 0L),
                        s.getOrDefault("queue.nacks", 0L), s.getOrDefault("queue.releases", 0L), s.getOrDefault("queue.dead", 0L), s.getOrDefault("queue.rejected", 0L)));
            }
            sb.append(String.format("  replica comparisons    : %d%n", s.getOrDefault("replica.checks", 0L)));
            sb.append(String.format("  VIOLATIONS             : %d%n", result.violations().size()));
            if (!trends.isEmpty()) {
                sb.append("  resource trends (after a 2 minute warm-up):\n").append(SoakAnalysis.table(trends).indent(4));
            }
            for (Violation v : result.violations()) sb.append("    ").append(v).append('\n');
            return sb.toString();
        }
    }

    private ChaosRunner() {
    }

    /** A run writes to its OWN topic and consumer group (unique per run): data left by earlier runs on a reused cluster would look like phantoms. */
    private static String uniqueSuffix() {
        return Long.toString(System.currentTimeMillis(), 36);
    }

    /** Everything a run needs from the environment, in-process or external. */
    public interface Environment {
        List<String> bootstrap();

        FaultDriver driver();

        /** Waits until the cluster serves again after healing. */
        boolean awaitReady(long timeoutMs);

        Collector collector(String topic, int partitions);

        /** Hook after the final collection (in-process: stop the cluster and digest the Raft logs). Default: nothing. */
        default void afterCollection(HistoryRecorder recorder) throws Exception {
        }
    }

    public static Summary run(Config cfg, Environment env) throws Exception {
        HistoryRecorder rec = cfg.historyFile() == null ? new HistoryRecorder() : new HistoryRecorder(cfg.historyFile());
        long started = System.currentTimeMillis();
        String suffix = uniqueSuffix();
        String TOPIC = "chaos-" + suffix;
        String GROUP = "chaos-group-" + suffix;
        Collector collector = env.collector(TOPIC, cfg.partitions());
        List<String> schedule;
        MetricsSampler sampler = cfg.sampleAdmins().isEmpty() ? null : new MetricsSampler(cfg.sampleAdmins(), 10_000, cfg.samplesCsv());
        if (sampler != null) sampler.start();
        try (Workload workload = new Workload(new Workload.Config(TOPIC, cfg.partitions(), cfg.producers(), cfg.readers(), cfg.consumers(), env.bootstrap(), GROUP,
                cfg.ratePerSecond(), cfg.queueWorkers(), "chaos-queue-" + suffix, cfg.cooperative()), rec)) {
            try {
                workload.createTopic();
                workload.start();
                if (cfg.faults().isEmpty()) {                                  // --nemesis=none: a calm run
                    java.util.concurrent.locks.LockSupport.parkNanos(cfg.durationMs() * 1_000_000L);
                    schedule = List.of();
                } else {
                    Nemesis nemesis = new Nemesis(env.driver(), rec, cfg.seed(), cfg.faults(), cfg.consumers() > 0 ? workload : null, cfg.minGapMs(), cfg.maxGapMs());
                    nemesis.runFor(cfg.durationMs());
                    nemesis.healAll();
                    schedule = nemesis.schedule();
                }
                if (!env.awaitReady(120_000)) throw new IllegalStateException("the cluster did not become ready after healing");
                workload.stopProducers();
                if (cfg.consumers() > 0) {
                    long deadline = System.nanoTime() + 90_000_000_000L;
                    while (System.nanoTime() < deadline && !workload.deliveredValues().containsAll(workload.ackedValues())) {
                        java.util.concurrent.locks.LockSupport.parkNanos(100_000_000L);
                    }
                }
                workload.stopConsumers();
                if (cfg.queueWorkers() > 0) {
                    // the queue must settle by itself (leases run out, retries come due), then the operator deploys the fix and retries the dead letters
                    if (!workload.drainQueue(120_000)) System.err.println("warning: the ack queue did not drain within 120 s; the checker will report what is stuck");
                    workload.queueFinal(false);
                    workload.operatorFix();
                    if (!workload.drainQueue(120_000)) System.err.println("warning: the ack queue did not drain after the fix; the checker will report what is stuck");
                    workload.queueFinal(true);
                    workload.stopQueueWorkers();
                }
                workload.stopReaders();
                if (!workload.crashes().isEmpty()) throw new IllegalStateException("a workload thread crashed: " + workload.crashes().get(0), workload.crashes().get(0));
                if (!collector.awaitConverged(60_000)) {
                    System.err.println("warning: the replicas did not converge within 60 s; the checker will report what differs");
                }
                workload.finalReads();
                collector.collect(rec);
                env.afterCollection(rec);
            } finally {
                workload.close();
            }
        }
        History history = rec.snapshot();
        rec.close();
        List<SoakAnalysis.Trend> trends = List.of();
        if (sampler != null) {
            sampler.close();
            trends = SoakAnalysis.analyze(sampler.rows(), 120);
        }
        Checker.Result result = Checker.check(history);
        return new Summary(cfg.seed(), System.currentTimeMillis() - started, result, schedule, trends, result.stats().getOrDefault("produce.ok", 0L),
                result.stats().getOrDefault("produce.info", 0L), result.stats().getOrDefault("produce.fail", 0L), history);
    }

    // ------------------------------------------------------------------ in-process

    public static Summary runInProcess(Config cfg, Path dataDir, int brokers, boolean fsync) throws Exception {
        try (InProcessCluster cluster = cfg.cooperative() ? new InProcessCluster(dataDir, brokers, fsync, "--group-rebalance=cooperative")
                : new InProcessCluster(dataDir, brokers, fsync)) {
            InProcessDriver driver = new InProcessDriver(cluster);
            InProcessCollector[] collectorHolder = new InProcessCollector[1];
            return run(cfg, new Environment() {
                @Override
                public List<String> bootstrap() {
                    return cluster.bootstrap();
                }

                @Override
                public FaultDriver driver() {
                    return driver;
                }

                @Override
                public boolean awaitReady(long timeoutMs) {
                    return cluster.awaitReady(timeoutMs);
                }

                @Override
                public Collector collector(String topic, int partitions) {
                    collectorHolder[0] = new InProcessCollector(cluster, topic, partitions);
                    return collectorHolder[0];
                }

                @Override
                public void afterCollection(HistoryRecorder recorder) {
                    cluster.stopAll();                                   // graceful: files are complete and closed
                    collectorHolder[0].collectRaftLogs(recorder);
                }
            });
        }
    }

    /**
     * Majority loss: with 2 of 3 brokers down NOTHING may be acknowledged, and after healing progress resumes. The ordinary
     * checker then runs over the whole history. Returns the summary; the counts before / during / after are in the arguments.
     */
    public record MajorityLossResult(Summary summary, long acksBefore, long acksDuring, long acksAfter) {
    }

    public static MajorityLossResult runMajorityLoss(Path dataDir, long seed) throws Exception {
        try (InProcessCluster cluster = new InProcessCluster(dataDir, 3, false, "--produce-timeout-ms=1500")) {
            HistoryRecorder rec = new HistoryRecorder();
            String suffix = uniqueSuffix();
            String TOPIC = "chaos-" + suffix;
            Workload workload = new Workload(new Workload.Config(TOPIC, 3, 2, 1, 1, cluster.bootstrap(), "chaos-group-" + suffix), rec);
            InProcessDriver driver = new InProcessDriver(cluster);
            InProcessCollector collector = new InProcessCollector(cluster, TOPIC, 3);
            workload.createTopic();
            workload.start();
            awaitAcks(workload, 30);
            long before = workload.ok();

            rec.nemesis("crash", "broker 2", "start");
            driver.crash(2);
            rec.nemesis("crash", "broker 3", "start");
            driver.crash(3);
            // a produce that was already in flight may still be answered; let the dust settle, then count
            java.util.concurrent.locks.LockSupport.parkNanos(2_500_000_000L);
            long settled = workload.ok();
            java.util.concurrent.locks.LockSupport.parkNanos(3_000_000_000L);
            long during = workload.ok() - settled;

            driver.healAll();
            rec.nemesis("crash", "all", "stop");
            if (!cluster.awaitReady(120_000)) throw new IllegalStateException("not ready after healing");
            long resumeFrom = workload.ok();
            awaitAcks(workload, 30);
            long after = workload.ok() - resumeFrom;

            workload.stopProducers();
            long deadline = System.nanoTime() + 90_000_000_000L;
            while (System.nanoTime() < deadline && !workload.deliveredValues().containsAll(workload.ackedValues())) {
                java.util.concurrent.locks.LockSupport.parkNanos(100_000_000L);
            }
            workload.stopConsumers();
            workload.stopReaders();
            collector.awaitConverged(60_000);
            workload.finalReads();
            collector.collect(rec);
            cluster.stopAll();
            collector.collectRaftLogs(rec);
            workload.close();
            History history = rec.snapshot();
            Checker.Result result = Checker.check(history);
            Summary summary = new Summary(seed, 0, result, List.of("crash 2", "crash 3", "heal"), List.of(), result.stats().getOrDefault("produce.ok", 0L),
                    result.stats().getOrDefault("produce.info", 0L), result.stats().getOrDefault("produce.fail", 0L), history);
            return new MajorityLossResult(summary, before, during, after);
        }
    }

    private static void awaitAcks(Workload workload, long count) {
        long target = workload.ok() + count;
        if (!InProcessCluster.await(90_000, () -> workload.ok() >= target)) {
            throw new IllegalStateException("producers made no progress: " + workload.ok() + " acknowledged, wanted " + target);
        }
    }

    static void writeHistory(History history, Path file) throws IOException {
        history.write(file);
    }
}
