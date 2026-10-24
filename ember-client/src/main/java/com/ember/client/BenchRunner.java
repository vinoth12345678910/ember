package com.ember.client;

import com.ember.protocol.ErrorCode;
import com.ember.protocol.ProducerRecord;
import com.ember.storage.LogRecord;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * The `bench` command. Closed loop (default): each producer sends its next request as soon as the previous one is answered, which
 * measures capacity. Open loop (--rate=N records/s in total): sends follow a schedule and latency is measured from the INTENDED
 * send time, which measures what a client arriving on schedule feels (see OpenLoopSchedule).
 */
final class BenchRunner {

    private static final int KEY_BYTES = 10;       // keys look like "key-123456"

    private record Sample(long elapsedMicros, int producer, int records, long latencyMicros) {
    }

    private BenchRunner() {
    }

    /** Returns false if the verification failed. */
    static boolean run(ClientConfig base, Flags flags, PrintStream out) throws IOException {
        String topic = flags.get("topic", "bench-" + System.currentTimeMillis());
        int partitions = flags.getInt("partitions", 3);
        int producers = flags.getInt("producers", 1);
        int records = flags.getInt("records", 200_000);
        int valueBytes = flags.getInt("value-bytes", 100);
        int batchSize = flags.getInt("batch", 500);
        int warmup = flags.getInt("warmup-records", 0);
        double rate = Double.parseDouble(flags.get("rate", "0"));
        String csv = flags.get("csv", null);
        int pipeline = flags.getInt("pipeline", 0);          // 0 = the classic one-partition-at-a-time producer; N = N parallel connections per producer
        if (pipeline < 0) {
            throw new IllegalArgumentException("pipeline must be >= 0");
        }
        if (partitions < 1 || producers < 1 || records < 1 || valueBytes < 0 || batchSize < 1 || warmup < 0 || rate < 0) {
            throw new IllegalArgumentException("partitions, producers, records and batch must be >= 1; value-bytes, warmup-records and rate >= 0");
        }
        ClientConfig cfg = flags.has("acks-timeout") ? base.withTimeouts(5_000, flags.getInt("acks-timeout", 30_000)) : base;
        long recordBytes = LogRecord.HEADER_SIZE + KEY_BYTES + valueBytes;

        // 1. make sure the topic exists, and remember where each partition ends NOW so we read back only our records
        long[] baseline;
        try (ClusterClient admin = new ClusterClient(cfg)) {
            try {
                admin.createTopic(topic, partitions);
            } catch (EmberException e) {
                if (e.errorCode() != ErrorCode.TOPIC_ALREADY_EXISTS) {
                    throw e;
                }
            }
            baseline = endOffsets(admin, topic);
        }
        out.println("bench: topic=" + topic + " partitions=" + baseline.length + " producers=" + producers
                + " records=" + records + " valueBytes=" + valueBytes + " batch=" + batchSize
                + (rate > 0 ? " mode=fixed-rate(" + String.format(Locale.ROOT, "%.0f", rate) + " records/s)" : " mode=closed-loop")
                + (warmup > 0 ? " warmup=" + warmup : "") + (pipeline > 0 ? " pipeline=" + pipeline : ""));

        // 2. P producer threads, each with its own connection, records split evenly
        ExecutorService pool = Executors.newFixedThreadPool(producers);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<LatencyHistogram>> futures = new ArrayList<>();
        ClientConfig config = cfg.withAutoCreate(true, partitions);
        AtomicLong retriedSends = new AtomicLong();
        AtomicLong failedRequests = new AtomicLong();
        AtomicLong failedRecords = new AtomicLong();
        AtomicLong measuredRecords = new AtomicLong();
        AtomicLong firstMeasuredStart = new AtomicLong(Long.MAX_VALUE);
        AtomicLong lastMeasuredEnd = new AtomicLong();
        List<Sample> samples = java.util.Collections.synchronizedList(new ArrayList<>());
        byte[] value = new byte[valueBytes];
        java.util.Arrays.fill(value, (byte) 'x');
        long[] origin = new long[1];
        for (int t = 0; t < producers; t++) {
            final int producerIndex = t;
            final int share = records / producers + (t < records % producers ? 1 : 0);
            final int warmupShare = warmup / producers + (t < warmup % producers ? 1 : 0);
            final long seed = t + 1;
            futures.add(pool.submit(() -> {
                Random random = new Random(seed);
                LatencyHistogram histogram = new LatencyHistogram();
                try (EmberProducer producer = pipeline > 0 ? EmberProducer.pipelined(config, pipeline) : new EmberProducer(config)) {
                    start.await();
                    OpenLoopSchedule schedule = rate > 0 ? new OpenLoopSchedule(origin[0], rate / producers / batchSize) : null;
                    long batchNumber = 0;
                    for (int sent = 0; sent < share; batchNumber++) {
                        int n = Math.min(batchSize, share - sent);
                        List<ProducerRecord> batch = new ArrayList<>(n);
                        for (int i = 0; i < n; i++) {
                            batch.add(new ProducerRecord(String.format("key-%06d", random.nextInt(1_000_000)).getBytes(StandardCharsets.US_ASCII), value));
                        }
                        long intended;
                        if (schedule != null) {
                            long wait = schedule.waitNanos(batchNumber, System.nanoTime());
                            if (wait > 0) {
                                LockSupport.parkNanos(wait);
                            }
                            intended = schedule.intendedNanos(batchNumber);
                        } else {
                            intended = System.nanoTime();
                        }
                        boolean measured = sent >= warmupShare;
                        try {
                            producer.sendBatch(topic, batch);
                            long end = System.nanoTime();
                            if (measured) {
                                long latencyMicros = (end - intended) / 1_000;
                                histogram.record(latencyMicros);
                                measuredRecords.addAndGet(n);
                                firstMeasuredStart.accumulateAndGet(intended, Math::min);
                                lastMeasuredEnd.accumulateAndGet(end, Math::max);
                                if (csv != null) {
                                    samples.add(new Sample((end - origin[0]) / 1_000, producerIndex, n, latencyMicros));
                                }
                            }
                        } catch (EmberException | IOException e) {
                            failedRequests.incrementAndGet();           // counted, not hidden: the verification below will say FAILED
                            failedRecords.addAndGet(n);
                        }
                        sent += n;
                    }
                    retriedSends.addAndGet(producer.retriedRequests());
                }
                return histogram;
            }));
        }
        origin[0] = System.nanoTime();
        long begin = origin[0];
        start.countDown();
        LatencyHistogram all = new LatencyHistogram();
        try {
            for (Future<LatencyHistogram> f : futures) {
                all.merge(f.get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException io) {
                throw io;
            }
            if (e.getCause() instanceof RuntimeException re) {
                throw re;
            }
            throw new IOException(e.getCause());
        } finally {
            pool.shutdownNow();
        }
        long totalNanos = System.nanoTime() - begin;
        report(out, "produce:", records - failedRecords.get(), (records - failedRecords.get()) * recordBytes, totalNanos);
        out.println("retried requests during produce: " + retriedSends.get());
        out.println("failed requests during produce: " + failedRequests.get() + " (" + failedRecords.get() + " records)");
        if (all.count() > 0) {
            long windowNanos = Math.max(1, lastMeasuredEnd.get() - firstMeasuredStart.get());
            out.println(String.format(Locale.ROOT, "measured: %,d records after %,d warm-up records = %,.0f records/s, %.2f MB/s",
                    measuredRecords.get(), warmup, measuredRecords.get() / (windowNanos / 1e9), measuredRecords.get() * recordBytes / 1e6 / (windowNanos / 1e9)));
            out.println(String.format(Locale.ROOT, "latency per request of up to %d records (%s): p50 %s  p90 %s  p99 %s  p99.9 %s  max %s",
                    batchSize, rate > 0 ? "from the INTENDED send time" : "from the actual send time",
                    ms(all.percentile(0.50)), ms(all.percentile(0.90)), ms(all.percentile(0.99)), ms(all.percentile(0.999)), ms(all.max())));
        }
        if (csv != null) {
            writeCsv(Path.of(csv), samples);
            out.println("csv: " + samples.size() + " rows written to " + csv);
        }

        // 3. read back with ONE consumer, from the baseline, and verify
        long expected = records - failedRecords.get();
        long[] expectedNext = baseline.clone();
        long read = 0, readBytes = 0;
        boolean contiguous = true;
        begin = System.nanoTime();
        try (EmberConsumer consumer = new EmberConsumer(cfg)) {
            for (int p = 0; p < baseline.length; p++) {
                consumer.assign(topic, p, baseline[p]);
            }
            while (read < expected) {
                List<ConsumedRecord> batch = consumer.poll(1024 * 1024);
                if (batch.isEmpty()) {
                    break;
                }
                for (ConsumedRecord c : batch) {
                    LogRecord r = c.record();
                    if (r.offset() != expectedNext[c.partition()]) {
                        contiguous = false;
                    }
                    expectedNext[c.partition()] = r.offset() + 1;
                    readBytes += r.sizeInBytes();
                    read++;
                }
            }
        }
        report(out, "read:", read, readBytes, System.nanoTime() - begin);

        boolean ok = read == records && failedRecords.get() == 0 && contiguous;
        out.println(ok ? "verify: OK (" + read + " records read back, every partition contiguous)"
                : "verify: FAILED (produced " + expected + " of " + records + ", read " + read + ", contiguous=" + contiguous + ")");
        return ok;
    }

    private static String ms(long micros) {
        return String.format(Locale.ROOT, "%.2f ms", micros / 1000.0);
    }

    private static void writeCsv(Path file, List<Sample> samples) throws IOException {
        List<Sample> sorted = new ArrayList<>(samples);
        sorted.sort(java.util.Comparator.comparingLong(Sample::elapsedMicros));
        StringBuilder sb = new StringBuilder("elapsed_us,producer,records,latency_us\n");
        for (Sample s : sorted) {
            sb.append(s.elapsedMicros()).append(',').append(s.producer()).append(',').append(s.records()).append(',').append(s.latencyMicros()).append('\n');
        }
        Files.writeString(file, sb);
    }

    private static long[] endOffsets(ClusterClient client, String topic) throws IOException {
        List<PartitionView> partitions = client.describe(topic);
        if (partitions != null) {
            long[] ends = new long[partitions.size()];
            for (PartitionView p : partitions) {
                ends[p.partition()] = p.logEndOffset();
            }
            return ends;
        }
        throw new EmberException(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
    }

    private static void report(PrintStream out, String what, long records, long bytes, long nanos) {
        double seconds = nanos / 1e9;
        out.println(String.format(Locale.ROOT, "%-9s %,d records in %.2f s = %,.0f records/s, %.2f MB/s",
                what, records, seconds, records / seconds, bytes / 1e6 / seconds));
    }
}
