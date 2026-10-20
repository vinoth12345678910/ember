package com.ember.chaos;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.locks.LockSupport;

/**
 * Scrapes /metrics of every broker at a fixed interval and writes one CSV row per broker per sample: the raw material of the soak
 * analysis (heap after GC, threads, file descriptors, direct buffers, log sizes, consumer-group lag, pending proposals).
 */
public final class MetricsSampler implements AutoCloseable {

    public static final String HEADER = "elapsed_s,node,uptime_s,heap_used,heap_after_gc,threads,open_fds,direct_buffers,log_bytes,group_lag,pending_proposals";

    public record Row(double elapsedSeconds, int node, double uptimeSeconds, double heapUsed, double heapAfterGc, double threads, double openFds, double directBuffers,
                      double logBytes, double groupLag, double pendingProposals) {
    }

    private final List<String> admins;
    private final long intervalMs;
    private final Path csv;
    private final List<Row> rows = new ArrayList<>();
    private volatile boolean stop;
    private Thread thread;

    public MetricsSampler(List<String> adminAddresses, long intervalMs, Path csv) {
        this.admins = List.copyOf(adminAddresses);
        this.intervalMs = intervalMs;
        this.csv = csv;
    }

    public void start() {
        long origin = System.nanoTime();
        thread = new Thread(() -> {
            while (!stop) {
                double elapsed = (System.nanoTime() - origin) / 1e9;
                for (int i = 0; i < admins.size(); i++) {
                    try {
                        Row row = sample(elapsed, i + 1, AdminClient.get(admins.get(i), "/metrics").body());
                        synchronized (rows) {
                            rows.add(row);
                        }
                    } catch (Exception e) {
                        // the broker is down or paused right now: a missing sample is information, not an error
                    }
                }
                LockSupport.parkNanos(intervalMs * 1_000_000L);
            }
        }, "metrics-sampler");
        thread.setDaemon(true);
        thread.start();
    }

    /** Parses one /metrics body. Package-private for tests. */
    static Row sample(double elapsed, int node, String body) {
        double uptime = Double.NaN, heapUsed = Double.NaN, afterGc = Double.NaN, threads = Double.NaN, fds = Double.NaN, direct = Double.NaN;
        double logBytes = 0, lag = 0, pending = 0;
        for (String line : body.split("\n")) {
            if (line.isEmpty() || line.charAt(0) == '#') continue;
            int space = line.lastIndexOf(' ');
            String name = line.substring(0, space);
            double value = Double.parseDouble(line.substring(space + 1));
            String metric = name.contains("{") ? name.substring(0, name.indexOf('{')) : name;
            switch (metric) {
                case "ember_process_uptime_seconds" -> uptime = value;
                case "ember_jvm_heap_used_bytes" -> heapUsed = value;
                case "ember_jvm_heap_after_gc_bytes" -> afterGc = value;
                case "ember_jvm_threads" -> threads = value;
                case "ember_process_open_fds" -> fds = value;
                case "ember_jvm_direct_buffer_used_bytes" -> direct = value;
                case "ember_log_size_bytes" -> logBytes += value;
                case "ember_group_lag" -> lag += value;
                case "ember_raft_pending_proposals" -> pending += value;
                default -> {
                }
            }
        }
        return new Row(elapsed, node, uptime, heapUsed, afterGc, threads, fds, direct, logBytes, lag, pending);
    }

    public List<Row> rows() {
        synchronized (rows) {
            return List.copyOf(rows);
        }
    }

    @Override
    public void close() throws IOException {
        stop = true;
        if (thread != null) thread.interrupt();
        StringBuilder sb = new StringBuilder(HEADER).append('\n');
        for (Row r : rows()) {
            sb.append(String.format(Locale.ROOT, "%.1f,%d,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f%n", r.elapsedSeconds(), r.node(), r.uptimeSeconds(), r.heapUsed(), r.heapAfterGc(),
                    r.threads(), r.openFds(), r.directBuffers(), r.logBytes(), r.groupLag(), r.pendingProposals()));
        }
        if (csv != null) Files.writeString(csv, sb, StandardCharsets.UTF_8);
    }
}
