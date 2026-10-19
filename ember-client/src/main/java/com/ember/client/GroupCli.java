package com.ember.client;

import com.ember.protocol.DescribeGroupResponse;
import com.ember.protocol.FindCoordinatorResponse;
import com.ember.protocol.PartitionOffset;
import com.ember.protocol.PartitionRef;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

/** The group commands of the CLI: consume --group, group describe, group offsets. */
final class GroupCli {

    private GroupCli() {
    }

    static void consume(ClientConfig cfg, Flags flags, PrintStream out, BooleanSupplier shouldStop) throws IOException {
        String group = flags.require("group");
        List<String> topics = List.of(flags.require("topic").split(","));
        boolean follow = flags.has("follow");
        long max = flags.getLong("max", Long.MAX_VALUE);
        GroupConsumerConfig.OffsetReset reset = switch (flags.get("from", "earliest")) {
            case "earliest" -> GroupConsumerConfig.OffsetReset.EARLIEST;
            case "latest" -> GroupConsumerConfig.OffsetReset.LATEST;
            default -> throw new IllegalArgumentException("--from is earliest or latest");
        };
        int session = flags.getInt("session-ms", 10_000);
        GroupConsumerConfig config = GroupConsumerConfig.of(cfg.bootstrap(), group)
                .withClient(cfg)
                .withClientId(flags.get("client-id", "ember-cli"))
                .withSession(session, Math.max(1, session / 3))
                .withOffsetReset(reset)
                .withAutoCommit(true, flags.getInt("commit-interval-ms", 5_000));
        boolean printTime = flags.has("print-time");
        long count = 0;
        try (EmberGroupConsumer consumer = new EmberGroupConsumer(config)) {
            consumer.subscribe(topics.toArray(new String[0]));
            while (count < max && !shouldStop.getAsBoolean()) {
                List<GroupRecord> batch = consumer.poll(Duration.ofMillis(follow ? 500 : 1500));
                Map<PartitionRef, Long> firstUnprinted = new HashMap<>();
                for (GroupRecord r : batch) {
                    if (count >= max) {
                        firstUnprinted.putIfAbsent(new PartitionRef(r.topic(), r.partition()), r.offset());
                        continue;
                    }
                    out.println((printTime ? "ts=" + System.currentTimeMillis() + " " : "") + "topic=" + r.topic() + " partition=" + r.partition() + " offset=" + r.offset()
                            + " key=" + text(r.key()) + " value=" + text(r.value()));
                    count++;
                }
                // process, THEN commit: records we fetched but did not print must not be committed, so rewind to them
                firstUnprinted.forEach((p, offset) -> consumer.seek(p.topic(), p.partition(), offset));
                out.flush();
                if (batch.isEmpty() && !follow) {
                    break;                                     // caught up
                }
            }
        }                                                      // close(): commit, leave
        out.println("(" + count + " records)");
    }

    static void run(String sub, ClientConfig cfg, Flags flags, PrintStream out) throws IOException {
        String group = flags.require("group");
        if (sub.equals("watch")) {
            watch(cfg.withRetries(1, 20, 50), flags, group, out);              // an observer must not hide an outage behind long retries
            return;
        }
        try (ClusterClient client = new ClusterClient(cfg)) {
            switch (sub) {
                case "describe" -> describe(client, group, out);
                case "commit-bench" -> commitBench(cfg, flags, group, out);
                case "commit-probe" -> commitProbe(cfg, flags, group, out);
                case "offsets" -> offsets(client, group, out);
                default -> throw new IllegalArgumentException("group subcommands: describe, offsets");
            }
        }
    }

    private static void offsets(ClusterClient client, String group, PrintStream out) throws IOException {
        DescribeGroupResponse d = client.describeGroup(group);
        if (d.state() == DescribeGroupResponse.UNKNOWN) {
            out.println("group " + group + " does not exist");
            return;
        }
        out.println("TOPIC  PARTITION  COMMITTED");
        for (PartitionOffset o : d.offsets()) {
            out.println(o.topic() + "  " + o.partition() + "  " + o.offset());
        }
    }

    private static void describe(ClusterClient client, String group, PrintStream out) throws IOException {
        DescribeGroupResponse d = client.describeGroup(group);
        if (d.state() == DescribeGroupResponse.UNKNOWN) {
            out.println("group " + group + " does not exist");
            return;
        }
        FindCoordinatorResponse coordinator = client.findCoordinator(group);
        out.println("coordinator: node " + coordinator.nodeId() + " " + coordinator.host() + ":" + coordinator.port());
        out.println("group " + group + "  generation " + d.generation() + "  state " + (d.state() == DescribeGroupResponse.STABLE ? "STABLE" : "EMPTY")
                + "  members " + d.members().size());
        Map<PartitionRef, String> owner = new TreeMap<>();
        Set<String> topics = new TreeSet<>();
        for (DescribeGroupResponse.Member m : d.members()) {
            out.println("  member " + m.memberId() + "  client " + m.clientId() + "  session " + m.sessionTimeoutMs() + "ms  topics "
                    + String.join(",", m.topics()) + "  assigned "
                    + (m.assignment().isEmpty() ? "(none)" : m.assignment().stream().map(p -> p.topic() + "-" + p.partition()).collect(Collectors.joining(","))));
            m.assignment().forEach(p -> owner.put(p, m.memberId()));
            topics.addAll(m.topics());
        }
        Map<PartitionRef, Long> committed = new TreeMap<>();
        for (PartitionOffset o : d.offsets()) {
            committed.put(new PartitionRef(o.topic(), o.partition()), o.offset());
            topics.add(o.topic());
        }
        Map<PartitionRef, PartitionView> views = new TreeMap<>();
        for (String topic : topics) {
            List<PartitionView> parts = client.describe(topic);
            if (parts != null) {
                for (PartitionView v : parts) {
                    views.put(new PartitionRef(topic, v.partition()), v);
                }
            }
        }
        if (views.isEmpty()) {
            out.println("(no partitions to show)");
            return;
        }
        out.println();
        String format = "%-16s %9s %10s %9s %7s  %s%n";
        out.printf(Locale.ROOT, format, "TOPIC", "PARTITION", "COMMITTED", "LOG END", "LAG", "OWNER");
        long totalLag = 0;
        for (Map.Entry<PartitionRef, PartitionView> e : views.entrySet()) {
            Long c = committed.get(e.getKey());
            long base = c != null ? c : e.getValue().logStartOffset();
            long lag = Math.max(0, e.getValue().logEndOffset() - base);
            totalLag += lag;
            out.printf(Locale.ROOT, format, e.getKey().topic(), e.getKey().partition(), c == null ? "-" : c.toString(),
                    e.getValue().logEndOffset(), lag, owner.getOrDefault(e.getKey(), "(unowned)"));
        }
        out.println("total lag: " + totalLag);
    }

    /**
     * Steady-state read throughput of the manual consumer versus a group consumer (with or without auto-commit) over the
     * records that are already in the topic. The clock starts when the FIRST batch has arrived, so the one-off cost of
     * joining and fetching offsets is excluded (and printed separately).
     */
    static void consumeBench(ClientConfig cfg, Flags flags, PrintStream out) throws IOException {
        String topic = flags.require("topic");
        long expect = flags.getLong("expect", 0);
        if (expect < 1) {
            throw new IllegalArgumentException("--expect=N (how many records are in the topic) is required");
        }
        String mode = flags.get("mode", "manual");
        long total = 0;
        long measured = 0;
        long startNanos = 0;
        long beginNanos = System.nanoTime();
        long firstBatchMs = 0;
        long endNanos = 0;                       // taken BEFORE close(): closing commits and leaves (two Raft writes) and is not read time
        long deadline = System.nanoTime() + 300_000_000_000L;
        if (mode.equals("manual")) {
            try (EmberConsumer consumer = new EmberConsumer(cfg)) {
                consumer.assignAll(topic, EmberConsumer.StartPosition.BEGINNING);
                while (total < expect && System.nanoTime() < deadline) {
                    List<ConsumedRecord> batch = consumer.poll(1024 * 1024);
                    if (batch.isEmpty()) {
                        continue;
                    }
                    if (startNanos == 0) {
                        startNanos = System.nanoTime();
                        firstBatchMs = (startNanos - beginNanos) / 1_000_000;
                    } else {
                        measured += batch.size();
                    }
                    total += batch.size();
                }
                endNanos = System.nanoTime();
            }
        } else if (mode.equals("group")) {
            boolean autoCommit = !flags.get("auto-commit", "true").equals("false");
            GroupConsumerConfig config = GroupConsumerConfig.of(cfg.bootstrap(), "bench-" + System.nanoTime()).withClient(cfg)
                    .withSession(10_000, 3_000).withAutoCommit(autoCommit, flags.getInt("commit-interval-ms", 5_000));
            try (EmberGroupConsumer consumer = new EmberGroupConsumer(config)) {
                consumer.subscribe(topic);
                while (total < expect && System.nanoTime() < deadline) {
                    List<GroupRecord> batch = consumer.poll(Duration.ofMillis(500));
                    if (batch.isEmpty()) {
                        continue;
                    }
                    if (startNanos == 0) {
                        startNanos = System.nanoTime();
                        firstBatchMs = (startNanos - beginNanos) / 1_000_000;
                    } else {
                        measured += batch.size();
                    }
                    total += batch.size();
                }
                endNanos = System.nanoTime();
            }
        } else {
            throw new IllegalArgumentException("--mode is manual or group");
        }
        double seconds = Math.max(1e-9, (endNanos - startNanos) / 1e9);
        out.printf(Locale.ROOT, "consume-bench mode=%s records=%d first-batch-after=%d ms steady-state=%d records in %.2f s = %,.0f records/s%n",
                mode + (mode.equals("group") ? (flags.get("auto-commit", "true").equals("false") ? " (no commit)" : " (auto-commit)") : ""),
                total, firstBatchMs, measured, seconds, measured / seconds);
        if (total < expect) {
            throw new IOException("only " + total + " of " + expect + " records arrived");
        }
    }

    /**
     * Prints a line, with a millisecond timestamp, every time the group's generation, member count, owned partitions or
     * total committed offset changes (or the coordinator becomes unreachable). Lets a script time a rebalance precisely.
     */
    static void watch(ClientConfig cfg, Flags flags, String group, PrintStream out) throws IOException {
        long forMs = flags.getLong("for-ms", 60_000);
        long intervalMs = Math.max(10, flags.getLong("interval-ms", 50));
        String last = null;
        long end = System.nanoTime() + forMs * 1_000_000L;
        try (ClusterClient client = new ClusterClient(cfg)) {
            while (System.nanoTime() < end) {
                String line;
                try {
                    DescribeGroupResponse d = client.describeGroup(group);
                    int owned = 0;
                    for (DescribeGroupResponse.Member m : d.members()) {
                        owned += m.assignment().size();
                    }
                    long committed = 0;
                    for (PartitionOffset o : d.offsets()) {
                        committed += o.offset();
                    }
                    line = "generation=" + d.generation() + " members=" + d.members().size() + " owned=" + owned + " committed=" + committed;
                } catch (IOException | RuntimeException e) {
                    line = "unavailable";
                }
                if (!line.equals(last)) {
                    out.println("ts=" + System.currentTimeMillis() + " " + line);
                    out.flush();
                    last = line;
                }
                java.util.concurrent.locks.LockSupport.parkNanos(intervalMs * 1_000_000L);
            }
        }
    }

    /**
     * Commits (with an ever-increasing offset) every few milliseconds and prints each outcome with a timestamp. Kill the
     * coordinator broker meanwhile: the gap between the kill and the next "ok" is the commit unavailability.
     */
    static void commitProbe(ClientConfig cfg, Flags flags, String group, PrintStream out) throws IOException {
        String topic = flags.require("topic");
        long forMs = flags.getLong("for-ms", 20_000);
        long intervalMs = flags.getLong("interval-ms", 20);
        GroupConsumerConfig config = GroupConsumerConfig.of(cfg.bootstrap(), group).withClient(cfg)
                .withSession(10_000, 3_000).withAutoCommit(false, 5_000).withClientId("commit-probe");
        try (EmberGroupConsumer consumer = new EmberGroupConsumer(config)) {
            consumer.subscribe(topic);
            long deadline = System.nanoTime() + 20_000_000_000L;
            while (consumer.assignment().isEmpty() && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200));
            }
            if (consumer.assignment().isEmpty()) {
                throw new IOException("no partition was assigned (does topic " + topic + " exist?)");
            }
            PartitionRef p = consumer.assignment().iterator().next();
            out.println("ts=" + System.currentTimeMillis() + " ready");
            out.flush();
            long end = System.nanoTime() + forMs * 1_000_000L;
            for (long i = 0; System.nanoTime() < end; i++) {
                consumer.seek(p.topic(), p.partition(), 3_000_000L + i);
                long t0 = System.currentTimeMillis();
                try {
                    consumer.commitSync();
                    out.println("ts=" + System.currentTimeMillis() + " ok took=" + (System.currentTimeMillis() - t0) + "ms");
                } catch (RuntimeException | IOException e) {
                    out.println("ts=" + System.currentTimeMillis() + " fail " + e.getClass().getSimpleName() + " " + e.getMessage());
                }
                out.flush();
                java.util.concurrent.locks.LockSupport.parkNanos(intervalMs * 1_000_000L);
            }
        }
    }

    /** OFFSET_COMMIT latency as the application sees it: one synchronous commit at a time, each with a different offset. */
    static void commitBench(ClientConfig cfg, Flags flags, String group, PrintStream out) throws IOException {
        String topic = flags.require("topic");
        int count = flags.getInt("count", 2000);
        GroupConsumerConfig config = GroupConsumerConfig.of(cfg.bootstrap(), group).withClient(cfg)
                .withSession(10_000, 3_000).withAutoCommit(false, 5_000).withClientId("commit-bench");
        try (EmberGroupConsumer consumer = new EmberGroupConsumer(config)) {
            consumer.subscribe(topic);
            long deadline = System.nanoTime() + 20_000_000_000L;
            while (consumer.assignment().isEmpty() && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200));
            }
            if (consumer.assignment().isEmpty()) {
                throw new IOException("no partition was assigned (does topic " + topic + " exist?)");
            }
            PartitionRef p = consumer.assignment().iterator().next();
            for (int i = 0; i < 50; i++) {                          // warm-up: connections, JIT
                consumer.seek(p.topic(), p.partition(), 1_000_000L + i);
                consumer.commitSync();
            }
            long[] nanos = new long[count];
            long begin = System.nanoTime();
            for (int i = 0; i < count; i++) {
                consumer.seek(p.topic(), p.partition(), 2_000_000L + i);
                long t0 = System.nanoTime();
                consumer.commitSync();
                nanos[i] = System.nanoTime() - t0;
            }
            double seconds = (System.nanoTime() - begin) / 1e9;
            Arrays.sort(nanos);
            out.printf(Locale.ROOT, "commit-bench: %d synchronous OFFSET_COMMITs on %s-%d: p50 %.2f ms  p90 %.2f ms  p99 %.2f ms  max %.2f ms  (%,.0f commits/s)%n",
                    count, p.topic(), p.partition(), nanos[count / 2] / 1e6, nanos[(int) (count * 0.90)] / 1e6,
                    nanos[(int) (count * 0.99)] / 1e6, nanos[count - 1] / 1e6, count / seconds);
        }
    }

    private static String text(byte[] bytes) {
        return bytes == null ? "null" : new String(bytes, StandardCharsets.UTF_8);
    }
}
