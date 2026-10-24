package com.ember.client;

import com.ember.protocol.ErrorCode;
import com.ember.protocol.ClusterMetadataResponse;
import com.ember.protocol.ProducerRecord;
import com.ember.storage.LogRecord;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
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
import java.util.function.BooleanSupplier;

public final class EmberCli {

    private static final String USAGE = """
            usage: java -jar ember-client-all.jar <command> [--host=localhost] [--port=9099] [flags]
              (all commands: --bootstrap=h1:p1,h2:p2 or --host/--port, --retries=N)
              create-topic --topic=t --partitions=N
              produce      --topic=t [--key=k] --value=v [--partitions=N]   (N: only used if the topic must be auto-created)
              produce      --topic=t --count=N [--keys=100] [--value=order]   (N keyed records: key user-<i mod keys>, value <value>-<i>)
              fetch        --topic=t [--partition=0] [--offset=0] [--max-bytes=65536]
              consume      --topic=t [--from-beginning] [--follow]
              consume      --group=g --topic=a,b [--from=earliest|latest] [--max=N] [--session-ms=10000] [--follow]   (consumer group)
              group describe --group=g      (members, assignment, committed offsets, lag)
              group offsets  --group=g
              group commit-bench --group=g --topic=t [--count=2000]   (OFFSET_COMMIT latency percentiles)
              group watch --group=g [--interval-ms=50] [--for-ms=60000]   (timestamped changes of generation/members/owned/committed)
              group commit-probe --group=g --topic=t [--for-ms=20000]   (commit every 20 ms, print each outcome)
              consume-bench --topic=t --expect=N --mode=manual|group [--auto-commit=true|false]   (steady-state read throughput)
              queue-create   --subscription=s --topic=t [--from-start=true|false] [--lease-ms=30000]   (ack queue: per-message acknowledgement)
              queue-describe --subscription=s
              queue-bench    --subscription=s [--workers=4] [--claim=100] [--records=N (stop after N acked)] [--hold-ms=0 (simulated work per message batch)]
              metadata     [--topic=t]
              bench        [--topic=t] [--partitions=3] [--producers=1] [--records=200000] [--value-bytes=100] [--batch=500]
                           [--warmup-records=0] [--rate=N records/s total (fixed-rate mode; default closed loop)] [--csv=FILE] [--acks-timeout=MS]
                           [--pipeline=N parallel connections per producer: sends a batch's partitions at the same time]
              cluster status  --admin=h1:p1,h2:p2     (nodes, readiness, controller, coordinator, role of every node in every Raft group)
              cluster metrics --admin=h:p [--filter=ember_raft]
              healthcheck  [--admin=localhost:8080]   (exit 0 only when /health/ready is 200)""";

    private EmberCli() {
    }

    public static void main(String[] args) {
        // Ctrl+C / SIGTERM: let a running `consume --group` commit and leave the group before the JVM exits
        java.util.concurrent.atomic.AtomicBoolean signalled = new java.util.concurrent.atomic.AtomicBoolean();
        CountDownLatch finished = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            signalled.set(true);
            try {
                finished.await(10, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "ember-cli-shutdown"));
        int code = run(args, System.out, System.err, signalled::get);
        finished.countDown();
        System.exit(code);
    }

    public static int run(String[] args, PrintStream out, PrintStream err) {
        return run(args, out, err, () -> false);
    }

    /**
     * Returns the process exit code: 0 ok, 1 broker/network error, 2 bad usage.
     * shouldStop ends `consume --follow` (the real CLI never stops it; tests do, so they never wait).
     */
    public static int run(String[] args, PrintStream out, PrintStream err, BooleanSupplier shouldStop) {
        if (args.length == 0) {
            err.println(USAGE);
            return 2;
        }
        try {
            boolean groupCommand = args[0].equals("group") || args[0].equals("cluster");
            if (groupCommand && args.length < 2) {
                throw new IllegalArgumentException(args[0] + " needs a subcommand");
            }
            Flags flags = new Flags(args, groupCommand ? 2 : 1);
            ClientConfig cfg = clientConfig(flags);
            switch (args[0]) {
                case "create-topic" -> createTopic(cfg, flags, out);
                case "produce" -> produce(cfg, flags, out);
                case "fetch" -> fetch(cfg, flags, out);
                case "consume" -> {
                    if (flags.has("group")) {
                        GroupCli.consume(cfg, flags, out, shouldStop);
                    } else {
                        consume(cfg, flags, out, shouldStop);
                    }
                }
                case "group" -> {
                    if (!args[0].equals("group")) {
                        throw new IllegalArgumentException("unreachable");
                    }
                    GroupCli.run(args[1], cfg, flags, out);
                }
                case "consume-bench" -> GroupCli.consumeBench(cfg, flags, out);
                case "queue-create", "queue-describe", "queue-bench" -> QueueCli.run(args[0], cfg, flags, out);
                case "cluster" -> ClusterCli.run(args[1], flags, out);
                case "healthcheck" -> {
                    int code = ClusterCli.healthcheck(flags, out, err);
                    if (code != 0) {
                        return code;
                    }
                }
                case "metadata" -> metadata(cfg, flags, out);
                case "bench" -> {
                    if (!bench(cfg, flags, out)) {
                        return 1;
                    }
                }
                default -> {
                    err.println("unknown command: " + args[0]);
                    err.println(USAGE);
                    return 2;
                }
            }
            return 0;
        } catch (IllegalArgumentException e) {
            err.println("error: " + e.getMessage());
            err.println(USAGE);
            return 2;
        } catch (EmberException e) {
            err.println("error: " + e.getMessage());
            return 1;
        } catch (IOException e) {
            err.println("error: " + e);
            return 1;
        }
    }

    /** --bootstrap=h1:p1,h2:p2 (or --host/--port for one broker), --retries=N */
    private static ClientConfig clientConfig(Flags flags) {
        List<String> bootstrap = new ArrayList<>();
        String list = flags.get("bootstrap", null);
        if (list != null) {
            for (String b : list.split(",")) {
                if (!b.matches(".+:\\d+")) throw new IllegalArgumentException("--bootstrap entries look like host:port, got: " + b);
                bootstrap.add(b.trim());
            }
        } else {
            bootstrap.add(flags.get("host", "localhost") + ":" + flags.getInt("port", 9099));
        }
        ClientConfig base = ClientConfig.defaults().withBootstrap(bootstrap);
        return base.withRetries(flags.getInt("retries", base.retries()), base.retryBackoffMs(), base.maxRetryBackoffMs());
    }

    private static void createTopic(ClientConfig cfg, Flags flags, PrintStream out) throws IOException {
        String topic = flags.require("topic");
        int partitions = Integer.parseInt(flags.require("partitions"));
        try (ClusterClient client = new ClusterClient(cfg)) {
            int created = client.createTopic(topic, partitions);
            out.println("created topic " + topic + " with " + created + " partition(s)");
        }
    }

    private static void produce(ClientConfig base, Flags flags, PrintStream out) throws IOException {
        String topic = flags.require("topic");
        if (flags.getInt("count", 0) > 0) {
            produceMany(base, flags, topic, out);
            return;
        }
        String key = flags.get("key", null);
        String value = flags.require("value");
        ClientConfig config = base.withAutoCreate(true, flags.getInt("partitions", 3));
        try (EmberProducer producer = new EmberProducer(config)) {
            RecordMetadata md = producer.send(topic,
                    key == null ? null : key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
            out.println("partition=" + md.partition() + " offset=" + md.offset());
        }
    }

    /** N keyed records with ONE producer (a demo/test helper: one JVM per record would take minutes). */
    private static void produceMany(ClientConfig base, Flags flags, String topic, PrintStream out) throws IOException {
        int count = flags.getInt("count", 0);
        int keys = Math.max(1, flags.getInt("keys", 100));
        String prefix = flags.get("value", "order");
        int start = flags.getInt("start", 0);
        ClientConfig config = base.withAutoCreate(true, flags.getInt("partitions", 3));
        try (EmberProducer producer = new EmberProducer(config)) {
            for (int i = start; i < start + count; i++) {
                producer.send(topic, ("user-" + (i % keys)).getBytes(StandardCharsets.UTF_8), (prefix + "-" + i).getBytes(StandardCharsets.UTF_8));
            }
        }
        out.println("produced " + count + " records to " + topic);
    }

    private static void fetch(ClientConfig cfg, Flags flags, PrintStream out) throws IOException {
        String topic = flags.require("topic");
        int partition = flags.getInt("partition", 0);
        try (ClusterClient client = new ClusterClient(cfg)) {
            FetchResult r = client.fetch(topic, partition, flags.getLong("offset", 0), flags.getInt("max-bytes", 65536));
            for (LogRecord rec : r.records()) {
                out.println("offset=" + rec.offset() + " timestamp=" + rec.timestamp()
                        + " key=" + text(rec.key()) + " value=" + text(rec.value()));
            }
            out.println("(" + r.records().size() + " records, partition=" + partition + ", logStartOffset=" + r.logStartOffset()
                    + ", highWatermark=" + r.highWatermark() + ", next offset to fetch=" + r.nextFetchOffset() + ")");
        }
    }

    private static String text(byte[] bytes) {
        return bytes == null ? "null" : new String(bytes, StandardCharsets.UTF_8);
    }

    private static void consume(ClientConfig cfg, Flags flags, PrintStream out, BooleanSupplier shouldStop)
            throws IOException {
        String topic = flags.require("topic");
        boolean fromBeginning = flags.has("from-beginning");
        boolean follow = flags.has("follow");
        long count = 0;
        try (EmberConsumer consumer = new EmberConsumer(cfg)) {
            consumer.assignAll(topic, fromBeginning ? EmberConsumer.StartPosition.BEGINNING : EmberConsumer.StartPosition.LATEST);
            while (true) {
                List<ConsumedRecord> batch = consumer.poll(1024 * 1024);
                for (ConsumedRecord c : batch) {
                    out.println("partition=" + c.partition() + " offset=" + c.record().offset()
                            + " key=" + text(c.record().key()) + " value=" + text(c.record().value()));
                }
                count += batch.size();
                if (batch.isEmpty()) {
                    if (!follow || shouldStop.getAsBoolean()) {
                        break;                                  // every partition was caught up
                    }
                    pauseBeforeNextPoll();
                }
            }
        }
        out.println("(" + count + " records)");
    }

    /** The CLI is a human tool, so it may sleep (tests may not: they stop the follow loop through the shouldStop hook). */
    private static void pauseBeforeNextPoll() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void metadata(ClientConfig cfg, Flags flags, PrintStream out) throws IOException {
        String topic = flags.get("topic", null);
        try (ClusterClient client = new ClusterClient(cfg)) {
            ClusterMetadataResponse m = topic == null ? client.clusterMetadata() : client.clusterMetadata(topic);
            out.println("brokers:");
            for (ClusterMetadataResponse.Broker b : m.brokers()) {
                out.println("  node " + b.nodeId() + "  " + b.host() + ":" + b.port() + (b.nodeId() == m.controllerId() ? "  (controller)" : ""));
            }
            if (m.controllerId() < 0) {
                out.println("  (no controller known right now)");
            }
            if (m.topics().isEmpty()) {
                out.println("(no topics)");
                return;
            }
            int width = Math.max(5, m.topics().stream().mapToInt(t -> t.name().length()).max().orElse(5));
            String format = "%-" + width + "s  %9s  %6s  %-9s  %12s  %12s%n";
            out.printf(Locale.ROOT, format, "TOPIC", "PARTITION", "LEADER", "REPLICAS", "START OFFSET", "END OFFSET");
            for (ClusterMetadataResponse.Topic t : m.topics()) {
                for (ClusterMetadataResponse.Partition p : t.partitions()) {
                    String replicas = p.replicas().stream().map(String::valueOf).collect(java.util.stream.Collectors.joining(","));
                    out.printf(Locale.ROOT, format, t.name(), p.partition(), p.leaderId(), replicas, p.logStartOffset(), p.logEndOffset());
                }
            }
        }
    }

    // ---- bench

    /** Returns false if the verification failed. */
    private static boolean bench(ClientConfig cfg, Flags flags, PrintStream out) throws IOException {
        return BenchRunner.run(cfg, flags, out);
    }
}
