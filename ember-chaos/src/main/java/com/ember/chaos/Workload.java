package com.ember.chaos;

import com.ember.client.ClientConfig;
import com.ember.client.ClusterClient;
import com.ember.client.CommitFailedException;
import com.ember.client.EmberException;
import com.ember.client.EmberGroupConsumer;
import com.ember.client.FetchResult;
import com.ember.client.GroupConsumerConfig;
import com.ember.client.GroupRecord;
import com.ember.protocol.ErrorCode;
import com.ember.protocol.Partitioner;
import com.ember.protocol.PartitionRef;
import com.ember.protocol.ProducerRecord;
import com.ember.storage.LogRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static com.ember.chaos.HistoryRecorder.data;

/**
 * The clients of a chaos run, every operation recorded in the history: producers (unique values "p<id>-<seq>"), readers
 * (fetch near the end of random partitions), group consumers (poll + explicit commits), and the final full read.
 */
public final class Workload implements Nemesis.ClientFaults, AutoCloseable {

    /** ratePerSecond: total produce rate of all producers (0 = as fast as they can go). */
    public record Config(String topic, int partitions, int producers, int readers, int consumers, List<String> bootstrap, String group, double ratePerSecond) {
        public Config(String topic, int partitions, int producers, int readers, int consumers, List<String> bootstrap, String group) {
            this(topic, partitions, producers, readers, consumers, bootstrap, group, 0);
        }
    }

    private static final int GROUP_SESSION_MS = 2000;
    private static final int GROUP_HEARTBEAT_MS = 400;

    private final Config config;
    private final HistoryRecorder rec;
    private final ClientConfig clientConfig;
    private final AtomicBoolean stopProducers = new AtomicBoolean();
    private final AtomicBoolean stopReaders = new AtomicBoolean();
    private final AtomicBoolean stopConsumers = new AtomicBoolean();
    private final Set<String> acked = ConcurrentHashMap.newKeySet();
    private final Set<String> delivered = ConcurrentHashMap.newKeySet();
    private final AtomicLong okCount = new AtomicLong();
    private final AtomicLong infoCount = new AtomicLong();
    private final AtomicLong failCount = new AtomicLong();
    private final List<Thread> producerThreads = new ArrayList<>();
    private final List<Thread> readerThreads = new ArrayList<>();
    private final List<Thread> consumerThreads = new ArrayList<>();
    private final List<EmberGroupConsumer> consumers = Collections.synchronizedList(new ArrayList<>());
    private final List<EmberGroupConsumer> paused = Collections.synchronizedList(new ArrayList<>());
    private final List<Throwable> crashes = Collections.synchronizedList(new ArrayList<>());

    public Workload(Config config, HistoryRecorder rec) {
        this.config = config;
        this.rec = rec;
        this.clientConfig = ClientConfig.defaults().withBootstrap(config.bootstrap()).withRetries(15, 50, 300).withTimeouts(2_000, 9_000);
    }

    public void createTopic() throws IOException {
        try (ClusterClient admin = new ClusterClient(clientConfig)) {
            try {
                admin.createTopic(config.topic(), config.partitions());
            } catch (EmberException e) {
                if (e.errorCode() != ErrorCode.TOPIC_ALREADY_EXISTS) throw e;
            }
        }
    }

    public void start() {
        for (int i = 0; i < config.producers(); i++) {
            String who = "p" + i;
            spawn(producerThreads, who, () -> produceLoop(who));
        }
        for (int i = 0; i < config.readers(); i++) {
            String who = "reader-" + i;
            spawn(readerThreads, who, () -> readLoop(who));
        }
        for (int i = 0; i < config.consumers(); i++) {
            String who = "c" + i;
            spawn(consumerThreads, who, () -> consumeLoop(who));
        }
    }

    private void spawn(List<Thread> into, String name, Runnable body) {
        Thread t = new Thread(() -> {
            try {
                body.run();
            } catch (Throwable e) {
                crashes.add(e);
            }
        }, "chaos-" + name);
        t.setDaemon(true);
        into.add(t);
        t.start();
    }

    public long ok() {
        return okCount.get();
    }

    public long info() {
        return infoCount.get();
    }

    public long fail() {
        return failCount.get();
    }

    public Set<String> ackedValues() {
        return Set.copyOf(acked);
    }

    public Set<String> deliveredValues() {
        return Set.copyOf(delivered);
    }

    public List<Throwable> crashes() {
        return List.copyOf(crashes);
    }

    // ------------------------------------------------------------------ producers

    private void produceLoop(String process) {
        Partitioner partitioner = new Partitioner();
        try (ClusterClient client = new ClusterClient(clientConfig)) {
            long seq = 0;
            com.ember.client.OpenLoopSchedule schedule = config.ratePerSecond() > 0
                    ? new com.ember.client.OpenLoopSchedule(System.nanoTime(), config.ratePerSecond() / Math.max(1, config.producers())) : null;
            while (!stopProducers.get()) {
                if (schedule != null) {
                    LockSupport.parkNanos(schedule.waitNanos(seq, System.nanoTime()));       // a steady stream; if we fall behind we do not skip
                }
                String key = "key-" + (seq % 40);
                String value = process + "-" + seq;
                seq++;
                int partition = partitioner.partition(key.getBytes(StandardCharsets.UTF_8), config.partitions());
                long op = rec.invoke(process, "produce", data("topic", config.topic(), "partition", partition, "key", key, "value", value));
                long retriesBefore = client.retriedRequests();
                try {
                    long offset = client.produce(config.topic(), partition,
                            List.of(new ProducerRecord(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8))));
                    rec.complete(op, process, "produce", Outcome.OK, data("offset", offset, "retried", client.retriedRequests() > retriesBefore));
                    acked.add(value);
                    okCount.incrementAndGet();
                } catch (EmberException e) {
                    // definitely-not-applied ONLY if the broker refused it as invalid on the very first attempt
                    boolean definite = client.retriedRequests() == retriesBefore
                            && (e.errorCode() == ErrorCode.INVALID_REQUEST || e.errorCode() == ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
                    rec.complete(op, process, "produce", definite ? Outcome.FAIL : Outcome.INFO, data("error", e.errorCode().name()));
                    (definite ? failCount : infoCount).incrementAndGet();
                } catch (IOException e) {
                    rec.complete(op, process, "produce", Outcome.INFO, data("error", e.toString()));
                    infoCount.incrementAndGet();
                }
                if (schedule == null) {
                    LockSupport.parkNanos(2_000_000L);
                }
            }
        } catch (IOException ignored) {
            // closing the client
        }
    }

    public void stopProducers() throws InterruptedException {
        stopProducers.set(true);
        for (Thread t : producerThreads) t.join(60_000);
    }

    // ------------------------------------------------------------------ readers

    private void readLoop(String process) {
        Random random = new Random(process.hashCode());
        Map<Integer, Long> lastEnd = new HashMap<>();
        try (ClusterClient client = new ClusterClient(clientConfig.withRetries(3, 50, 200))) {
            while (!stopReaders.get()) {
                int partition = random.nextInt(config.partitions());
                long from = Math.max(0, lastEnd.getOrDefault(partition, 0L) - 40);
                long op = rec.invoke(process, "read", data("topic", config.topic(), "partition", partition, "from", from, "final", false));
                try {
                    FetchResult r = client.fetch(config.topic(), partition, from, 32 * 1024);
                    rec.complete(op, process, "read", Outcome.OK, data("records", rows(r), "start", r.logStartOffset(), "end", r.highWatermark()));
                    lastEnd.put(partition, r.highWatermark());
                } catch (EmberException e) {
                    rec.complete(op, process, "read", Outcome.INFO, data("error", e.errorCode().name()));
                    if (e.errorCode() == ErrorCode.OFFSET_OUT_OF_RANGE) lastEnd.put(partition, 0L);   // read from 0 next: the end it reports is what P6 compares
                } catch (IOException e) {
                    rec.complete(op, process, "read", Outcome.INFO, data("error", e.toString()));
                }
                LockSupport.parkNanos(60_000_000L + random.nextInt(60) * 1_000_000L);
            }
        } catch (IOException ignored) {
            // closing the client
        }
    }

    private static List<List<Object>> rows(FetchResult r) {
        List<List<Object>> rows = new ArrayList<>();
        for (LogRecord rec : r.records()) {
            rows.add(java.util.Arrays.asList(rec.offset(), rec.key() == null ? null : new String(rec.key(), StandardCharsets.UTF_8),
                    new String(rec.value(), StandardCharsets.UTF_8)));
        }
        return rows;
    }

    public void stopReaders() throws InterruptedException {
        stopReaders.set(true);
        for (Thread t : readerThreads) t.join(30_000);
    }

    // ------------------------------------------------------------------ group consumers

    private void consumeLoop(String process) {
        GroupConsumerConfig groupConfig = GroupConsumerConfig.of(config.bootstrap(), config.group()).withClient(clientConfig)
                .withClientId(process).withSession(GROUP_SESSION_MS, GROUP_HEARTBEAT_MS).withAutoCommit(false, 5_000);
        EmberGroupConsumer consumer = new EmberGroupConsumer(groupConfig);
        consumers.add(consumer);
        consumer.subscribe(config.topic());
        long lastCommit = System.nanoTime();
        try {
            while (!stopConsumers.get()) {
                long op = rec.invoke(process, "poll", data("consumer", process, "group", config.group()));
                try {
                    List<GroupRecord> batch = consumer.poll(Duration.ofMillis(100));
                    List<List<Object>> rows = new ArrayList<>();
                    for (GroupRecord r : batch) {
                        String value = new String(r.value(), StandardCharsets.UTF_8);
                        rows.add(List.of(r.topic(), r.partition(), r.offset(), value));
                        delivered.add(value);
                    }
                    rec.complete(op, process, "poll", Outcome.OK, data("records", rows, "generation", consumer.generation(), "joinCount", consumer.joinCount()));
                } catch (EmberException | IOException e) {
                    rec.complete(op, process, "poll", Outcome.INFO, data("records", List.of(), "generation", consumer.generation(), "error", e.toString()));
                    LockSupport.parkNanos(50_000_000L);
                }
                if (System.nanoTime() - lastCommit > 400_000_000L) {
                    lastCommit = System.nanoTime();
                    commit(process, consumer);
                }
            }
            commit(process, consumer);                              // the last commit after the final drain
        } finally {
            try {
                consumer.close();
            } catch (IOException | RuntimeException ignored) {
                // best effort
            }
        }
    }

    private void commit(String process, EmberGroupConsumer consumer) {
        List<List<Object>> offsets = new ArrayList<>();
        for (PartitionRef p : consumer.assignment()) {
            try {
                offsets.add(List.of(p.topic(), p.partition(), consumer.position(p.topic(), p.partition())));
            } catch (IllegalStateException notAssigned) {
                // moved away between the two calls
            }
        }
        if (offsets.isEmpty()) return;
        long generationBefore = consumer.generation();
        long op = rec.invoke(process, "commit", data("group", config.group(), "member", consumer.memberId(), "generation", generationBefore, "offsets", offsets));
        try {
            boolean sent = consumer.commitSync();
            if (sent) {
                rec.complete(op, process, "commit", Outcome.OK, data("generationAfter", consumer.generation()));
            } else {
                // commitSync() had nothing to send (no position moved, or a rebalance is pending): the coordinator was never asked,
                // so this is not a commit at all. Recording it as OK would claim an acknowledgement that never happened.
                rec.complete(op, process, "commit", Outcome.FAIL, data("error", "not sent"));
            }
        } catch (CommitFailedException e) {
            rec.complete(op, process, "commit", Outcome.FAIL, data("error", e.errorCode().name()));          // refused: nothing was stored
        } catch (EmberException | IOException e) {
            rec.complete(op, process, "commit", Outcome.INFO, data("error", e.toString()));
        }
    }

    public void stopConsumers() throws InterruptedException {
        stopConsumers.set(true);
        for (Thread t : consumerThreads) t.join(30_000);
    }

    @Override
    public String pauseSomeConsumer(Random random) {
        synchronized (consumers) {
            if (consumers.isEmpty()) return null;
            EmberGroupConsumer c = consumers.get(random.nextInt(consumers.size()));
            c.pauseHeartbeatsForTesting(true);
            paused.add(c);
            return "consumer " + c.memberId();
        }
    }

    @Override
    public void resumeConsumers() {
        synchronized (paused) {
            for (EmberGroupConsumer c : paused) c.pauseHeartbeatsForTesting(false);
            paused.clear();
        }
    }

    // ------------------------------------------------------------------ final read

    /** Reads every partition completely, from its start to its end, as "final" reads. Run on a healed, quiet cluster. */
    public void finalReads() throws IOException {
        try (ClusterClient client = new ClusterClient(clientConfig.withRetries(40, 100, 500))) {
            for (int partition = 0; partition < config.partitions(); partition++) {
                long from = -1;
                while (true) {
                    long startAt = from < 0 ? 0 : from;
                    long op = rec.invoke("final", "read", data("topic", config.topic(), "partition", partition, "from", startAt, "final", true));
                    FetchResult r;
                    try {
                        r = client.fetch(config.topic(), partition, startAt, 1024 * 1024);
                    } catch (EmberException e) {
                        if (e.errorCode() == ErrorCode.OFFSET_OUT_OF_RANGE && from < 0) {                 // retention moved the start
                            r = client.fetch(config.topic(), partition, client.describe(config.topic()).get(partition).logStartOffset(), 1024 * 1024);
                        } else {
                            rec.complete(op, "final", "read", Outcome.INFO, data("error", e.errorCode().name()));
                            throw e;
                        }
                    }
                    rec.complete(op, "final", "read", Outcome.OK, data("records", rows(r), "start", r.logStartOffset(), "end", r.highWatermark()));
                    from = r.nextFetchOffset();
                    if (r.records().isEmpty() || from >= r.highWatermark()) break;
                }
            }
        }
    }

    @Override
    public void close() {
        stopProducers.set(true);
        stopReaders.set(true);
        stopConsumers.set(true);
    }
}
