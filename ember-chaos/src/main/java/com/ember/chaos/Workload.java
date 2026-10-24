package com.ember.chaos;

import com.ember.client.ClientConfig;
import com.ember.client.ClusterClient;
import com.ember.client.CommitFailedException;
import com.ember.client.EmberException;
import com.ember.client.EmberGroupConsumer;
import com.ember.client.FetchResult;
import com.ember.client.GroupConsumerConfig;
import com.ember.client.QueueConsumer;
import com.ember.client.GroupRecord;
import com.ember.protocol.ErrorCode;
import com.ember.protocol.Partitioner;
import com.ember.protocol.PartitionRef;
import com.ember.protocol.QueueDeadListResponse;
import com.ember.protocol.QueueDescribeResponse;
import com.ember.protocol.QueueMessage;
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
    public record Config(String topic, int partitions, int producers, int readers, int consumers, List<String> bootstrap, String group, double ratePerSecond,
                         int queueWorkers, String queue, boolean cooperative) {
        public Config(String topic, int partitions, int producers, int readers, int consumers, List<String> bootstrap, String group, double ratePerSecond,
                      int queueWorkers, String queue) {
            this(topic, partitions, producers, readers, consumers, bootstrap, group, ratePerSecond, queueWorkers, queue, false);
        }

        public Config(String topic, int partitions, int producers, int readers, int consumers, List<String> bootstrap, String group, double ratePerSecond) {
            this(topic, partitions, producers, readers, consumers, bootstrap, group, ratePerSecond, 0, null, false);
        }

        public Config(String topic, int partitions, int producers, int readers, int consumers, List<String> bootstrap, String group) {
            this(topic, partitions, producers, readers, consumers, bootstrap, group, 0, 0, null, false);
        }
    }

    /** Ack-queue chaos: a subscription with a short lease and 4 attempts, so redelivery and dead-lettering happen within a run. */
    static final int QUEUE_LEASE_MS = 1500;
    static final int QUEUE_MAX_ATTEMPTS = 4;

    private static final int GROUP_SESSION_MS = 2000;
    private static final int GROUP_HEARTBEAT_MS = 400;

    private final Config config;
    private final HistoryRecorder rec;
    private final ClientConfig clientConfig;
    private final AtomicBoolean stopProducers = new AtomicBoolean();
    private final AtomicBoolean stopReaders = new AtomicBoolean();
    private final AtomicBoolean stopConsumers = new AtomicBoolean();
    private final AtomicBoolean stopQueue = new AtomicBoolean();
    private volatile boolean poisonActive = true;                  // flips to false when the operator "deploys the fix"
    private final List<Thread> queueThreads = new ArrayList<>();
    private final Set<String> acked = ConcurrentHashMap.newKeySet();
    private final Set<String> delivered = ConcurrentHashMap.newKeySet();
    private final Set<String> queueDelivered = ConcurrentHashMap.newKeySet();
    private final Set<String> queueAcked = ConcurrentHashMap.newKeySet();
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
        if (config.queueWorkers() > 0) {
            QueueConsumer.createSubscription(clientConfig.withRetries(40, 100, 500), config.queue(), config.topic(), true, QUEUE_LEASE_MS, QUEUE_MAX_ATTEMPTS, 100, 300);
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
        for (int i = 0; i < config.queueWorkers(); i++) {
            String who = "q" + i;
            spawn(queueThreads, who, () -> queueLoop(who));
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
                .withClientId(process).withSession(GROUP_SESSION_MS, GROUP_HEARTBEAT_MS).withAutoCommit(false, 5_000)
                .withIncrementalRejoin(config.cooperative());
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

    // ------------------------------------------------------------------ ack-queue workers

    /** The behaviour of a worker for one message: a poison message always fails (until the fix), a few workers "die" holding it, a few give it back. */
    private String decide(Random random, String value) {
        if (poisonActive && isPoison(value)) return "nack";
        int roll = random.nextInt(100);
        if (roll < 3) return "abandon";                                  // the worker dies: nothing is settled, the lease must run out
        if (roll < 5) return "late-ack";                                 // the worker is slow: it acks after its lease ran out (fenced if someone took over)
        if (roll < 8) return "release";
        return "ack";
    }

    /** Every 47th message of each producer is poison. */
    static boolean isPoison(String value) {
        int dash = value.lastIndexOf('-');
        if (dash < 0) return false;
        try {
            return Long.parseLong(value.substring(dash + 1)) % 47 == 3;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private void queueLoop(String process) {
        Random random = new Random(process.hashCode());
        try (QueueConsumer q = new QueueConsumer(clientConfig.withRetries(6, 50, 300), config.queue(), process)) {
            while (!stopQueue.get()) {
                long op = rec.invoke(process, "claim", data("worker", process, "queue", config.queue()));
                List<QueueMessage> got;
                try {
                    got = q.claim(8);
                    List<List<Object>> rows = new ArrayList<>();
                    for (QueueMessage m : got) {
                        String value = new String(m.value(), StandardCharsets.UTF_8);
                        rows.add(List.of(m.partition(), m.offset(), m.attempt(), value));
                        queueDelivered.add(value);
                    }
                    rec.complete(op, process, "claim", Outcome.OK, data("messages", rows));
                } catch (EmberException | IOException e) {
                    rec.complete(op, process, "claim", Outcome.INFO, data("messages", List.of(), "error", e.toString()));
                    LockSupport.parkNanos(60_000_000L);
                    continue;
                }
                if (got.isEmpty()) {
                    LockSupport.parkNanos(30_000_000L);
                    continue;
                }
                for (QueueMessage m : got) {
                    if (stopQueue.get()) break;
                    String value = new String(m.value(), StandardCharsets.UTF_8);
                    LockSupport.parkNanos(1_000_000L);                    // a little work
                    switch (decide(random, value)) {
                        case "ack" -> settle(q, process, "ack", m, "");
                        case "nack" -> settle(q, process, "nack", m, "poison message");
                        case "release" -> settle(q, process, "release", m, "");
                        case "late-ack" -> {
                            LockSupport.parkNanos((QUEUE_LEASE_MS + 400L) * 1_000_000L);
                            settle(q, process, "ack", m, "");
                        }
                        default -> { }                                    // abandon: no settle at all
                    }
                }
            }
        } catch (IOException ignored) {
            // closing the client
        }
    }

    /** One handle per operation, so the history says exactly which deliveries were accepted. */
    private void settle(QueueConsumer q, String process, String action, QueueMessage m, String reason) {
        List<Object> handle = List.of(m.partition(), m.offset(), m.attempt());
        long op = rec.invoke(process, "settle", data("worker", process, "queue", config.queue(), "action", action, "handles", List.of(handle)));
        try {
            com.ember.protocol.QueueSettleResponse r = switch (action) {
                case "ack" -> q.ack(List.of(m));
                case "nack" -> q.nack(List.of(m), reason);
                default -> q.release(List.of(m));
            };
            rec.complete(op, process, "settle", Outcome.OK, data("settled", r.settled(), "rejected", r.rejected()));
            if (action.equals("ack") && r.settled() > 0) queueAcked.add(new String(m.value(), StandardCharsets.UTF_8));
        } catch (EmberException | IOException e) {
            rec.complete(op, process, "settle", Outcome.INFO, data("error", e.toString()));      // unknown: it may or may not have been applied
        }
    }

    /** Waits until nothing is waiting, claimed or retrying (messages that died stay dead). Returns false on timeout. */
    public boolean drainQueue(long timeoutMs) {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        try (QueueConsumer observer = new QueueConsumer(clientConfig.withRetries(40, 100, 500), config.queue(), "observer")) {
            while (System.nanoTime() < deadline) {
                try {
                    QueueDescribeResponse d = observer.describe();
                    boolean open = false;
                    for (QueueDescribeResponse.PartitionState p : d.partitions()) {
                        if (p.next() < p.logEnd() || p.inFlight() > 0 || p.redeliver() > 0) open = true;
                    }
                    if (!open) return true;
                } catch (EmberException | IOException e) {
                    // not reachable right now: keep waiting
                }
                LockSupport.parkNanos(200_000_000L);
            }
        } catch (IOException ignored) {
            // closing
        }
        return false;
    }

    /** Records the final state of the queue (per-partition progress and the dead-letter list) as one operation. */
    public void queueFinal(boolean afterFix) throws IOException {
        long op = rec.invoke("final", "queue-final", data("queue", config.queue(), "afterFix", afterFix));
        try (QueueConsumer q = new QueueConsumer(clientConfig.withRetries(40, 100, 500), config.queue(), "final")) {
            QueueDescribeResponse d = q.describe();
            QueueDeadListResponse dead = q.deadLetters(1000);
            List<List<Object>> parts = new ArrayList<>();
            for (QueueDescribeResponse.PartitionState p : d.partitions()) {
                parts.add(List.of(p.partition(), p.floor(), p.next(), p.logEnd(), p.inFlight(), p.redeliver(), p.doneAbove(), p.dead()));
            }
            List<List<Object>> items = new ArrayList<>();
            for (QueueDeadListResponse.DeadMessage m : dead.items()) {
                items.add(List.of(m.partition(), m.offset(), m.failures(), new String(m.value(), StandardCharsets.UTF_8)));
            }
            rec.complete(op, "final", "queue-final", Outcome.OK, data("partitions", parts, "dead", items, "deadTotal", dead.total(), "maxAttempts", d.maxAttempts()));
        } catch (EmberException | IOException e) {
            rec.complete(op, "final", "queue-final", Outcome.INFO, data("error", e.toString()));
            throw e instanceof IOException io ? io : new IOException(e);
        }
    }

    /** The operator deploys the fix (poison messages stop failing) and retries every dead letter. */
    public int operatorFix() throws IOException {
        poisonActive = false;
        try (QueueConsumer q = new QueueConsumer(clientConfig.withRetries(40, 100, 500), config.queue(), "operator")) {
            QueueDeadListResponse dead = q.deadLetters(1000);
            if (dead.items().isEmpty()) return 0;
            List<List<Object>> handles = new ArrayList<>();
            for (QueueDeadListResponse.DeadMessage m : dead.items()) handles.add(List.of(m.partition(), m.offset(), 0));
            long op = rec.invoke("operator", "settle", data("worker", "operator", "queue", config.queue(), "action", "dead-retry", "handles", handles));
            try {
                var r = q.retryDead(dead.items());
                rec.complete(op, "operator", "settle", Outcome.OK, data("settled", r.settled(), "rejected", r.rejected()));
                return r.settled();
            } catch (EmberException | IOException e) {
                rec.complete(op, "operator", "settle", Outcome.INFO, data("error", e.toString()));
                throw e instanceof IOException io ? io : new IOException(e);
            }
        }
    }

    public void stopQueueWorkers() throws InterruptedException {
        stopQueue.set(true);
        for (Thread t : queueThreads) t.join(30_000);
    }

    public Set<String> queueDeliveredValues() {
        return Set.copyOf(queueDelivered);
    }

    public Set<String> queueAckedValues() {
        return Set.copyOf(queueAcked);
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
        stopQueue.set(true);
    }
}
