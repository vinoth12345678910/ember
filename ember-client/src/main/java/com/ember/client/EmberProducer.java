package com.ember.client;

import com.ember.protocol.ErrorCode;
import com.ember.protocol.Partitioner;
import com.ember.protocol.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * Routes records to partitions (this is where partitioning happens: the broker never looks at keys),
 * and sends one PRODUCE request per partition. Owns its client: close() closes the connection.
 *
 * Pipelined mode ({@link #pipelined}): the per-partition requests of one batch are sent at the same time over separate
 * connections instead of one after the other, so a batch spread over P partitions costs about one round trip, not P.
 * The default mode is unchanged.
 *
 * Delivery is at-least-once: if a request fails after the broker stored it (for example the reply is
 * lost), a retry by the caller creates a duplicate. An idempotent producer is a later phase.
 */
public class EmberProducer implements AutoCloseable {

    /** What we remember about a topic: its (immutable) partition count and who leads each partition. */
    private record TopicInfo(int partitionCount, List<Integer> leaders) {
    }

    private final Router router;
    private final ClientConfig config;
    private final Supplier<IntSupplier> roundRobinFactory;
    private final Map<String, TopicInfo> cache = new HashMap<>();
    private final Map<String, Partitioner> partitioners = new HashMap<>();
    private final BlockingQueue<Router> idleConnections;       // pipelined mode only: connections not currently carrying a request
    private final List<Router> allConnections;
    private final ExecutorService senders;

    /** The outcome of one record in {@link #sendBatchEach}: either where it landed or why it did not. */
    public record Outcome(RecordMetadata metadata, Exception error) {
    }

    public EmberProducer(String host, int port) throws IOException {
        this(host, port, ClientConfig.defaults());
    }

    /** Cluster-aware: finds the leaders itself and retries retriable errors (see ClusterClient). */
    public EmberProducer(String host, int port, ClientConfig config) throws IOException {
        this(new ClusterClient(host, port, config), config, EmberProducer::newCounter);
    }

    /** Several bootstrap brokers (host:port) are given in the config. */
    public EmberProducer(ClientConfig config) {
        this(new ClusterClient(config), config, EmberProducer::newCounter);
    }

    /** One fixed connection, no routing: the Phase 3 behaviour (refresh once, retry once). */
    public EmberProducer(EmberClient client, ClientConfig config) {
        this(client, config, EmberProducer::newCounter);
    }

    /** The factory makes one round-robin counter per topic; tests inject deterministic ones. */
    public EmberProducer(EmberClient client, ClientConfig config, Supplier<IntSupplier> roundRobinFactory) {
        this(new SingleConnectionRouter(client), config, roundRobinFactory);
    }

    private EmberProducer(Router router, ClientConfig config, Supplier<IntSupplier> roundRobinFactory) {
        this(router, config, roundRobinFactory, 0);
    }

    private EmberProducer(Router router, ClientConfig config, Supplier<IntSupplier> roundRobinFactory, int connections) {
        this.router = router;
        this.config = config;
        this.roundRobinFactory = roundRobinFactory;
        if (connections > 0) {
            this.idleConnections = new LinkedBlockingQueue<>();
            this.allConnections = new ArrayList<>();
            for (int i = 0; i < connections; i++) {
                ClusterClient c = new ClusterClient(config);
                allConnections.add(c);
                idleConnections.add(c);
            }
            this.senders = Executors.newFixedThreadPool(connections, r -> {
                Thread t = new Thread(r, "ember-producer-sender");
                t.setDaemon(true);
                return t;
            });
        } else {
            this.idleConnections = null;
            this.allConnections = List.of();
            this.senders = null;
        }
    }

    /**
     * A cluster-aware producer that sends the partitions of a batch in parallel over {@code connections} separate
     * cluster clients. Records of one partition still keep their order: one request per partition per batch,
     * and batches are sent one after another.
     */
    public static EmberProducer pipelined(ClientConfig config, int connections) {
        if (connections < 1) {
            throw new IllegalArgumentException("connections must be >= 1");
        }
        return new EmberProducer(new ClusterClient(config), config, EmberProducer::newCounter, connections);
    }

    /** Sends that had to be retried by the cluster client (0 for the single-connection mode). */
    public long retriedRequests() {
        long total = router instanceof ClusterClient c ? c.retriedRequests() : 0;
        for (Router r : allConnections) {
            total += r instanceof ClusterClient c ? c.retriedRequests() : 0;
        }
        return total;
    }

    private static IntSupplier newCounter() {
        int[] next = {0};
        return () -> next[0]++;
    }

    public synchronized RecordMetadata send(String topic, byte[] key, byte[] value) throws IOException {
        return sendBatch(topic, List.of(new ProducerRecord(key, value))).get(0);
    }

    /**
     * Returns one RecordMetadata per input record, in INPUT order, however the records were spread
     * over partitions. Inside each partition the input order is preserved.
     */
    public synchronized List<RecordMetadata> sendBatch(String topic, List<ProducerRecord> records) throws IOException {
        if (records.isEmpty()) {
            return List.of();
        }
        if (senders != null) {
            List<RecordMetadata> out = new ArrayList<>(records.size());
            for (Outcome o : sendBatchEach(topic, records)) {
                if (o.error() instanceof IOException e) {
                    throw e;
                }
                if (o.error() instanceof RuntimeException e) {
                    throw e;
                }
                out.add(o.metadata());
            }
            return out;
        }
        int partitionCount = topicInfo(topic).partitionCount();
        Partitioner partitioner = partitioners.computeIfAbsent(topic, t -> new Partitioner(roundRobinFactory.get()));

        // partition -> indexes of the input records going there (ascending partition order = deterministic)
        Map<Integer, List<Integer>> byPartition = new TreeMap<>();
        for (int i = 0; i < records.size(); i++) {
            int partition = partitioner.partition(records.get(i).key(), partitionCount);
            byPartition.computeIfAbsent(partition, p -> new ArrayList<>()).add(i);
        }

        RecordMetadata[] result = new RecordMetadata[records.size()];
        for (Map.Entry<Integer, List<Integer>> entry : byPartition.entrySet()) {
            int partition = entry.getKey();
            List<Integer> indexes = entry.getValue();
            List<ProducerRecord> group = new ArrayList<>(indexes.size());
            for (int index : indexes) {
                group.add(records.get(index));
            }
            long baseOffset = produceWithOneRetry(topic, partition, group);
            for (int j = 0; j < indexes.size(); j++) {
                result[indexes.get(j)] = new RecordMetadata(topic, partition, baseOffset + j);   // offsets are contiguous
            }
        }
        return List.of(result);
    }

    /**
     * Pipelined mode only. Like sendBatch, but every partition is attempted even if another failed, and the result says per record
     * what happened (input order). Never throws for a failed partition: that is reported in its records' outcomes.
     */
    public synchronized List<Outcome> sendBatchEach(String topic, List<ProducerRecord> records) throws IOException {
        if (senders == null) {
            throw new IllegalStateException("sendBatchEach needs a pipelined producer (EmberProducer.pipelined)");
        }
        if (records.isEmpty()) {
            return List.of();
        }
        int partitionCount = topicInfo(topic).partitionCount();
        Partitioner partitioner = partitioners.computeIfAbsent(topic, t -> new Partitioner(roundRobinFactory.get()));
        Map<Integer, List<Integer>> byPartition = new TreeMap<>();
        for (int i = 0; i < records.size(); i++) {
            byPartition.computeIfAbsent(partitioner.partition(records.get(i).key(), partitionCount), p -> new ArrayList<>()).add(i);
        }

        Map<Integer, Future<Long>> inFlight = new TreeMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : byPartition.entrySet()) {
            int partition = entry.getKey();
            List<ProducerRecord> group = new ArrayList<>(entry.getValue().size());
            for (int index : entry.getValue()) {
                group.add(records.get(index));
            }
            inFlight.put(partition, senders.submit(() -> {
                Router connection = idleConnections.take();
                try {
                    return connection.produce(topic, partition, group);
                } finally {
                    idleConnections.add(connection);
                }
            }));
        }

        Outcome[] result = new Outcome[records.size()];
        for (Map.Entry<Integer, Future<Long>> entry : inFlight.entrySet()) {
            int partition = entry.getKey();
            List<Integer> indexes = byPartition.get(partition);
            Long base = null;
            Exception failure = null;
            try {
                base = waitFor(entry.getValue());
            } catch (EmberException e) {
                if (e.errorCode() == ErrorCode.UNKNOWN_TOPIC_OR_PARTITION) {        // stale metadata: refresh once and retry this one request
                    try {
                        cache.remove(topic);
                        topicInfo(topic);
                        List<ProducerRecord> group = new ArrayList<>();
                        for (int index : indexes) {
                            group.add(records.get(index));
                        }
                        base = router.produce(topic, partition, group);
                    } catch (IOException | RuntimeException again) {
                        failure = again;
                    }
                } else {
                    failure = e;
                }
            } catch (IOException | RuntimeException e) {
                failure = e;
            }
            for (int j = 0; j < indexes.size(); j++) {
                result[indexes.get(j)] = failure == null ? new Outcome(new RecordMetadata(topic, partition, base + j), null) : new Outcome(null, failure);
            }
        }
        return List.of(result);
    }

    private static long waitFor(Future<Long> future) throws IOException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while sending", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IOException(cause);
        }
    }

    public synchronized int partitionCount(String topic) throws IOException {
        return topicInfo(topic).partitionCount();
    }

    /** Forgets what we know about a topic; the next send asks the broker again. */
    public synchronized void invalidate(String topic) {
        cache.remove(topic);
    }

    private long produceWithOneRetry(String topic, int partition, List<ProducerRecord> group) throws IOException {
        try {
            return router.produce(topic, partition, group);
        } catch (EmberException e) {
            if (e.errorCode() != ErrorCode.UNKNOWN_TOPIC_OR_PARTITION) {
                throw e;                                    // nothing else is retried: see the class comment
            }
            // our metadata may be stale: refresh ONCE, retry this request ONCE, then give up
            cache.remove(topic);
            topicInfo(topic);
            return router.produce(topic, partition, group);
        }
    }

    private TopicInfo topicInfo(String topic) throws IOException {
        TopicInfo cached = cache.get(topic);
        if (cached != null) {
            return cached;
        }
        TopicInfo info = lookup(topic);
        if (info == null) {
            if (!config.autoCreateTopics()) {
                throw new EmberException(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
            }
            try {
                router.createTopic(topic, config.defaultPartitions());
            } catch (EmberException e) {
                if (e.errorCode() != ErrorCode.TOPIC_ALREADY_EXISTS) {
                    throw e;
                }                                           // somebody else created it first: that is fine
            }
            info = lookup(topic);
            if (info == null) {
                throw new EmberException(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
            }
        }
        cache.put(topic, info);
        return info;
    }

    private TopicInfo lookup(String topic) throws IOException {
        List<PartitionView> partitions = router.describe(topic);
        if (partitions == null) {
            return null;
        }
        List<Integer> leaders = new ArrayList<>();
        for (PartitionView p : partitions) {
            leaders.add(p.leaderId());
        }
        return new TopicInfo(partitions.size(), leaders);
    }

    @Override
    public synchronized void close() throws IOException {
        if (senders != null) {
            senders.shutdownNow();
        }
        IOException first = null;
        for (Router r : allConnections) {
            try {
                r.close();
            } catch (IOException e) {
                first = first == null ? e : first;
            }
        }
        router.close();
        if (first != null) {
            throw first;
        }
    }
}
