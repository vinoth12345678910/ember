package com.ember.client;

import com.ember.protocol.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One producer shared by any number of threads. send() returns at once with a future; a single sender thread collects what has
 * arrived (waiting at most lingerMs for more), groups it by topic and ships each group as one pipelined batch, so thousands of
 * small publishes cost a handful of requests. While one batch is in flight the next one fills up on its own: the busier the
 * system, the bigger the batches.
 *
 * Order: records of the same key reach their partition in the order send() was called (one sender thread, one batch at a time).
 * Back-pressure: the queue is bounded. When it is full, send() waits up to maxBlockMs and then fails the future with
 * {@link ProducerOverloadedException} instead of letting memory grow without limit.
 * Delivery is at-least-once, like {@link EmberProducer}.
 */
public final class BatchingProducer implements AutoCloseable {

    public record Settings(int connections, int maxBatchRecords, int maxBatchBytes, int lingerMs, int maxQueuedRecords, int maxBlockMs) {
        public static Settings defaults() {
            return new Settings(8, 500, 4 * 1024 * 1024, 2, 20_000, 200);
        }

        public Settings {
            if (connections < 1 || maxBatchRecords < 1 || maxBatchBytes < 1 || lingerMs < 0 || maxQueuedRecords < 1 || maxBlockMs < 0) {
                throw new IllegalArgumentException("bad batching settings");
            }
        }
    }

    /** Counters for dashboards. */
    public record Stats(long acknowledged, long failed, long rejected, long batches, int queued) {
        public double averageBatch() {
            return batches == 0 ? 0 : (double) (acknowledged + failed) / batches;
        }
    }

    private record Pending(String topic, ProducerRecord record, int bytes, CompletableFuture<RecordMetadata> future) {
    }

    private final EmberProducer producer;
    private final Settings settings;
    private final BlockingQueue<Pending> queue;
    private final Thread sender;
    private final AtomicLong acknowledged = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong batches = new AtomicLong();
    private volatile boolean closed;

    public BatchingProducer(ClientConfig config, Settings settings) {
        this.producer = EmberProducer.pipelined(config, settings.connections());
        this.settings = settings;
        this.queue = new ArrayBlockingQueue<>(settings.maxQueuedRecords());
        this.sender = new Thread(this::run, "ember-batching-producer");
        this.sender.setDaemon(true);
        this.sender.start();
    }

    public CompletableFuture<RecordMetadata> send(String topic, byte[] key, byte[] value) {
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        if (closed) {
            future.completeExceptionally(new IOException("producer is closed"));
            return future;
        }
        int bytes = (key == null ? 0 : key.length) + (value == null ? 0 : value.length) + 32;
        try {
            if (!queue.offer(new Pending(topic, new ProducerRecord(key, value), bytes, future), settings.maxBlockMs(), TimeUnit.MILLISECONDS)) {
                rejected.incrementAndGet();
                future.completeExceptionally(new ProducerOverloadedException("producer queue is full (" + settings.maxQueuedRecords() + " records waiting)"));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.completeExceptionally(new IOException("interrupted while queueing", e));
        }
        return future;
    }

    public Stats stats() {
        return new Stats(acknowledged.get(), failed.get(), rejected.get(), batches.get(), queue.size());
    }

    private void run() {
        List<Pending> batch = new ArrayList<>();
        while (!closed || !queue.isEmpty()) {
            batch.clear();
            try {
                Pending first = queue.poll(100, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                int bytes = first.bytes();
                long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.lingerMs());
                while (batch.size() < settings.maxBatchRecords() && bytes < settings.maxBatchBytes()) {
                    Pending next = queue.poll();
                    if (next == null) {
                        long left = deadline - System.nanoTime();
                        if (left <= 0) {
                            break;
                        }
                        next = queue.poll(left, TimeUnit.NANOSECONDS);
                        if (next == null) {
                            break;
                        }
                    }
                    batch.add(next);
                    bytes += next.bytes();
                }
            } catch (InterruptedException e) {
                break;
            }
            ship(batch);
        }
        for (Pending p : queue) {                                   // closed with work left over
            p.future().completeExceptionally(new IOException("producer closed before the record was sent"));
        }
        queue.clear();
    }

    private void ship(List<Pending> batch) {
        Map<String, List<Pending>> byTopic = new LinkedHashMap<>();
        for (Pending p : batch) {
            byTopic.computeIfAbsent(p.topic(), t -> new ArrayList<>()).add(p);
        }
        for (Map.Entry<String, List<Pending>> e : byTopic.entrySet()) {
            List<Pending> pending = e.getValue();
            batches.incrementAndGet();
            try {
                List<ProducerRecord> records = new ArrayList<>(pending.size());
                for (Pending p : pending) {
                    records.add(p.record());
                }
                List<EmberProducer.Outcome> outcomes = producer.sendBatchEach(e.getKey(), records);
                for (int i = 0; i < pending.size(); i++) {
                    EmberProducer.Outcome o = outcomes.get(i);
                    if (o.error() == null) {
                        acknowledged.incrementAndGet();
                        pending.get(i).future().complete(o.metadata());
                    } else {
                        failed.incrementAndGet();
                        pending.get(i).future().completeExceptionally(o.error());
                    }
                }
            } catch (IOException | RuntimeException ex) {
                for (Pending p : pending) {
                    failed.incrementAndGet();
                    p.future().completeExceptionally(ex);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        try {
            sender.join(15_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        producer.close();
    }
}
