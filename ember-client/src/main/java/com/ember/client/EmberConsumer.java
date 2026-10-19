package com.ember.client;

import com.ember.protocol.ErrorCode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A consumer with MANUAL assignment: you say which partitions to read and from which offset.
 * There are no consumer groups and no committed offsets (Phase 6): positions live in memory only.
 * Owns its client: close() closes the connection.
 */
public class EmberConsumer implements AutoCloseable {

    public enum StartPosition {
        BEGINNING, LATEST
    }

    public record TopicPartition(String topic, int partition) {
    }

    private final Router router;
    private final Map<TopicPartition, Long> positions = new LinkedHashMap<>();
    private int rotation = 0;           // which partition a poll starts with: advances so none is always first

    public EmberConsumer(String host, int port) throws IOException {
        this(host, port, ClientConfig.defaults());
    }

    /** Cluster-aware: every partition is read from its leader. */
    public EmberConsumer(String host, int port, ClientConfig config) throws IOException {
        this.router = new ClusterClient(host, port, config);
    }

    public EmberConsumer(ClientConfig config) {
        this.router = new ClusterClient(config);
    }

    /** One fixed connection, no routing. */
    public EmberConsumer(EmberClient client) {
        this.router = new SingleConnectionRouter(client);
    }

    public synchronized void assign(String topic, int partition, long startOffset) {
        positions.put(new TopicPartition(topic, partition), startOffset);
    }

    /**
     * Assigns every partition of the topic at its oldest offset (BEGINNING) or at the current end
     * (LATEST, i.e. only records produced from now on).
     */
    public synchronized void assignAll(String topic, StartPosition from) throws IOException {
        List<PartitionView> partitions = router.describe(topic);
        if (partitions == null) {
            throw new EmberException(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
        }
        for (PartitionView p : partitions) {
            assign(topic, p.partition(), from == StartPosition.BEGINNING ? p.logStartOffset() : p.logEndOffset());
        }
    }

    public synchronized void seek(String topic, int partition, long offset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        if (!positions.containsKey(tp)) {
            throw new IllegalStateException("not assigned: " + tp);
        }
        positions.put(tp, offset);
    }

    public synchronized long position(String topic, int partition) {
        Long position = positions.get(new TopicPartition(topic, partition));
        if (position == null) {
            throw new IllegalStateException("not assigned: " + topic + "-" + partition);
        }
        return position;
    }

    public synchronized List<TopicPartition> assignment() {
        return new ArrayList<>(positions.keySet());
    }

    /**
     * Fetches every assigned partition once and moves each position just past the records returned.
     * An empty result means every partition was caught up at the moment of the poll.
     * If any fetch fails nothing is returned and no position moves, so no record is skipped.
     * OFFSET_OUT_OF_RANGE is thrown as EmberException: resetting automatically could hide lost data.
     */
    public synchronized List<ConsumedRecord> poll(int maxBytesPerPartition) throws IOException {
        List<TopicPartition> partitions = new ArrayList<>(positions.keySet());
        List<ConsumedRecord> out = new ArrayList<>();
        if (partitions.isEmpty()) {
            return out;
        }
        int start = rotation++ % partitions.size();
        // Positions are only moved after EVERY fetch succeeded. If partition 3 fails, the records already
        // fetched from partitions 0-2 are not returned, so their positions must not move either - otherwise
        // those records would be skipped forever.
        Map<TopicPartition, Long> nextPositions = new LinkedHashMap<>();
        for (int i = 0; i < partitions.size(); i++) {
            TopicPartition tp = partitions.get((start + i) % partitions.size());
            FetchResult result = router.fetch(tp.topic(), tp.partition(), positions.get(tp), maxBytesPerPartition);
            result.records().forEach(r -> out.add(new ConsumedRecord(tp.topic(), tp.partition(), r)));
            nextPositions.put(tp, result.nextFetchOffset());
        }
        positions.putAll(nextPositions);
        return out;
    }

    @Override
    public synchronized void close() throws IOException {
        router.close();
    }
}
