package com.ember.client;

import com.ember.protocol.ProducerRecord;

import java.io.IOException;
import java.util.List;

/**
 * How producers and consumers reach the cluster. Two implementations: one fixed connection (the Phase 3 way, good for
 * tests that control exactly what the client sees) and the cluster-aware router that follows leaders.
 */
interface Router extends AutoCloseable {

    long produce(String topic, int partition, List<ProducerRecord> records) throws IOException;

    FetchResult fetch(String topic, int partition, long offset, int maxBytes) throws IOException;

    int createTopic(String topic, int partitions) throws IOException;

    /** The partitions of the topic, or null if the topic does not exist. */
    List<PartitionView> describe(String topic) throws IOException;

    @Override
    void close() throws IOException;
}
