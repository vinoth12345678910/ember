package com.ember.broker;

import com.ember.client.ConsumedRecord;
import com.ember.client.EmberConsumer;
import com.ember.storage.LogRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Shared helpers for the multi-partition integration tests. */
final class PartitionTestSupport {

    private PartitionTestSupport() {
    }

    /** Reads every record of every partition from the beginning, keeping each partition's offset order. */
    static Map<Integer, List<LogRecord>> readAll(int port, String topic) throws Exception {
        Map<Integer, List<LogRecord>> byPartition = new TreeMap<>();
        try (EmberConsumer consumer = new EmberConsumer("localhost", port)) {
            consumer.assignAll(topic, EmberConsumer.StartPosition.BEGINNING);
            consumer.assignment().forEach(tp -> byPartition.put(tp.partition(), new ArrayList<>()));
            while (true) {
                List<ConsumedRecord> batch = consumer.poll(1_000_000);
                if (batch.isEmpty()) {
                    return byPartition;
                }
                for (ConsumedRecord r : batch) {
                    byPartition.get(r.partition()).add(r.record());
                }
            }
        }
    }

    /** Offsets in one partition must be exactly start, start+1, start+2, ... */
    static void assertContiguous(List<LogRecord> records, long start) {
        long expected = start;
        for (LogRecord r : records) {
            if (r.offset() != expected) {
                throw new AssertionError("expected offset " + expected + " but found " + r.offset());
            }
            expected++;
        }
    }

    static int total(Map<Integer, List<LogRecord>> byPartition) {
        return byPartition.values().stream().mapToInt(List::size).sum();
    }
}
