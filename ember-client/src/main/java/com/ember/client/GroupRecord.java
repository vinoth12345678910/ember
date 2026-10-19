package com.ember.client;

/** One record delivered to a group consumer. */
public record GroupRecord(String topic, int partition, long offset, byte[] key, byte[] value, long timestamp) {
}
