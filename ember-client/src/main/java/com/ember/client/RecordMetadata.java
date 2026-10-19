package com.ember.client;

/** Where the broker stored one produced record. */
public record RecordMetadata(String topic, int partition, long offset) {
}
