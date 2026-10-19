package com.ember.protocol;

/** A (topic, partition, offset) triple: a committed or fetched offset (-1 = none). */
public record PartitionOffset(String topic, int partition, long offset) {
}
