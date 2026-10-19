package com.ember.client;

/** What a client knows about one partition: who leads it and the offsets the answering broker reported. */
public record PartitionView(int partition, int leaderId, long logStartOffset, long logEndOffset) {
}
