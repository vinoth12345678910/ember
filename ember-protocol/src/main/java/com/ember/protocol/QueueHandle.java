package com.ember.protocol;

/** Identifies one delivery of one message: acks from an older attempt are rejected (fencing). */
public record QueueHandle(int partition, long offset, int attempt) {
}
