package com.ember.protocol;

import java.util.List;

/** total: how many dead messages the subscription has (items may be fewer). */
public record QueueDeadListResponse(int total, List<DeadMessage> items) implements Response {

    /** A message that used up its attempts. key/value are empty if retention already removed the record. */
    public record DeadMessage(int partition, long offset, int attempts, int failures, long deadAt, String reason, byte[] key, byte[] value) {
    }

    public QueueDeadListResponse {
        items = List.copyOf(items);
    }
}
