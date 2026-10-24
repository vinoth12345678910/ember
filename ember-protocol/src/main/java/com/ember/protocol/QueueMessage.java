package com.ember.protocol;

/** One claimed message: where it is, which delivery this is (1 = first), and its content. */
public record QueueMessage(int partition, long offset, int attempt, long timestamp, byte[] key, byte[] value) {
    public QueueHandle handle() {
        return new QueueHandle(partition, offset, attempt);
    }
}
