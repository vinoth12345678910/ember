package com.ember.storage;

/**
 * When the log should fsync. A flush happens as soon as EITHER limit is reached.
 * Long.MAX_VALUE means "this limit is off".
 */
public record FlushPolicy(long maxUnflushedMessages, long maxUnflushedMs) {

    /** Never fsync on our own (only on segment roll and clean close). Rely on replication. */
    public static FlushPolicy never() {
        return new FlushPolicy(Long.MAX_VALUE, Long.MAX_VALUE);
    }

    /** fsync after every single append: safest and slowest. */
    public static FlushPolicy everyMessage() {
        return new FlushPolicy(1, Long.MAX_VALUE);
    }

    public static FlushPolicy afterMessages(long n) {
        return new FlushPolicy(n, Long.MAX_VALUE);
    }

    public static FlushPolicy afterMillis(long ms) {
        return new FlushPolicy(Long.MAX_VALUE, ms);
    }
}