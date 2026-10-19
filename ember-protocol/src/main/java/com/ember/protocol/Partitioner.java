package com.ember.protocol;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

/**
 * Decides which partition a record goes to. Lives in the protocol module so every client (and
 * every broker-side test) shares ONE implementation: if two clients disagreed, the same key would
 * land in different partitions and per-key ordering would silently break.
 */
public final class Partitioner {

    private final IntSupplier roundRobinCounter;

    /** Round-robin starts at partition 0. */
    public Partitioner() {
        AtomicInteger counter = new AtomicInteger();
        this.roundRobinCounter = counter::getAndIncrement;
    }

    /** The counter is injectable so tests can make null-key placement deterministic. */
    public Partitioner(IntSupplier roundRobinCounter) {
        this.roundRobinCounter = roundRobinCounter;
    }

    public int partition(byte[] key, int partitionCount) {
        if (partitionCount < 1) {
            throw new IllegalArgumentException("partitionCount must be >= 1");
        }
        if (key == null) {
            // mask, not Math.abs: abs(Integer.MIN_VALUE) is still negative
            return (roundRobinCounter.getAsInt() & 0x7fffffff) % partitionCount;
        }
        return (murmur2(key) & 0x7fffffff) % partitionCount;
    }

    /** Kafka's murmur2, bit for bit, so keys land where any Kafka-compatible client would put them. */
    public static int murmur2(byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        final int m = 0x5bd1e995;
        final int r = 24;

        int h = seed ^ length;
        int blocks = length / 4;
        for (int i = 0; i < blocks; i++) {
            int i4 = i * 4;
            int k = (data[i4] & 0xff) | ((data[i4 + 1] & 0xff) << 8)
                    | ((data[i4 + 2] & 0xff) << 16) | ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        int tail = length & ~3;                     // index of the first byte that is not in a full block
        switch (length % 4) {
            case 3:
                h ^= (data[tail + 2] & 0xff) << 16;
                // fall through on purpose: the remaining bytes are mixed in too
            case 2:
                h ^= (data[tail + 1] & 0xff) << 8;
            case 1:
                h ^= data[tail] & 0xff;
                h *= m;
            default:
                break;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }
}
