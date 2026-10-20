package com.ember.client;

/**
 * A fixed-bucket latency histogram in microseconds: 32 sub-buckets per power of two, so every recorded value is reported within
 * within 1/32 (about 3%) of the truth (up to hours), in a few kilobytes. Values below 32 us are exact. Not thread-safe: use one per thread
 * and merge() them at the end.
 */
public final class LatencyHistogram {

    private static final int SUB_BITS = 5;
    private static final int SUB = 1 << SUB_BITS;            // 32 sub-buckets per power of two
    private static final int MAX_EXP = 42;                   // 2^42 us is about 50 days

    private final long[] counts = new long[(MAX_EXP - SUB_BITS + 2) * SUB];
    private long total;
    private long max;

    static int indexOf(long micros) {
        if (micros < SUB) {
            return (int) Math.max(micros, 0);
        }
        int exp = 63 - Long.numberOfLeadingZeros(micros);
        if (exp > MAX_EXP) {
            return counts_length_guard();
        }
        int sub = (int) ((micros >> (exp - SUB_BITS)) & (SUB - 1));
        return (exp - SUB_BITS + 1) * SUB + sub;
    }

    private static int counts_length_guard() {
        return (MAX_EXP - SUB_BITS + 2) * SUB - 1;
    }

    /** The largest value that falls into the bucket (what a percentile reports). */
    static long upperBoundOf(int index) {
        if (index < SUB) {
            return index;
        }
        int exp = index / SUB + SUB_BITS - 1;
        int sub = index % SUB;
        long lower = ((long) SUB + sub) << (exp - SUB_BITS);
        return lower + (1L << (exp - SUB_BITS)) - 1;
    }

    public void record(long micros) {
        counts[indexOf(micros)]++;
        total++;
        if (micros > max) {
            max = micros;
        }
    }

    public void merge(LatencyHistogram other) {
        for (int i = 0; i < counts.length; i++) {
            counts[i] += other.counts[i];
        }
        total += other.total;
        max = Math.max(max, other.max);
    }

    public long count() {
        return total;
    }

    public long max() {
        return max;
    }

    /** q in (0,1]: the smallest value v such that at least q of the recordings are <= v (to bucket resolution). 0 when empty. */
    public long percentile(double q) {
        if (total == 0) {
            return 0;
        }
        long rank = (long) Math.ceil(q * total);
        long seen = 0;
        for (int i = 0; i < counts.length; i++) {
            seen += counts[i];
            if (seen >= rank) {
                return Math.min(upperBoundOf(i), max);
            }
        }
        return max;
    }
}
