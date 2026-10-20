package com.ember.metrics;

import java.util.concurrent.atomic.LongAdder;

/**
 * Counts how many recorded values fell into each bucket. Bucket i holds values v with bounds[i-1] < v <= bounds[i]
 * (a value EQUAL to a bound belongs to that bucket); one extra bucket holds everything above the last bound (+Inf).
 * Values are recorded as longs (microseconds, bytes) and exported multiplied by scale (microseconds -> seconds).
 */
public final class Histogram {

    /** 50 us to 30 s in roughly x2 steps, in microseconds. */
    public static final long[] DEFAULT_LATENCY_BOUNDS_MICROS = {
            50, 100, 200, 400, 800, 1_600, 3_200, 6_400, 12_800, 25_600, 51_200, 102_400, 204_800, 409_600, 819_200,
            1_638_400, 3_276_800, 6_553_600, 13_107_200, 30_000_000};

    /** 64 B to 16 MiB in x4 steps. */
    public static final long[] DEFAULT_BYTES_BOUNDS = {64, 256, 1_024, 4_096, 16_384, 65_536, 262_144, 1_048_576, 4_194_304, 16_777_216};

    private final long[] bounds;
    private final double scale;
    private final LongAdder[] buckets;          // bounds.length + 1 (the last one is +Inf)
    private final LongAdder sum = new LongAdder();
    private final LongAdder count = new LongAdder();

    public Histogram(long[] bounds, double scale) {
        if (bounds.length == 0) {
            throw new IllegalArgumentException("a histogram needs at least one bucket bound");
        }
        for (int i = 1; i < bounds.length; i++) {
            if (bounds[i] <= bounds[i - 1]) {
                throw new IllegalArgumentException("bucket bounds must be strictly increasing");
            }
        }
        this.bounds = bounds.clone();
        this.scale = scale;
        this.buckets = new LongAdder[bounds.length + 1];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new LongAdder();
        }
    }

    public void record(long value) {
        int lo = 0;
        int hi = bounds.length;                   // hi == bounds.length means "above every bound"
        while (lo < hi) {                         // first index whose bound >= value
            int mid = (lo + hi) >>> 1;
            if (bounds[mid] >= value) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        buckets[lo].increment();
        sum.add(value);
        count.increment();
    }

    /** What an exporter needs: cumulative counts per bound, then +Inf, plus sum and count. */
    public Snapshot snapshot() {
        long[] cumulative = new long[buckets.length];
        long running = 0;
        for (int i = 0; i < buckets.length; i++) {
            running += buckets[i].sum();
            cumulative[i] = running;
        }
        return new Snapshot(bounds, scale, cumulative, sum.sum(), count.sum());
    }

    public record Snapshot(long[] bounds, double scale, long[] cumulative, long sum, long count) {
        public long total() {
            return cumulative[cumulative.length - 1];
        }
    }
}
