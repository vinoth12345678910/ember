package com.ember.client;

/**
 * The send schedule of a fixed-rate (open-loop) benchmark: item i of a producer is INTENDED at start + i * interval, whether or
 * not the previous item has finished. Latency is measured from the intended time, so a stall shows up in every request that
 * should have been sent during it (avoiding "coordinated omission").
 */
public final class OpenLoopSchedule {

    private final long startNanos;
    private final double intervalNanos;

    /** ratePerSecond is this producer's own rate (the total rate divided by the number of producers). */
    public OpenLoopSchedule(long startNanos, double ratePerSecond) {
        if (ratePerSecond <= 0) {
            throw new IllegalArgumentException("the rate must be positive");
        }
        this.startNanos = startNanos;
        this.intervalNanos = 1e9 / ratePerSecond;
    }

    public long intendedNanos(long index) {
        return startNanos + (long) (index * intervalNanos);
    }

    /** How long to wait before sending item `index`: zero if we are already late (we then send at once, never skip). */
    public long waitNanos(long index, long nowNanos) {
        return Math.max(0, intendedNanos(index) - nowNanos);
    }

    /** From the INTENDED time, not from when the send actually started. */
    public long latencyNanos(long index, long completedNanos) {
        return completedNanos - intendedNanos(index);
    }
}
