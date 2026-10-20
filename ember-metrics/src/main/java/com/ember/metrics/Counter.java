package com.ember.metrics;

import java.util.concurrent.atomic.LongAdder;

/** A number that only goes up. LongAdder keeps threads from fighting over one memory cell. */
public final class Counter {

    private final LongAdder value = new LongAdder();
    private final java.util.function.LongSupplier source;       // non-null: a read-only counter that mirrors a number someone else owns

    public Counter() {
        this.source = null;
    }

    private Counter(java.util.function.LongSupplier source) {
        this.source = source;
    }

    /** A counter whose value is read from elsewhere at scrape time (it must be monotonic, or Prometheus sees a reset). */
    public static Counter fromSupplier(java.util.function.LongSupplier source) {
        return new Counter(source);
    }

    public void inc() {
        add(1);
    }

    public void add(long n) {
        if (n < 0) {
            throw new IllegalArgumentException("a counter never goes down");
        }
        if (source != null) {
            throw new UnsupportedOperationException("this counter mirrors another value and is read-only");
        }
        value.add(n);
    }

    public long value() {
        if (source != null) {
            try {
                return source.getAsLong();
            } catch (RuntimeException e) {
                return 0;
            }
        }
        return value.sum();
    }
}
