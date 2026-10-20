package com.ember.metrics;

import java.util.function.DoubleSupplier;

/**
 * A number that is READ when somebody scrapes, not pushed. The supplier runs on the HTTP thread: it must be cheap and
 * must never block (no locks the Raft threads hold, no I/O). If it throws, that one sample is skipped.
 */
public final class Gauge {

    private final DoubleSupplier supplier;

    public Gauge(DoubleSupplier supplier) {
        this.supplier = supplier;
    }

    /** NaN when the supplier failed. */
    public double value() {
        try {
            return supplier.getAsDouble();
        } catch (RuntimeException e) {
            return Double.NaN;
        }
    }
}
