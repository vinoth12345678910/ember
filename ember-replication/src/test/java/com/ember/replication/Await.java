package com.ember.replication;

import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

/** The ONE poll-until-true helper: parks briefly between checks, always has a timeout and a clear failure message. */
final class Await {

    private Await() {
    }

    static void until(long timeoutMs, String what, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out after " + timeoutMs + " ms waiting for: " + what);
            }
            LockSupport.parkNanos(2_000_000);
        }
    }
}
