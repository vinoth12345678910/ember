package com.ember.replication;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** A FaultInjector that tests and the chaos tool switch on and off at run time. Thread-safe. Starts with nothing injected. */
public final class ConfigurableFaults implements FaultInjector {

    private volatile boolean active;
    private volatile long fsyncDelayNanos;
    private volatile double dropProbability;
    private volatile long peerDelayMillis;
    private volatile boolean paused;
    private volatile Random random = new Random(0);

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition resumed = lock.newCondition();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong delayed = new AtomicLong();

    private void recompute() {
        active = fsyncDelayNanos > 0 || dropProbability > 0 || peerDelayMillis > 0 || paused;
    }

    public synchronized ConfigurableFaults slowFsync(long millis) {
        fsyncDelayNanos = TimeUnit.MILLISECONDS.toNanos(millis);
        recompute();
        return this;
    }

    /** probability in [0,1]; the seed makes the drops repeatable. */
    public synchronized ConfigurableFaults dropMessages(double probability, long seed) {
        if (probability < 0 || probability > 1) {
            throw new IllegalArgumentException("probability must be in [0,1]");
        }
        random = new Random(seed);
        dropProbability = probability;
        recompute();
        return this;
    }

    public synchronized ConfigurableFaults delayMessages(long millis) {
        peerDelayMillis = millis;
        recompute();
        return this;
    }

    public ConfigurableFaults pause() {
        lock.lock();
        try {
            paused = true;
            recompute();
        } finally {
            lock.unlock();
        }
        return this;
    }

    public ConfigurableFaults resume() {
        lock.lock();
        try {
            paused = false;
            recompute();
            resumed.signalAll();
        } finally {
            lock.unlock();
        }
        return this;
    }

    /** Switches every fault off (and resumes a paused broker). */
    public ConfigurableFaults clear() {
        synchronized (this) {
            fsyncDelayNanos = 0;
            dropProbability = 0;
            peerDelayMillis = 0;
        }
        return resume();
    }

    public boolean isPaused() {
        return paused;
    }

    public long droppedMessages() {
        return dropped.get();
    }

    public long delayedMessages() {
        return delayed.get();
    }

    @Override
    public boolean active() {
        return active;
    }

    @Override
    public long fsyncDelayNanos() {
        return fsyncDelayNanos;
    }

    @Override
    public boolean dropPeerMessage(int toNode) {
        double p = dropProbability;
        if (p <= 0) {
            return false;
        }
        boolean drop;
        synchronized (this) {
            drop = random.nextDouble() < p;
        }
        if (drop) {
            dropped.incrementAndGet();
        }
        return drop;
    }

    @Override
    public long peerDelayMillis(int toNode) {
        long d = peerDelayMillis;
        if (d > 0) {
            delayed.incrementAndGet();
        }
        return d;
    }

    @Override
    public void awaitNotPaused() throws InterruptedException {
        if (!paused) {
            return;
        }
        lock.lock();
        try {
            while (paused) {
                resumed.await();
            }
        } finally {
            lock.unlock();
        }
    }
}
