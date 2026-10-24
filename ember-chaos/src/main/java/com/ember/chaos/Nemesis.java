package com.ember.chaos;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

/**
 * The fault injector: a seeded schedule of faults, each with a start and a stop. RULES: at most ONE broker-level fault is
 * active at a time (so a majority of brokers always exists), only fault types the driver supports are used, every start and
 * stop is written to the history, and healAll() puts everything back before the final verification. The seed reproduces the
 * SCHEDULE; real timing still varies between runs.
 */
public final class Nemesis {

    /** What the nemesis may do to the CLIENT side (the workload implements it). */
    public interface ClientFaults {
        /** Stops the heartbeats of one group consumer and returns its name, or null if there is none. */
        String pauseSomeConsumer(Random random);

        void resumeConsumers();
    }

    private final FaultDriver driver;
    private final HistoryRecorder recorder;
    private final Random random;
    private final List<Fault> allowed = new ArrayList<>();
    private final ClientFaults client;
    private final long minGapMs;
    private final long maxGapMs;
    private final List<String> schedule = new ArrayList<>();

    private Fault active;
    private int target;
    private int target2;
    private boolean clientPaused;

    public Nemesis(FaultDriver driver, HistoryRecorder recorder, long seed, Set<Fault> wanted, ClientFaults client, long minGapMs, long maxGapMs) {
        this.driver = driver;
        this.recorder = recorder;
        this.random = new Random(seed);
        this.client = client;
        this.minGapMs = minGapMs;
        this.maxGapMs = maxGapMs;
        for (Fault f : wanted) {
            if (f == Fault.CLIENT_PAUSE ? client != null : driver.supports(f)) allowed.add(f);
        }
        if (allowed.isEmpty()) {
            throw new IllegalArgumentException("none of the requested faults is supported by this driver/workload");
        }
    }

    public List<String> schedule() {
        return List.copyOf(schedule);
    }

    /** Runs the schedule for the given time. Always returns with the faults still possibly active: call healAll(). */
    public void runFor(long durationMs) {
        long end = System.nanoTime() + durationMs * 1_000_000L;
        while (System.nanoTime() < end) {
            long gapNanos = (minGapMs + (long) (random.nextDouble() * (maxGapMs - minGapMs))) * 1_000_000L;
            long remaining = end - System.nanoTime();
            if (gapNanos >= remaining) {                          // the next action would fall after the end of the window: stop waiting there
                LockSupport.parkNanos(Math.max(0, remaining));
                break;
            }
            LockSupport.parkNanos(gapNanos);
            try {
                step();
            } catch (Exception e) {
                schedule.add("action failed: " + e);
            }
        }
    }

    private void step() throws Exception {
        if (active != null) {
            if (random.nextInt(10) < 6) stop();
            return;
        }
        Fault next = allowed.get(random.nextInt(allowed.size()));
        int n = driver.brokers();
        switch (next) {
            case CRASH_RESTART -> {
                target = 1 + random.nextInt(n);
                begin(next, "broker " + target);
                driver.crash(target);
            }
            case PAUSE -> {
                target = 1 + random.nextInt(n);
                begin(next, "broker " + target);
                driver.pause(target);
            }
            case QUEUE_LEADER_CRASH -> {
                int leader = driver.queueLeader();
                target = leader > 0 ? leader : 1 + random.nextInt(n);
                begin(next, "broker " + target + (leader > 0 ? " (the __acks leader)" : ""));
                driver.crash(target);
            }
            case ISOLATE -> {
                target = 1 + random.nextInt(n);
                begin(next, "broker " + target);
                driver.isolate(target);
            }
            case LINK -> {
                target = 1 + random.nextInt(n);
                target2 = 1 + (target % n);
                begin(next, "link " + target + "<->" + target2);
                driver.blockLink(target, target2);
            }
            case SLOW_FSYNC -> {
                target = 1 + random.nextInt(n);
                begin(next, "broker " + target);
                driver.slowFsync(target, 20 + random.nextInt(60));
            }
            case DROP -> {
                target = 1 + random.nextInt(n);
                begin(next, "broker " + target);
                driver.dropMessages(target, 0.2 + random.nextDouble() * 0.4, random.nextLong());
            }
            case DELAY -> {
                target = 1 + random.nextInt(n);
                begin(next, "broker " + target);
                driver.delayMessages(target, 20 + random.nextInt(60));    // below the election timeout: slow, not dead
            }
            case CLIENT_PAUSE -> {
                String who = client.pauseSomeConsumer(random);
                if (who != null) {
                    clientPaused = true;
                    begin(next, who);
                }
            }
        }
    }

    private void begin(Fault fault, String what) {
        active = fault;
        schedule.add("start " + fault.label() + " " + what);
        recorder.nemesis(fault.label(), what, "start");
    }

    private void stop() throws Exception {
        Fault f = active;
        switch (f) {
            case CRASH_RESTART, QUEUE_LEADER_CRASH -> driver.restart(target);
            case PAUSE -> driver.resume(target);
            case ISOLATE -> driver.heal();
            case LINK -> driver.unblockLink(target, target2);
            case SLOW_FSYNC, DROP, DELAY -> driver.clearSoftFaults(target);
            case CLIENT_PAUSE -> {
                client.resumeConsumers();
                clientPaused = false;
            }
        }
        active = null;
        schedule.add("stop " + f.label());
        recorder.nemesis(f.label(), "", "stop");
    }

    /** Restores everything and records the heal. */
    public void healAll() throws Exception {
        if (client != null && clientPaused) {
            client.resumeConsumers();
            clientPaused = false;
        }
        driver.healAll();
        if (active != null) {
            recorder.nemesis(active.label(), "", "stop");
            active = null;
        }
        schedule.add("HEAL");
    }
}
