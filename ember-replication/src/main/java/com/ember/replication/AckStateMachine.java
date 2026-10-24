package com.ember.replication;

import com.ember.protocol.QueueHandle;
import com.ember.raft.EntryType;
import com.ember.raft.LogEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The state machine of the "__acks" Raft group: per subscription and partition, which messages are finished, which are held by a
 * worker (a "lease"), and which wait to be handed out again.
 *
 * The records themselves never move: they stay in the partition's data log and are referred to by offset.
 *
 * apply() is a PURE function of (state, command): no clock, no randomness, no I/O, no peeking at other Raft groups. Times
 * (lease deadlines, "now" of an expiry) are numbers the leader wrote into the command, so every replica reaches the same state.
 * Wrong clocks can therefore only make a redelivery early or late, never break a safety property.
 *
 * Per partition: every offset below {@code floor} is finished; {@code done} holds finished offsets at or above the floor as ranges
 * (out-of-order acks); {@code next} is the first offset never handed out; {@code leases} are messages being processed; {@code redeliver}
 * are messages whose lease expired or were released (value = the attempt that last held them).
 *
 * Readers (the coordinator, DESCRIBE, tests) may run on other threads: every public method is synchronized.
 */
public final class AckStateMachine implements StateMachine {

    public enum Result { OK, UNKNOWN_SUBSCRIPTION, ALREADY_EXISTS, STALE, INVALID }

    /** What ACK / RELEASE / EXTEND / EXPIRE answer. */
    public record SettleOutcome(int settled, int rejected) {
    }

    record Lease(String worker, int attempt, long deadline) {
    }

    private static final class PartitionQueue {
        long floor;
        long next;
        final TreeMap<Long, Long> done = new TreeMap<>();            // start -> end (exclusive); ranges are disjoint, non-adjacent, all above floor
        final TreeMap<Long, Lease> leases = new TreeMap<>();
        final TreeMap<Long, Integer> redeliver = new TreeMap<>();

        PartitionQueue(long start) {
            floor = start;
            next = start;
        }

        boolean finished(long offset) {
            if (offset < floor) {
                return true;
            }
            Map.Entry<Long, Long> r = done.floorEntry(offset);
            return r != null && offset < r.getValue();
        }

        /** Marks [from, to) finished and advances the floor over everything contiguous. */
        void addDone(long from, long to) {
            from = Math.max(from, floor);
            if (to <= from) {
                return;
            }
            Map.Entry<Long, Long> lo = done.floorEntry(from);
            if (lo != null && lo.getValue() >= from) {
                from = lo.getKey();
                to = Math.max(to, lo.getValue());
                done.remove(lo.getKey());
            }
            Map.Entry<Long, Long> hi = done.ceilingEntry(from);
            while (hi != null && hi.getKey() <= to) {
                to = Math.max(to, hi.getValue());
                done.remove(hi.getKey());
                hi = done.ceilingEntry(from);
            }
            if (from <= floor) {
                floor = to;
            } else {
                done.put(from, to);
            }
        }

        long doneAbove() {
            long n = 0;
            for (Map.Entry<Long, Long> r : done.entrySet()) {
                n += r.getValue() - r.getKey();
            }
            return n;
        }
    }

    private static final class Subscription {
        final String topic;
        final int leaseMs;
        final PartitionQueue[] partitions;

        Subscription(String topic, int leaseMs, List<Long> starts) {
            this.topic = topic;
            this.leaseMs = leaseMs;
            this.partitions = new PartitionQueue[starts.size()];
            for (int i = 0; i < partitions.length; i++) {
                partitions[i] = new PartitionQueue(starts.get(i));
            }
        }
    }

    /** An immutable snapshot of one partition's progress. */
    public record PartitionView(int partition, long floor, long next, int inFlight, int redeliver, long doneAbove) {
    }

    public record SubscriptionView(String name, String topic, int leaseMs, List<PartitionView> partitions) {
    }

    /** A message waiting to be handed out again, with the attempt that last held it. */
    public record Redelivery(long offset, int attempts) {
    }

    private final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();

    @Override
    public synchronized Object apply(long index, LogEntry entry) {
        if (entry.type() == EntryType.NOOP) {
            return null;
        }
        byte[] command = entry.command();
        try {
            return switch (AckCommands.type(command)) {
                case AckCommands.CREATE -> create(AckCommands.decodeCreate(command));
                case AckCommands.CLAIM -> claim(AckCommands.decodeClaim(command));
                case AckCommands.ACK, AckCommands.RELEASE, AckCommands.EXTEND -> settle(AckCommands.decodeSettle(command));
                case AckCommands.EXPIRE -> expire(AckCommands.decodeExpire(command));
                case AckCommands.DELETE -> delete(AckCommands.decodeDelete(command));
                default -> throw new IllegalStateException("unknown __acks command type " + AckCommands.type(command));
            };
        } catch (IllegalArgumentException malformed) {
            return Result.INVALID;                           // identical on every replica, changes nothing
        }
    }

    private Result create(AckCommands.Create c) {
        if (subscriptions.containsKey(c.subscription())) {
            return Result.ALREADY_EXISTS;
        }
        if (c.leaseMs() < 1 || c.starts().isEmpty() || c.starts().stream().anyMatch(s -> s < 0)) {
            return Result.INVALID;
        }
        subscriptions.put(c.subscription(), new Subscription(c.topic(), c.leaseMs(), c.starts()));
        return Result.OK;
    }

    private Result delete(AckCommands.Delete d) {
        return subscriptions.remove(d.subscription()) == null ? Result.UNKNOWN_SUBSCRIPTION : Result.OK;
    }

    /** One Result per allotment, in order. An allotment is applied whole or not at all. */
    private Object claim(AckCommands.Claim c) {
        Subscription sub = subscriptions.get(c.subscription());
        if (sub == null) {
            return Result.UNKNOWN_SUBSCRIPTION;
        }
        List<Result> results = new ArrayList<>(c.allotments().size());
        for (AckCommands.Allotment a : c.allotments()) {
            results.add(claimOne(sub, a));
        }
        return List.copyOf(results);
    }

    private Result claimOne(Subscription sub, AckCommands.Allotment a) {
        if (a.partition() < 0 || a.partition() >= sub.partitions.length || a.worker().isEmpty() || a.deadline() <= 0 || a.freshCount() < 0
                || (a.freshCount() == 0 && a.redeliver().isEmpty())) {
            return Result.INVALID;
        }
        PartitionQueue pq = sub.partitions[a.partition()];
        if (a.freshCount() > 0 && a.freshFrom() < pq.next) {
            return Result.STALE;                                        // someone else was handed these offsets first
        }
        for (long o : a.redeliver()) {
            if (!pq.redeliver.containsKey(o)) {
                return Result.STALE;                                    // acked or claimed since the leader looked
            }
        }
        for (long o : a.redeliver()) {
            int attempts = pq.redeliver.remove(o);
            pq.leases.put(o, new Lease(a.worker(), attempts + 1, a.deadline()));
        }
        if (a.freshCount() > 0) {
            if (a.freshFrom() > pq.next) {
                pq.addDone(pq.next, a.freshFrom());                      // offsets the log no longer holds (retention): nothing to deliver
            }
            for (long o = a.freshFrom(); o < a.freshFrom() + a.freshCount(); o++) {
                pq.leases.put(o, new Lease(a.worker(), 1, a.deadline()));
            }
            pq.next = a.freshFrom() + a.freshCount();
        }
        return Result.OK;
    }

    private Object settle(AckCommands.Settle s) {
        Subscription sub = subscriptions.get(s.subscription());
        if (sub == null) {
            return Result.UNKNOWN_SUBSCRIPTION;
        }
        int settled = 0, rejected = 0;
        for (QueueHandle h : s.handles()) {
            PartitionQueue pq = h.partition() >= 0 && h.partition() < sub.partitions.length ? sub.partitions[h.partition()] : null;
            if (pq == null || h.offset() < 0) {
                rejected++;
                continue;
            }
            boolean ok = switch (s.type()) {
                case AckCommands.ACK -> ack(pq, s.worker(), h);
                case AckCommands.RELEASE -> release(pq, s.worker(), h);
                default -> extend(pq, s.worker(), h, s.deadline());
            };
            if (ok) {
                settled++;
            } else {
                rejected++;
            }
        }
        return new SettleOutcome(settled, rejected);
    }

    private static boolean holds(Lease l, String worker, int attempt) {
        return l != null && l.worker().equals(worker) && l.attempt() == attempt;
    }

    private boolean ack(PartitionQueue pq, String worker, QueueHandle h) {
        if (pq.finished(h.offset())) {
            return true;                                                // idempotent: acking twice is fine
        }
        if (holds(pq.leases.get(h.offset()), worker, h.attempt())) {
            pq.leases.remove(h.offset());
            pq.addDone(h.offset(), h.offset() + 1);
            return true;
        }
        Integer waiting = pq.redeliver.get(h.offset());                 // lease expired but nobody took it yet: the work WAS done
        if (waiting != null && waiting == h.attempt()) {
            pq.redeliver.remove(h.offset());
            pq.addDone(h.offset(), h.offset() + 1);
            return true;
        }
        return false;                                                   // fenced: another attempt owns it
    }

    private boolean release(PartitionQueue pq, String worker, QueueHandle h) {
        if (holds(pq.leases.get(h.offset()), worker, h.attempt())) {
            pq.leases.remove(h.offset());
            pq.redeliver.put(h.offset(), h.attempt());
            return true;
        }
        Integer waiting = pq.redeliver.get(h.offset());
        return waiting != null && waiting == h.attempt();               // already released: idempotent
    }

    private boolean extend(PartitionQueue pq, String worker, QueueHandle h, long deadline) {
        Lease l = pq.leases.get(h.offset());
        if (!holds(l, worker, h.attempt())) {
            return false;
        }
        if (deadline > l.deadline()) {
            pq.leases.put(h.offset(), new Lease(l.worker(), l.attempt(), deadline));
        }
        return true;
    }

    private Object expire(AckCommands.Expire e) {
        Subscription sub = subscriptions.get(e.subscription());
        if (sub == null) {
            return Result.UNKNOWN_SUBSCRIPTION;
        }
        int expired = 0, skipped = 0;
        for (QueueHandle h : e.handles()) {
            PartitionQueue pq = h.partition() >= 0 && h.partition() < sub.partitions.length ? sub.partitions[h.partition()] : null;
            Lease l = pq == null ? null : pq.leases.get(h.offset());
            if (l != null && l.attempt() == h.attempt() && l.deadline() <= e.now()) {
                pq.leases.remove(h.offset());
                pq.redeliver.put(h.offset(), l.attempt());
                expired++;
            } else {
                skipped++;                                              // acked, extended or re-claimed since the leader looked
            }
        }
        return new SettleOutcome(expired, skipped);
    }

    // ------------------------------------------------------------------ readers

    public synchronized List<String> names() {
        return new ArrayList<>(subscriptions.keySet());
    }

    public synchronized SubscriptionView view(String name) {
        Subscription s = subscriptions.get(name);
        if (s == null) {
            return null;
        }
        List<PartitionView> parts = new ArrayList<>(s.partitions.length);
        for (int i = 0; i < s.partitions.length; i++) {
            PartitionQueue pq = s.partitions[i];
            parts.add(new PartitionView(i, pq.floor, pq.next, pq.leases.size(), pq.redeliver.size(), pq.doneAbove()));
        }
        return new SubscriptionView(name, s.topic, s.leaseMs, parts);
    }

    /** Up to max messages waiting to be handed out again, lowest offset first. */
    public synchronized List<Redelivery> redeliveries(String name, int partition, int max) {
        Subscription s = subscriptions.get(name);
        List<Redelivery> out = new ArrayList<>();
        if (s == null || partition < 0 || partition >= s.partitions.length) {
            return out;
        }
        for (Map.Entry<Long, Integer> e : s.partitions[partition].redeliver.entrySet()) {
            if (out.size() >= max) {
                break;
            }
            out.add(new Redelivery(e.getKey(), e.getValue()));
        }
        return out;
    }

    /** Every lease whose deadline is at or before now, grouped by subscription (what the leader proposes EXPIRE for). */
    public synchronized Map<String, List<QueueHandle>> expiredLeases(long now, int maxPerSubscription) {
        Map<String, List<QueueHandle>> out = new TreeMap<>();
        for (Map.Entry<String, Subscription> se : subscriptions.entrySet()) {
            List<QueueHandle> list = new ArrayList<>();
            Subscription s = se.getValue();
            for (int p = 0; p < s.partitions.length && list.size() < maxPerSubscription; p++) {
                for (Map.Entry<Long, Lease> le : s.partitions[p].leases.entrySet()) {
                    if (le.getValue().deadline() <= now) {
                        list.add(new QueueHandle(p, le.getKey(), le.getValue().attempt()));
                        if (list.size() >= maxPerSubscription) {
                            break;
                        }
                    }
                }
            }
            if (!list.isEmpty()) {
                out.put(se.getKey(), list);
            }
        }
        return out;
    }

    /** Test hook: a canonical text form of the whole state, to compare replicas. */
    public synchronized String fingerprint() {
        StringBuilder b = new StringBuilder();
        new TreeMap<>(subscriptions).forEach((name, s) -> {
            b.append(name).append('|').append(s.topic).append('|').append(s.leaseMs);
            for (PartitionQueue pq : s.partitions) {
                b.append("|f").append(pq.floor).append("n").append(pq.next).append("d").append(pq.done).append("l").append(pq.leases).append("r").append(pq.redeliver);
            }
            b.append('\n');
        });
        return b.toString();
    }
}
