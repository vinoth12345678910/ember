package com.ember.replication;

import com.ember.protocol.ErrorCode;
import com.ember.protocol.QueueDeadListResponse;
import com.ember.protocol.QueueDescribeResponse;
import com.ember.protocol.QueueHandle;
import com.ember.protocol.QueueMessage;
import com.ember.storage.LogRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.LongSupplier;

/**
 * The logic that runs on the leader of the "__acks" Raft group: it plans which messages to hand out, reads them from this broker's
 * copy of the partition (every broker holds every partition), proposes the claim through Raft, and only then returns them.
 *
 * DURABLE state (progress, leases) lives in the Raft log and is read from the {@link AckStateMachine}. The leader COMPUTES, the state
 * machine only INSTALLS: a claim is planned from the applied state and guarded by what it saw, so a plan that raced with an ack
 * or another claim is refused whole (STALE) and re-planned.
 *
 * Lease deadlines are wall-clock milliseconds written into the commands. If clocks disagree a message is redelivered a little early
 * or late; nothing else depends on them.
 */
public final class AckCoordinator {

    /** What the coordinator needs to know about the topics and their data on THIS broker. */
    public interface QueueData {
        /** Partition count of the topic, or -1 if it does not exist (here, yet). */
        int partitionCount(String topic);

        /** First offset still in the log. */
        long logStart(String topic, int partition);

        /** Next offset to be written: everything below it is committed and applied on this broker. */
        long logEnd(String topic, int partition);

        /** The record at that offset, or null if the log no longer has it. */
        LogRecord read(String topic, int partition, long offset) throws IOException;
    }

    public record Config(int defaultLeaseMs, long proposeTimeoutMs, int maxCasAttempts, int maxClaim) {
        public Config {
            if (defaultLeaseMs < 1 || proposeTimeoutMs < 1 || maxCasAttempts < 1 || maxClaim < 1) {
                throw new IllegalArgumentException("bad ack coordinator config");
            }
        }
    }

    public record DeadListResult(ErrorCode error, int total, List<QueueDeadListResponse.DeadMessage> items) {
    }

    public record CreateResult(ErrorCode error, int partitions) {
    }

    public record ClaimResult(ErrorCode error, List<QueueMessage> messages) {
    }

    public record SettleResult(ErrorCode error, int settled, int rejected) {
    }

    public record DescribeResult(ErrorCode error, QueueDescribeResponse body) {
    }

    private static final System.Logger LOG = System.getLogger(AckCoordinator.class.getName());

    private final String nodeLabel;
    private final GroupLog log;
    private final AckStateMachine state;
    private final QueueData data;
    private final LongSupplier wallClock;
    private final Config config;

    private final Map<String, Object> claimLocks = new HashMap<>();             // one claim at a time per subscription
    private final Set<String> expiryInFlight = new HashSet<>();
    private final Map<String, Integer> rotation = new HashMap<>();              // which partition to look at first, per subscription

    public AckCoordinator(String nodeLabel, GroupLog log, AckStateMachine state, QueueData data, LongSupplier wallClock, Config config) {
        this.nodeLabel = nodeLabel;
        this.log = log;
        this.state = state;
        this.data = data;
        this.wallClock = wallClock;
        this.config = config;
    }

    private ErrorCode checkLeader() {
        if (!log.isLeader()) {
            return log.leaderId() >= 0 ? ErrorCode.NOT_QUEUE_LEADER : ErrorCode.QUEUE_LEADER_NOT_AVAILABLE;
        }
        return log.isLeaderReady() ? ErrorCode.NONE : ErrorCode.QUEUE_LEADER_NOT_AVAILABLE;
    }

    private synchronized Object lockFor(String subscription) {
        return claimLocks.computeIfAbsent(subscription, k -> new Object());
    }

    // ------------------------------------------------------------------ CREATE

    public CreateResult create(String subscription, String topic, boolean fromStart, int leaseMs) {
        return create(subscription, topic, fromStart, leaseMs, 0, 0, 0);
    }

    /** maxAttempts / backoff values of 0 mean the defaults. */
    public CreateResult create(String subscription, String topic, boolean fromStart, int leaseMs, int maxAttempts, int backoffBaseMs, int backoffMaxMs) {
        ErrorCode leader = checkLeader();
        if (leader != ErrorCode.NONE) {
            return new CreateResult(leader, 0);
        }
        int partitions = data.partitionCount(topic);
        if (partitions < 1) {
            return new CreateResult(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, 0);
        }
        if (state.view(subscription) != null) {
            return new CreateResult(ErrorCode.SUBSCRIPTION_ALREADY_EXISTS, 0);
        }
        List<Long> starts = new ArrayList<>(partitions);
        for (int p = 0; p < partitions; p++) {
            starts.add(fromStart ? data.logStart(topic, p) : data.logEnd(topic, p));
        }
        int base = backoffBaseMs > 0 ? backoffBaseMs : AckCommands.DEFAULT_BACKOFF_BASE_MS;
        int max = backoffMaxMs > 0 ? backoffMaxMs : Math.max(AckCommands.DEFAULT_BACKOFF_MAX_MS, base);
        Object result = proposeAndWait(AckCommands.encode(new AckCommands.Create(subscription, topic, leaseMs > 0 ? leaseMs : config.defaultLeaseMs(), starts,
                maxAttempts > 0 ? maxAttempts : AckCommands.DEFAULT_MAX_ATTEMPTS, base, max)));
        if (result == AckStateMachine.Result.OK) {
            return new CreateResult(ErrorCode.NONE, partitions);
        }
        return new CreateResult(errorOf(result), 0);
    }

    public ErrorCode delete(String subscription) {
        ErrorCode leader = checkLeader();
        if (leader != ErrorCode.NONE) {
            return leader;
        }
        Object result = proposeAndWait(AckCommands.encode(new AckCommands.Delete(subscription)));
        return result == AckStateMachine.Result.OK ? ErrorCode.NONE : errorOf(result);
    }

    // ------------------------------------------------------------------ CLAIM

    /** One planned handout from one partition, with its records already read. */
    private record Planned(AckCommands.Allotment allotment, List<QueueMessage> messages, List<QueueHandle> lostToRetention) {
    }

    public ClaimResult claim(String subscription, String worker, int maxMessages, int leaseMs, int maxBytes) {
        ErrorCode leader = checkLeader();
        if (leader != ErrorCode.NONE) {
            return new ClaimResult(leader, List.of());
        }
        AckStateMachine.SubscriptionView first = state.view(subscription);
        if (first == null) {
            return new ClaimResult(ErrorCode.UNKNOWN_SUBSCRIPTION, List.of());
        }
        List<QueueMessage> got = new ArrayList<>();
        synchronized (lockFor(subscription)) {
            int wanted = Math.min(maxMessages, config.maxClaim());
            long bytesLeft = maxBytes;
            for (int attempt = 0; attempt < config.maxCasAttempts() && got.size() < wanted && bytesLeft > 0; attempt++) {
                AckStateMachine.SubscriptionView view = state.view(subscription);
                if (view == null) {
                    return new ClaimResult(got.isEmpty() ? ErrorCode.UNKNOWN_SUBSCRIPTION : ErrorCode.NONE, got);
                }
                long lease = leaseMs > 0 ? leaseMs : view.leaseMs();
                List<Planned> plan;
                try {
                    plan = plan(subscription, view, worker, wanted - got.size(), bytesLeft, wallClock.getAsLong() + lease);
                } catch (IOException e) {
                    LOG.log(System.Logger.Level.WARNING, nodeLabel + ": reading records for a claim failed", e);
                    return new ClaimResult(got.isEmpty() ? ErrorCode.INTERNAL_ERROR : ErrorCode.NONE, got);
                }
                if (plan.isEmpty()) {
                    break;                                                          // nothing to hand out right now
                }
                List<AckCommands.Allotment> allotments = new ArrayList<>();
                for (Planned p : plan) {
                    allotments.add(p.allotment());
                }
                Object result = proposeAndWait(AckCommands.encode(new AckCommands.Claim(subscription, allotments)));
                if (!(result instanceof List<?> results)) {
                    return new ClaimResult(got.isEmpty() ? errorOf(result) : ErrorCode.NONE, got);   // what was claimed earlier stays claimed: hand it out
                }
                boolean anyStale = false;
                for (int i = 0; i < plan.size(); i++) {
                    Object r = results.get(i);
                    if (r == AckStateMachine.Result.OK) {
                        for (QueueMessage m : plan.get(i).messages()) {
                            got.add(m);
                            bytesLeft -= (m.key() == null ? 0 : m.key().length) + m.value().length + 32;
                        }
                        finishLost(subscription, worker, plan.get(i).lostToRetention());
                    } else if (r == AckStateMachine.Result.STALE) {
                        anyStale = true;
                    }
                }
                if (!anyStale) {
                    break;
                }
            }
        }
        return new ClaimResult(ErrorCode.NONE, got);
    }

    /** Builds the allotments for up to {@code remaining} messages and reads their records. Touches no state. */
    private List<Planned> plan(String subscription, AckStateMachine.SubscriptionView view, String worker, int remaining, long bytesLeft,
                               long deadline) throws IOException {
        List<Planned> out = new ArrayList<>();
        int partitions = view.partitions().size();
        int startAt;
        synchronized (this) {
            startAt = rotation.merge(subscription, 1, Integer::sum) % partitions;
        }
        long bytes = 0;
        for (int i = 0; i < partitions && remaining > 0 && bytes < bytesLeft; i++) {
            int p = (startAt + i) % partitions;
            AckStateMachine.PartitionView pv = view.partitions().get(p);
            long logStart = data.logStart(view.topic(), p);
            long logEnd = data.logEnd(view.topic(), p);
            List<QueueMessage> messages = new ArrayList<>();
            List<QueueHandle> lost = new ArrayList<>();
            List<Long> redeliverOffsets = new ArrayList<>();

            for (AckStateMachine.Redelivery r : state.redeliveries(subscription, p, remaining, wallClock.getAsLong())) {
                if (remaining <= 0 || bytes >= bytesLeft) {
                    break;
                }
                int attempt = r.attempts() + 1;
                redeliverOffsets.add(r.offset());
                LogRecord rec = r.offset() >= logStart && r.offset() < logEnd ? data.read(view.topic(), p, r.offset()) : null;
                if (rec == null) {
                    lost.add(new QueueHandle(p, r.offset(), attempt));
                } else {
                    messages.add(new QueueMessage(p, rec.offset(), attempt, rec.timestamp(), rec.key(), rec.value()));
                    bytes += size(rec);
                }
                remaining--;
            }
            long freshFrom = Math.max(pv.next(), logStart);
            int freshCount = 0;
            while (remaining > 0 && bytes < bytesLeft && freshFrom + freshCount < logEnd) {
                LogRecord rec = data.read(view.topic(), p, freshFrom + freshCount);
                if (rec == null) {
                    break;                                                          // raced with retention: stop here, the next claim moves on
                }
                messages.add(new QueueMessage(p, rec.offset(), 1, rec.timestamp(), rec.key(), rec.value()));
                bytes += size(rec);
                freshCount++;
                remaining--;
            }
            if (!redeliverOffsets.isEmpty() || freshCount > 0) {
                out.add(new Planned(new AckCommands.Allotment(p, worker, deadline, freshFrom, freshCount, redeliverOffsets), messages, lost));
            }
        }
        return out;
    }

    private static long size(LogRecord r) {
        return (r.key() == null ? 0 : r.key().length) + r.value().length + 32;
    }

    /** Messages the log no longer holds cannot be delivered: finish them on the worker's behalf so they do not wait for ever. */
    private void finishLost(String subscription, String worker, List<QueueHandle> lost) {
        if (lost.isEmpty()) {
            return;
        }
        LOG.log(System.Logger.Level.WARNING, "{0}: subscription {1}: {2} message(s) were removed by retention before delivery", nodeLabel, subscription, lost.size());
        proposeAndWait(AckCommands.encode(new AckCommands.Settle(AckCommands.ACK, subscription, worker, 0, lost)));
    }

    // ------------------------------------------------------------------ ACK / RELEASE / EXTEND

    public SettleResult settle(byte type, String subscription, String worker, int leaseMs, List<QueueHandle> handles) {
        ErrorCode leader = checkLeader();
        if (leader != ErrorCode.NONE) {
            return new SettleResult(leader, 0, 0);
        }
        AckStateMachine.SubscriptionView view = state.view(subscription);
        if (view == null) {
            return new SettleResult(ErrorCode.UNKNOWN_SUBSCRIPTION, 0, 0);
        }
        long deadline = type == AckCommands.EXTEND ? wallClock.getAsLong() + (leaseMs > 0 ? leaseMs : view.leaseMs()) : 0;
        if (handles.isEmpty()) {
            return new SettleResult(ErrorCode.NONE, 0, 0);
        }
        Object result = proposeAndWait(AckCommands.encode(new AckCommands.Settle(type, subscription, worker, deadline, handles)));
        if (result instanceof AckStateMachine.SettleOutcome o) {
            return new SettleResult(ErrorCode.NONE, o.settled(), o.rejected());
        }
        return new SettleResult(errorOf(result), 0, 0);
    }

    // ------------------------------------------------------------------ NACK and the dead-letter set

    /** The worker could not process these: retry after a backoff, or dead-letter once the attempts are used up. */
    public SettleResult nack(String subscription, String worker, String reason, List<QueueHandle> handles) {
        ErrorCode leader = checkLeader();
        if (leader != ErrorCode.NONE) {
            return new SettleResult(leader, 0, 0);
        }
        if (state.view(subscription) == null) {
            return new SettleResult(ErrorCode.UNKNOWN_SUBSCRIPTION, 0, 0);
        }
        if (handles.isEmpty()) {
            return new SettleResult(ErrorCode.NONE, 0, 0);
        }
        Object result = proposeAndWait(AckCommands.encode(new AckCommands.Nack(subscription, worker, wallClock.getAsLong(), reason, handles)));
        return result instanceof AckStateMachine.SettleOutcome o ? new SettleResult(ErrorCode.NONE, o.settled(), o.rejected())
                : new SettleResult(errorOf(result), 0, 0);
    }

    /** The dead-lettered messages with their content (empty content if retention already removed the record). */
    public DeadListResult deadList(String subscription, int max) {
        ErrorCode leader = checkLeader();
        if (leader != ErrorCode.NONE) {
            return new DeadListResult(leader, 0, List.of());
        }
        AckStateMachine.SubscriptionView v = state.view(subscription);
        if (v == null) {
            return new DeadListResult(ErrorCode.UNKNOWN_SUBSCRIPTION, 0, List.of());
        }
        List<QueueDeadListResponse.DeadMessage> items = new ArrayList<>();
        for (AckStateMachine.DeadView d : state.deadLetters(subscription, max)) {
            LogRecord rec = null;
            try {
                rec = data.read(v.topic(), d.partition(), d.offset());
            } catch (IOException e) {
                LOG.log(System.Logger.Level.WARNING, nodeLabel + ": reading a dead message failed", e);
            }
            items.add(new QueueDeadListResponse.DeadMessage(d.partition(), d.offset(), d.attempts(), d.failures(), d.deadAt(), d.reason(),
                    rec == null ? null : rec.key(), rec == null ? new byte[0] : rec.value()));
        }
        return new DeadListResult(ErrorCode.NONE, state.deadCount(subscription), items);
    }

    /** type is AckCommands.DEAD_RETRY (back into the queue with a fresh failure count) or DEAD_DISCARD (finished for good). */
    public SettleResult deadAction(byte type, String subscription, List<QueueHandle> handles) {
        ErrorCode leader = checkLeader();
        if (leader != ErrorCode.NONE) {
            return new SettleResult(leader, 0, 0);
        }
        if (state.view(subscription) == null) {
            return new SettleResult(ErrorCode.UNKNOWN_SUBSCRIPTION, 0, 0);
        }
        if (handles.isEmpty()) {
            return new SettleResult(ErrorCode.NONE, 0, 0);
        }
        Object result = proposeAndWait(AckCommands.encode(new AckCommands.DeadAction(type, subscription, handles)));
        return result instanceof AckStateMachine.SettleOutcome o ? new SettleResult(ErrorCode.NONE, o.settled(), o.rejected())
                : new SettleResult(errorOf(result), 0, 0);
    }

    // ------------------------------------------------------------------ DESCRIBE

    public DescribeResult describe(String subscription) {
        ErrorCode leader = checkLeader();
        if (leader != ErrorCode.NONE) {
            return new DescribeResult(leader, null);
        }
        AckStateMachine.SubscriptionView v = state.view(subscription);
        if (v == null) {
            return new DescribeResult(ErrorCode.UNKNOWN_SUBSCRIPTION, null);
        }
        List<QueueDescribeResponse.PartitionState> parts = new ArrayList<>();
        for (AckStateMachine.PartitionView p : v.partitions()) {
            parts.add(new QueueDescribeResponse.PartitionState(p.partition(), p.floor(), p.next(), data.logEnd(v.topic(), p.partition()),
                    p.inFlight(), p.redeliver(), p.doneAbove(), p.dead()));
        }
        return new DescribeResult(ErrorCode.NONE, new QueueDescribeResponse(v.topic(), v.leaseMs(), v.maxAttempts(), v.backoffBaseMs(), v.backoffMaxMs(), parts));
    }

    // ------------------------------------------------------------------ lease expiry

    /** Proposes EXPIRE for every lease past its deadline. Does not block; the owner calls it every few hundred milliseconds. */
    public void expireLeases() {
        if (checkLeader() != ErrorCode.NONE) {
            return;
        }
        long now = wallClock.getAsLong();
        for (Map.Entry<String, List<QueueHandle>> e : state.expiredLeases(now, 2000).entrySet()) {
            String sub = e.getKey();
            synchronized (this) {
                if (!expiryInFlight.add(sub)) {
                    continue;
                }
            }
            CompletableFuture<Object> future;
            try {
                future = log.propose(AckCommands.encode(new AckCommands.Expire(sub, now, e.getValue())), config.proposeTimeoutMs());
            } catch (RuntimeException ex) {
                finishExpiry(sub);
                continue;
            }
            future.whenComplete((result, error) -> finishExpiry(sub));          // a failed attempt is simply redone on the next tick
        }
    }

    private synchronized void finishExpiry(String sub) {
        expiryInFlight.remove(sub);
    }

    // ------------------------------------------------------------------ helpers

    private Object proposeAndWait(byte[] command) {
        try {
            return log.propose(command, config.proposeTimeoutMs()).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ErrorCode.QUEUE_LEADER_NOT_AVAILABLE;
        } catch (ExecutionException e) {
            return errorOfFailure(e.getCause());
        } catch (RuntimeException e) {
            return errorOfFailure(e);
        }
    }

    private ErrorCode errorOfFailure(Throwable cause) {
        if (cause instanceof GroupRuntime.NotLeaderException) {
            return log.leaderId() >= 0 ? ErrorCode.NOT_QUEUE_LEADER : ErrorCode.QUEUE_LEADER_NOT_AVAILABLE;
        }
        if (cause instanceof GroupRuntime.ProposeTimeoutException) {
            return ErrorCode.REQUEST_TIMED_OUT;                                // ambiguous: it may still commit (leases then simply expire)
        }
        if (cause instanceof GroupRuntime.OverloadedException) {
            return ErrorCode.BROKER_OVERLOADED;
        }
        LOG.log(System.Logger.Level.WARNING, nodeLabel + ": ack proposal failed", cause);
        return ErrorCode.INTERNAL_ERROR;
    }

    private static ErrorCode errorOf(Object result) {
        if (result instanceof ErrorCode e) {
            return e;
        }
        if (result instanceof AckStateMachine.Result r) {
            return switch (r) {
                case OK -> ErrorCode.NONE;
                case UNKNOWN_SUBSCRIPTION -> ErrorCode.UNKNOWN_SUBSCRIPTION;
                case ALREADY_EXISTS -> ErrorCode.SUBSCRIPTION_ALREADY_EXISTS;
                case STALE -> ErrorCode.REQUEST_TIMED_OUT;
                case INVALID -> ErrorCode.INVALID_REQUEST;
            };
        }
        return ErrorCode.INTERNAL_ERROR;
    }
}
