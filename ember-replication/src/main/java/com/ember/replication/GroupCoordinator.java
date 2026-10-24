package com.ember.replication;

import com.ember.protocol.DescribeGroupResponse;
import com.ember.protocol.ErrorCode;
import com.ember.protocol.PartitionOffset;
import com.ember.protocol.PartitionRef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * The coordinator logic that runs on the leader of the "__groups" Raft group.
 *
 * DURABLE state (generation, members, assignments, offsets) lives in the Raft log and is read from the
 * GroupStateMachine. SOFT state (the time of each member's last heartbeat) lives here, in memory, and only on the
 * leader: heartbeats would flood the Raft log. A new leader starts every member's session fresh (full grace).
 *
 * The leader COMPUTES, the state machine only INSTALLS: membership changes are computed here, including the whole
 * assignment, and committed as one SET_MEMBERSHIP guarded by the generation it was based on.
 */
public final class GroupCoordinator {

    /** Partition count of a topic from the local topic list, or -1 if the topic does not exist (yet). */
    public interface TopicDirectory {
        int partitionCount(String topic);
    }

    /**
     * cooperative: partitions that stay with their owner are never interrupted by a rebalance. The assignment is sticky, and a
     * partition that must change owner is first REVOKED (unowned for one generation) and given to its new owner only after the
     * old owner has acknowledged by heartbeating the new generation. false = the eager behaviour: range assignment, everything
     * changes in one generation.
     */
    public record Config(int minSessionMs, int maxSessionMs, long proposeTimeoutMs, int maxCasAttempts, boolean cooperative) {
        public Config {
            if (minSessionMs < 1 || maxSessionMs < minSessionMs || proposeTimeoutMs < 1 || maxCasAttempts < 1) {
                throw new IllegalArgumentException("bad coordinator config");
            }
        }

        public Config(int minSessionMs, int maxSessionMs, long proposeTimeoutMs, int maxCasAttempts) {
            this(minSessionMs, maxSessionMs, proposeTimeoutMs, maxCasAttempts, false);
        }
    }

    public record JoinResult(ErrorCode error, int generation, String memberId, List<PartitionRef> assignment) {
        static JoinResult error(ErrorCode e) {
            return new JoinResult(e, 0, "", List.of());
        }
    }

    public record DescribeResult(ErrorCode error, DescribeGroupResponse body) {
    }

    public record FetchResult(ErrorCode error, List<PartitionOffset> offsets) {
    }

    private static final System.Logger LOG = System.getLogger(GroupCoordinator.class.getName());

    private final String nodeLabel;
    private final GroupLog log;
    private final GroupStateMachine state;
    private final TopicDirectory topics;
    private final LongSupplier clock;
    private final Supplier<String> idGenerator;
    private final Config config;

    // ---- soft state, guarded by "this"; never held while waiting for Raft
    private final Map<String, Long> lastHeartbeat = new HashMap<>();
    private final java.util.Set<String> expiryInFlight = new HashSet<>();
    /** group+member -> the generation the member last heartbeated successfully (= has processed that generation's revocations) */
    private final Map<String, Integer> ackedGeneration = new HashMap<>();
    private long epochTerm = -1;
    private long leaderSinceMs;
    private final Map<String, Long> rebalancesByGroup = new HashMap<>();

    public GroupCoordinator(String nodeLabel, GroupLog log, GroupStateMachine state, TopicDirectory topics, LongSupplier clock,
                            Supplier<String> idGenerator, Config config) {
        this.nodeLabel = nodeLabel;
        this.log = log;
        this.state = state;
        this.topics = topics;
        this.clock = clock;
        this.idGenerator = idGenerator;
        this.config = config;
    }

    // ------------------------------------------------------------------ leadership and soft state

    private ErrorCode checkLeader() {
        if (!log.isLeader()) {
            return log.leaderId() >= 0 ? ErrorCode.NOT_COORDINATOR : ErrorCode.COORDINATOR_NOT_AVAILABLE;
        }
        if (!log.isLeaderReady()) {
            return ErrorCode.COORDINATOR_NOT_AVAILABLE;
        }
        syncEpoch();
        return ErrorCode.NONE;
    }

    /**
     * A different term means this node may have been a follower in between, so its heartbeat memory is worthless.
     * Forget it and start every member's session now: a full grace period instead of a mass expiry.
     */
    private synchronized void syncEpoch() {
        long term = log.term();
        if (term != epochTerm) {
            epochTerm = term;
            lastHeartbeat.clear();
            ackedGeneration.clear();
            expiryInFlight.clear();
            leaderSinceMs = clock.getAsLong();
        }
    }

    private static String key(String groupId, String memberId) {
        return groupId + '\0' + memberId;
    }

    private synchronized void touch(String groupId, String memberId) {
        lastHeartbeat.put(key(groupId, memberId), clock.getAsLong());
    }

    private synchronized void forget(String groupId, String memberId) {
        lastHeartbeat.remove(key(groupId, memberId));
        ackedGeneration.remove(key(groupId, memberId));
    }

    private synchronized void acknowledge(String groupId, String memberId, int generation) {
        ackedGeneration.put(key(groupId, memberId), generation);
    }

    /** True when every member has heartbeated the group's CURRENT generation in this leader's tenure. */
    private synchronized boolean everyoneAcknowledged(String groupId, GroupStateMachine.GroupData group) {
        for (String member : group.members().keySet()) {
            Integer acked = ackedGeneration.get(key(groupId, member));
            if (acked == null || acked != group.generation()) {
                return false;
            }
        }
        return true;
    }

    private synchronized long lastSeen(String groupId, String memberId) {
        return lastHeartbeat.getOrDefault(key(groupId, memberId), leaderSinceMs);
    }

    public synchronized long rebalances() {
        long total = 0;
        for (long n : rebalancesByGroup.values()) total += n;
        return total;
    }

    /** Rebalances this coordinator performed for the group during its tenure (memory only; restarts after a failover). */
    public synchronized long rebalancesOf(String groupId) {
        return rebalancesByGroup.getOrDefault(groupId, 0L);
    }

    // ------------------------------------------------------------------ JOIN_GROUP

    public JoinResult join(String groupId, String memberId, String clientId, int sessionTimeoutMs, List<String> subscribed) {
        ErrorCode leader = checkLeader();
        if (leader != ErrorCode.NONE) {
            return JoinResult.error(leader);
        }
        if (sessionTimeoutMs < config.minSessionMs() || sessionTimeoutMs > config.maxSessionMs()) {
            return JoinResult.error(ErrorCode.INVALID_SESSION_TIMEOUT);
        }
        if (subscribed.isEmpty()) {
            return JoinResult.error(ErrorCode.INVALID_REQUEST);
        }
        // generated BEFORE proposing (the command must be deterministic) and kept across compare-and-set retries
        String id = memberId.isEmpty() ? clientId + "-" + idGenerator.get() : memberId;
        for (int attempt = 0; attempt < config.maxCasAttempts(); attempt++) {
            GroupStateMachine.GroupData group = state.group(groupId);
            int base = group == null ? 0 : group.generation();
            GroupCommands.Member existing = group == null ? null : group.members().get(id);
            if (!memberId.isEmpty() && existing == null) {
                return JoinResult.error(ErrorCode.UNKNOWN_MEMBER_ID);       // expired or never joined: start again with ""
            }
            if (existing != null && existing.sessionTimeoutMs() == sessionTimeoutMs
                    && new HashSet<>(existing.topics()).equals(new HashSet<>(subscribed))) {
                touch(groupId, id);                                         // idempotent rejoin: NO new generation
                return new JoinResult(ErrorCode.NONE, base, id, existing.assigned());
            }
            Map<String, GroupCommands.Member> next = new TreeMap<>();
            if (group != null) {
                next.putAll(group.members());
            }
            next.put(id, new GroupCommands.Member(id, clientId, sessionTimeoutMs, subscribed, List.of()));
            GroupCommands.SetMembership command = withAssignment(groupId, base, next);
            Object result = proposeAndWait(GroupCommands.encode(command));
            if (result == GroupStateMachine.Result.OK) {
                touch(groupId, id);
                countRebalance(groupId);
                return new JoinResult(ErrorCode.NONE, command.newGeneration(), id, assignmentOf(command, id));
            }
            if (result != GroupStateMachine.Result.STALE_GENERATION) {
                return JoinResult.error(errorOf(result));
            }
            // someone else changed the group first: look again and recompute
        }
        return JoinResult.error(ErrorCode.REBALANCE_IN_PROGRESS);
    }

    // ------------------------------------------------------------------ HEARTBEAT (no Raft involved)

    public ErrorCode heartbeat(String groupId, String memberId, int generation) {
        ErrorCode leader = checkLeader();
        if (leader != ErrorCode.NONE) {
            return leader;
        }
        GroupStateMachine.GroupData group = state.group(groupId);
        if (group == null || !group.members().containsKey(memberId)) {
            return ErrorCode.UNKNOWN_MEMBER_ID;
        }
        if (generation > group.generation()) {
            return ErrorCode.ILLEGAL_GENERATION;                            // nobody may be ahead of the leader
        }
        if (generation < group.generation()) {
            return ErrorCode.REBALANCE_IN_PROGRESS;                         // the chart changed: rejoin
        }
        touch(groupId, memberId);
        acknowledge(groupId, memberId, generation);
        return ErrorCode.NONE;
    }

    // ------------------------------------------------------------------ LEAVE_GROUP

    public ErrorCode leave(String groupId, String memberId) {
        ErrorCode leader = checkLeader();
        if (leader != ErrorCode.NONE) {
            return leader;
        }
        for (int attempt = 0; attempt < config.maxCasAttempts(); attempt++) {
            GroupStateMachine.GroupData group = state.group(groupId);
            if (group == null || !group.members().containsKey(memberId)) {
                return ErrorCode.UNKNOWN_MEMBER_ID;
            }
            Map<String, GroupCommands.Member> remaining = new TreeMap<>(group.members());
            remaining.remove(memberId);
            Object result = proposeAndWait(GroupCommands.encode(withAssignment(groupId, group.generation(), remaining)));
            if (result == GroupStateMachine.Result.OK) {
                forget(groupId, memberId);
                countRebalance(groupId);
                return ErrorCode.NONE;
            }
            if (result != GroupStateMachine.Result.STALE_GENERATION) {
                return errorOf(result);
            }
        }
        return ErrorCode.REBALANCE_IN_PROGRESS;
    }

    // ------------------------------------------------------------------ OFFSET_COMMIT / OFFSET_FETCH / DESCRIBE

    public ErrorCode commit(String groupId, String memberId, int generation, List<PartitionOffset> offsets) {
        ErrorCode leader = checkLeader();
        if (leader != ErrorCode.NONE) {
            return leader;
        }
        // cheap pre-check on the applied state for a fast failure; the REAL check is the one inside apply()
        GroupStateMachine.GroupData group = state.group(groupId);
        GroupStateMachine.Result pre = GroupStateMachine.checkCommit(group, memberId, generation, offsets);
        boolean lastWords = false;
        if (pre == GroupStateMachine.Result.PARTITION_NOT_ASSIGNED && config.cooperative()
                && GroupStateMachine.checkCommit(group, memberId, generation, offsets, true) == GroupStateMachine.Result.OK) {
            lastWords = true;                              // only partitions nobody owns are missing: the old owner of a handoff
            pre = GroupStateMachine.Result.OK;
        }
        if (pre != GroupStateMachine.Result.OK) {
            return errorOf(pre);
        }
        GroupCommands.OffsetCommit command = new GroupCommands.OffsetCommit(groupId, memberId, generation, offsets);
        Object result = proposeAndWait(lastWords ? GroupCommands.encodeRevoked(command) : GroupCommands.encode(command));
        return result == GroupStateMachine.Result.OK ? ErrorCode.NONE : errorOf(result);
    }

    public FetchResult fetchOffsets(String groupId, List<PartitionRef> partitions) {
        ErrorCode leader = checkLeader();
        if (leader != ErrorCode.NONE) {
            return new FetchResult(leader, List.of());
        }
        List<PartitionOffset> out = new ArrayList<>(partitions.size());
        for (PartitionRef p : partitions) {
            out.add(new PartitionOffset(p.topic(), p.partition(), state.committedOffset(groupId, p)));
        }
        return new FetchResult(ErrorCode.NONE, out);
    }

    public DescribeResult describe(String groupId) {
        ErrorCode leader = checkLeader();
        if (leader != ErrorCode.NONE) {
            return new DescribeResult(leader, null);
        }
        GroupStateMachine.GroupData group = state.group(groupId);
        if (group == null) {
            return new DescribeResult(ErrorCode.NONE, new DescribeGroupResponse(0, DescribeGroupResponse.UNKNOWN, List.of(), List.of()));
        }
        List<DescribeGroupResponse.Member> members = new ArrayList<>();
        for (GroupCommands.Member m : group.members().values()) {
            members.add(new DescribeGroupResponse.Member(m.memberId(), m.clientId(), m.sessionTimeoutMs(), m.topics(), m.assigned()));
        }
        List<PartitionOffset> offsets = new ArrayList<>();
        group.offsets().forEach((p, o) -> offsets.add(new PartitionOffset(p.topic(), p.partition(), o)));
        int s = members.isEmpty() ? DescribeGroupResponse.EMPTY : DescribeGroupResponse.STABLE;
        return new DescribeResult(ErrorCode.NONE, new DescribeGroupResponse(group.generation(), s, members, offsets));
    }

    // ------------------------------------------------------------------ session expiry

    /** How often the owner should call expireSessions(): a quarter of the shortest session in use, at least 50 ms. */
    public long tickIntervalMs() {
        long shortest = Long.MAX_VALUE;
        for (String g : state.groupIds()) {
            GroupStateMachine.GroupData data = state.group(g);
            if (data != null) {
                for (GroupCommands.Member m : data.members().values()) {
                    shortest = Math.min(shortest, m.sessionTimeoutMs());
                }
            }
        }
        if (shortest == Long.MAX_VALUE) {
            shortest = config.minSessionMs();                  // no member yet: be ready for the shortest session allowed
        }
        return Math.max(50, shortest / 4);
    }

    /**
     * Removes every member whose last heartbeat is older than its own session timeout. ALL expired members of one
     * group go in ONE command (two commands would waste a generation). Does not block: the outcome is handled when
     * the proposal completes. If the compare-and-set loses, the next call recomputes.
     */
    public void expireSessions() {
        if (checkLeader() != ErrorCode.NONE) {
            return;
        }
        long now = clock.getAsLong();
        for (String groupId : state.groupIds()) {
            GroupStateMachine.GroupData group = state.group(groupId);
            if (group == null) {
                continue;
            }
            List<String> expired = new ArrayList<>();
            for (GroupCommands.Member m : group.members().values()) {
                if (now - lastSeen(groupId, m.memberId()) > m.sessionTimeoutMs()) {
                    expired.add(m.memberId());
                }
            }
            if (expired.isEmpty()) {
                refreshAssignment(groupId, group);
                continue;
            }
            if (!startExpiry(groupId)) {
                continue;
            }
            Map<String, GroupCommands.Member> remaining = new TreeMap<>(group.members());
            expired.forEach(remaining::remove);
            byte[] command = GroupCommands.encode(withAssignment(groupId, group.generation(), remaining));
            LOG.log(System.Logger.Level.INFO, "{0}: group {1}: session expired for {2}", nodeLabel, groupId, expired);
            CompletableFuture<Object> future;
            try {
                future = log.propose(command, config.proposeTimeoutMs());
            } catch (RuntimeException e) {
                finishExpiry(groupId);
                continue;
            }
            future.whenComplete((result, error) -> {
                finishExpiry(groupId);
                if (error == null && result == GroupStateMachine.Result.OK) {
                    expired.forEach(m -> forget(groupId, m));
                    countRebalance(groupId);
                }
                // STALE_GENERATION or any failure: the next tick looks again with fresh state
            });
        }
    }

    /**
     * The assignment was computed from the topic list this broker knew AT THAT MOMENT. A topic that was created just before the
     * members subscribed may not have reached this broker yet (a follower applies a new entry when it hears the next commit
     * index), and an idempotent rejoin would hand out the same empty assignment for ever. So the tick re-derives the assignment
     * from the CURRENT topic list and installs a new generation if it differs. Partition counts never change, so this fires once
     * per topic and cannot flap.
     */
    private void refreshAssignment(String groupId, GroupStateMachine.GroupData group) {
        if (group.members().isEmpty()) {
            return;
        }
        GroupCommands.SetMembership desired = withAssignment(groupId, group.generation(), group.members());
        boolean same = true;
        for (GroupCommands.Member m : desired.members()) {
            GroupCommands.Member current = group.members().get(m.memberId());
            if (current == null || !current.assigned().equals(m.assigned())) {
                same = false;
            }
        }
        if (same) {
            return;
        }
        if (config.cooperative() && !everyoneAcknowledged(groupId, group)) {
            return;                                    // a partition may still be in use by its old owner: wait for its acknowledgement
        }
        if (!startExpiry(groupId)) {
            return;
        }
        LOG.log(System.Logger.Level.INFO, "{0}: group {1}: completing the assignment (new topic or finished handoff)", nodeLabel, groupId);
        CompletableFuture<Object> future;
        try {
            future = log.propose(GroupCommands.encode(desired), config.proposeTimeoutMs());
        } catch (RuntimeException e) {
            finishExpiry(groupId);
            return;
        }
        future.whenComplete((result, error) -> {
            finishExpiry(groupId);
            if (error == null && result == GroupStateMachine.Result.OK) {
                countRebalance(groupId);
            }
        });
    }

    private synchronized boolean startExpiry(String groupId) {
        return expiryInFlight.add(groupId);
    }

    private synchronized void finishExpiry(String groupId) {
        expiryInFlight.remove(groupId);
    }

    private synchronized void countRebalance(String groupId) {
        rebalancesByGroup.merge(groupId, 1L, Long::sum);
    }

    // ------------------------------------------------------------------ helpers

    /** Computes the assignment for the given members and wraps it into the next-generation command. */
    private GroupCommands.SetMembership withAssignment(String groupId, int baseGeneration, Map<String, GroupCommands.Member> members) {
        Map<String, List<String>> subscriptions = new TreeMap<>();
        Map<String, Integer> counts = new TreeMap<>();
        for (GroupCommands.Member m : members.values()) {
            subscriptions.put(m.memberId(), m.topics());
            for (String t : m.topics()) {
                int partitions = topics.partitionCount(t);
                if (partitions > 0) {
                    counts.put(t, partitions);
                }
            }
        }
        Map<String, List<PartitionRef>> assignment = config.cooperative()
                ? cooperativeAssignment(state.group(groupId), members, subscriptions, counts)
                : Assignor.assign(subscriptions, counts);
        List<GroupCommands.Member> installed = new ArrayList<>();
        for (GroupCommands.Member m : members.values()) {
            installed.add(new GroupCommands.Member(m.memberId(), m.clientId(), m.sessionTimeoutMs(), m.topics(),
                    assignment.getOrDefault(m.memberId(), List.of())));
        }
        return new GroupCommands.SetMembership(groupId, baseGeneration, baseGeneration + 1, installed);
    }

    /**
     * Sticky target minus the handoffs: a partition whose CURRENT owner is still a member but is no longer the target owner is
     * revoked (given to nobody now); it is given to the target owner by a later generation, once the old owner acknowledged. A
     * partition without a live owner (new, or its owner left or expired) goes straight to its target owner.
     */
    private static Map<String, List<PartitionRef>> cooperativeAssignment(GroupStateMachine.GroupData current, Map<String, GroupCommands.Member> members,
                                                                        Map<String, List<String>> subscriptions, Map<String, Integer> counts) {
        Map<String, List<PartitionRef>> previous = new TreeMap<>();
        Map<PartitionRef, String> holder = new HashMap<>();
        if (current != null) {
            for (GroupCommands.Member m : current.members().values()) {
                if (members.containsKey(m.memberId())) {
                    previous.put(m.memberId(), m.assigned());
                    for (PartitionRef p : m.assigned()) {
                        holder.put(p, m.memberId());
                    }
                }
            }
        }
        Map<String, List<PartitionRef>> target = Assignor.assignSticky(subscriptions, counts, previous);
        Map<String, List<PartitionRef>> installed = new TreeMap<>();
        for (Map.Entry<String, List<PartitionRef>> e : target.entrySet()) {
            List<PartitionRef> keep = new ArrayList<>();
            for (PartitionRef p : e.getValue()) {
                String owner = holder.get(p);
                if (owner == null || owner.equals(e.getKey())) {
                    keep.add(p);
                }
            }
            installed.put(e.getKey(), keep);
        }
        return installed;
    }

    private static List<PartitionRef> assignmentOf(GroupCommands.SetMembership command, String memberId) {
        for (GroupCommands.Member m : command.members()) {
            if (m.memberId().equals(memberId)) {
                return m.assigned();
            }
        }
        return List.of();
    }

    /** Proposes and waits. Returns the state machine's Result, or an ErrorCode when Raft itself failed. */
    private Object proposeAndWait(byte[] command) {
        try {
            return log.propose(command, config.proposeTimeoutMs()).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ErrorCode.COORDINATOR_NOT_AVAILABLE;
        } catch (ExecutionException e) {
            return errorOfFailure(e.getCause());
        } catch (RuntimeException e) {
            return errorOfFailure(e);
        }
    }

    private ErrorCode errorOfFailure(Throwable cause) {
        if (cause instanceof GroupRuntime.NotLeaderException) {
            return log.leaderId() >= 0 ? ErrorCode.NOT_COORDINATOR : ErrorCode.COORDINATOR_NOT_AVAILABLE;
        }
        if (cause instanceof GroupRuntime.ProposeTimeoutException) {
            return ErrorCode.REQUEST_TIMED_OUT;                            // ambiguous: it may still commit
        }
        if (cause instanceof GroupRuntime.OverloadedException) {
            return ErrorCode.BROKER_OVERLOADED;
        }
        LOG.log(System.Logger.Level.WARNING, nodeLabel + ": proposal failed", cause);
        return ErrorCode.INTERNAL_ERROR;
    }

    private static ErrorCode errorOf(Object result) {
        if (result instanceof ErrorCode e) {
            return e;
        }
        if (result instanceof GroupStateMachine.Result r) {
            return switch (r) {
                case OK -> ErrorCode.NONE;
                case STALE_GENERATION -> ErrorCode.REBALANCE_IN_PROGRESS;
                case UNKNOWN_MEMBER_ID -> ErrorCode.UNKNOWN_MEMBER_ID;
                case ILLEGAL_GENERATION -> ErrorCode.ILLEGAL_GENERATION;
                case PARTITION_NOT_ASSIGNED -> ErrorCode.PARTITION_NOT_ASSIGNED;
                case INVALID -> ErrorCode.INVALID_REQUEST;
            };
        }
        return ErrorCode.INTERNAL_ERROR;
    }
}
