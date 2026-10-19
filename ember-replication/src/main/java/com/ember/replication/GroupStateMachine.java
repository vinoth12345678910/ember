package com.ember.replication;

import com.ember.protocol.PartitionOffset;
import com.ember.protocol.PartitionRef;
import com.ember.raft.EntryType;
import com.ember.raft.LogEntry;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The state machine of the "__groups" Raft group: for every consumer group its generation, members with their
 * assignments, and the committed offsets.
 *
 * apply() is a PURE function of (state, command): no clock, no randomness, no I/O, no peeking at other Raft groups
 * (for example the topic list). Replicas that replay the same log, today or in five years, must reach the same
 * state; whatever the commands need was decided by the coordinator leader before it proposed them.
 *
 * Readers (the coordinator, DESCRIBE, tests) may be on other threads, so every group is an IMMUTABLE GroupData
 * that is replaced as a whole (copy-on-write).
 */
public final class GroupStateMachine implements StateMachine {

    /** What apply() answers; the proposer's future completes with it. */
    public enum Result {
        OK,
        /** SET_MEMBERSHIP built on a generation that is no longer current: nothing changed, recompute and retry. */
        STALE_GENERATION,
        UNKNOWN_MEMBER_ID,
        ILLEGAL_GENERATION,
        PARTITION_NOT_ASSIGNED,
        /** malformed or inconsistent command: rejected deterministically on every replica, nothing changed */
        INVALID
    }

    /** An immutable view of one group. */
    public record GroupData(int generation, Map<String, GroupCommands.Member> members, Map<PartitionRef, Long> offsets) {
        public GroupData {
            members = Collections.unmodifiableMap(new TreeMap<>(members));
            offsets = Collections.unmodifiableMap(new TreeMap<>(offsets));
        }
    }

    private final Map<String, GroupData> groups = new ConcurrentHashMap<>();

    @Override
    public Object apply(long index, LogEntry entry) {
        if (entry.type() == EntryType.NOOP) {
            return null;
        }
        byte[] command = entry.command();
        try {
            return switch (GroupCommands.type(command)) {
                case GroupCommands.SET_MEMBERSHIP -> setMembership(GroupCommands.decodeSetMembership(command));
                case GroupCommands.OFFSET_COMMIT -> commit(GroupCommands.decodeOffsetCommit(command));
                default -> throw new IllegalStateException("unknown __groups command type " + GroupCommands.type(command));
            };
        } catch (IllegalArgumentException malformed) {
            return Result.INVALID;                           // identical on every replica, changes nothing
        }
    }

    private Result setMembership(GroupCommands.SetMembership c) {
        GroupData current = groups.get(c.groupId());
        int currentGeneration = current == null ? 0 : current.generation();
        if (c.baseGeneration() != currentGeneration) {
            return Result.STALE_GENERATION;
        }
        if (c.newGeneration() != c.baseGeneration() + 1) {
            return Result.INVALID;
        }
        Map<String, GroupCommands.Member> members = new LinkedHashMap<>();
        Set<PartitionRef> owned = new HashSet<>();
        for (GroupCommands.Member m : c.members()) {
            if (members.put(m.memberId(), m) != null) {
                return Result.INVALID;                       // the same member twice
            }
            for (PartitionRef p : m.assigned()) {
                if (!owned.add(p)) {
                    return Result.INVALID;                   // a partition with two owners would defeat the whole point
                }
            }
        }
        groups.put(c.groupId(), new GroupData(c.newGeneration(), members, current == null ? Map.of() : current.offsets()));
        return Result.OK;
    }

    private Result commit(GroupCommands.OffsetCommit c) {
        Result check = checkCommit(groups.get(c.groupId()), c.memberId(), c.generation(), c.offsets());
        if (check != Result.OK) {
            return check;
        }
        GroupData current = groups.get(c.groupId());
        Map<PartitionRef, Long> offsets = new TreeMap<>(current.offsets());
        for (PartitionOffset o : c.offsets()) {
            offsets.put(new PartitionRef(o.topic(), o.partition()), o.offset());
        }
        groups.put(c.groupId(), new GroupData(current.generation(), current.members(), offsets));
        return Result.OK;
    }

    /**
     * The checks of an OFFSET_COMMIT, shared with the coordinator's cheap pre-check so that both give the same
     * answer. All or nothing: the first failing check rejects the whole request.
     */
    static Result checkCommit(GroupData group, String memberId, int generation, List<PartitionOffset> offsets) {
        if (group == null || !group.members().containsKey(memberId)) {
            return Result.UNKNOWN_MEMBER_ID;
        }
        if (generation != group.generation()) {
            return Result.ILLEGAL_GENERATION;
        }
        Set<PartitionRef> assigned = new HashSet<>(group.members().get(memberId).assigned());
        for (PartitionOffset o : offsets) {
            if (!assigned.contains(new PartitionRef(o.topic(), o.partition()))) {
                return Result.PARTITION_NOT_ASSIGNED;
            }
            if (o.offset() < 0) {
                return Result.INVALID;
            }
        }
        return Result.OK;
    }

    // ------------------------------------------------------------------ reads

    /** null when the group does not exist. */
    public GroupData group(String groupId) {
        return groups.get(groupId);
    }

    public Set<String> groupIds() {
        return new java.util.TreeSet<>(groups.keySet());
    }

    /** -1 when nothing was committed. */
    public long committedOffset(String groupId, PartitionRef partition) {
        GroupData g = groups.get(groupId);
        Long offset = g == null ? null : g.offsets().get(partition);
        return offset == null ? -1 : offset;
    }

    /** A sorted copy of everything, comparable with equals(): used by tests and by the replicas-identical checks. */
    public Map<String, GroupData> snapshot() {
        return new TreeMap<>(groups);
    }
}
