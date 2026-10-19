package com.ember.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * A fake disk with the one property that matters for Raft: writes are NOT durable until sync().
 * Writes go to the PENDING copy (which is what the node itself reads); sync() copies pending to
 * durable; crash() throws pending away, exactly like losing power with data still in the page cache.
 */
public final class InMemoryRaftStorage implements RaftStorage {

    private static final class State {
        long term;
        int votedFor = -1;
        ArrayList<LogEntry> log = new ArrayList<>();

        State copy() {
            State s = new State();
            s.term = term;
            s.votedFor = votedFor;
            s.log = new ArrayList<>(log);       // LogEntry is immutable, so a shallow copy is a real copy
            return s;
        }
    }

    private State pending = new State();
    private State durable = new State();
    private boolean dirty = false;
    private long syncCount = 0;

    public InMemoryRaftStorage() {
    }

    /** For scripted tests: a node that starts with a given persistent state (already durable). */
    public InMemoryRaftStorage(long term, int votedFor, List<LogEntry> log) {
        pending.term = term;
        pending.votedFor = votedFor;
        pending.log = new ArrayList<>(log);
        durable = pending.copy();
    }

    @Override
    public long currentTerm() {
        return pending.term;
    }

    @Override
    public int votedFor() {
        return pending.votedFor;
    }

    @Override
    public void setTermAndVote(long term, int votedFor) {
        pending.term = term;
        pending.votedFor = votedFor;
        dirty = true;
    }

    @Override
    public long lastIndex() {
        return pending.log.size();
    }

    @Override
    public long lastTerm() {
        int n = pending.log.size();
        return n == 0 ? 0 : pending.log.get(n - 1).term();
    }

    @Override
    public long termAt(long index) {
        if (index == 0) {
            return 0;
        }
        return entryAt(index).term();
    }

    @Override
    public LogEntry entryAt(long index) {
        if (index < 1 || index > pending.log.size()) {
            throw new IllegalArgumentException("no entry at index " + index + " (last index is " + pending.log.size() + ")");
        }
        return pending.log.get((int) index - 1);
    }

    @Override
    public List<LogEntry> entries(long fromIndex, int maxCount) {
        if (fromIndex < 1) {
            throw new IllegalArgumentException("fromIndex must be >= 1");
        }
        int from = (int) fromIndex - 1;
        if (from >= pending.log.size() || maxCount <= 0) {
            return List.of();
        }
        return new ArrayList<>(pending.log.subList(from, Math.min(pending.log.size(), from + maxCount)));
    }

    @Override
    public void append(List<LogEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        pending.log.addAll(entries);
        dirty = true;
    }

    @Override
    public void truncateFrom(long index) {
        if (index < 1 || index > pending.log.size() + 1) {
            throw new IllegalArgumentException("cannot truncate from " + index + " (last index is " + pending.log.size() + ")");
        }
        if (index <= pending.log.size()) {
            pending.log.subList((int) index - 1, pending.log.size()).clear();
            dirty = true;
        }
    }

    @Override
    public void sync() {
        if (dirty) {
            durable = pending.copy();
            dirty = false;
        }
        syncCount++;
    }

    // ---- test and simulator support (not part of RaftStorage)

    /** Power loss: whatever was not synced is gone, and the node starts again from the durable copy. */
    public void crash() {
        pending = durable.copy();
        dirty = false;
    }

    /** True if something was written since the last sync() (i.e. a crash right now would lose data). */
    public boolean hasUnsyncedWrites() {
        return dirty;
    }

    public long syncCount() {
        return syncCount;
    }

    public long durableTerm() {
        return durable.term;
    }

    public int durableVotedFor() {
        return durable.votedFor;
    }

    public List<LogEntry> durableLog() {
        return List.copyOf(durable.log);
    }

    public List<LogEntry> pendingLog() {
        return List.copyOf(pending.log);
    }
}
