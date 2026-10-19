package com.ember.replication;

import com.ember.raft.LogEntry;

/** Applies COMMITTED entries, in index order, on the group's serial thread. Must be deterministic. */
public interface StateMachine {

    /** @return the result handed to the waiting client (for a produce: the base offset), or null */
    Object apply(long index, LogEntry entry);
}
