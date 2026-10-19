package com.ember.raft;

public interface RaftListener {

    /** Called when this node's role or its known leader changes. leaderId is -1 if unknown. */
    void onRoleChange(int nodeId, Role role, long term, int leaderId);

    /**
     * Called exactly once per log index, in index order with no gaps, during one node lifetime.
     * After a restart the node applies again from index 1. NOOP entries are included.
     */
    void onCommitted(int nodeId, long index, LogEntry entry);
}
