package com.ember.raft;

public interface RaftListener {

    /** Called when this node's role or its known leader changes. leaderId is -1 if unknown. */
    void onRoleChange(int nodeId, Role role, long term, int leaderId);

    /**
     * Called exactly once per log index, in index order with no gaps, during one node lifetime.
     * After a restart the node applies again from index 1. NOOP entries are included.
     */
    void onCommitted(int nodeId, long index, LogEntry entry);

    /**
     * Called when a leader steps down WITHOUT a new term because it has not heard from a majority (CheckQuorum).
     * A read-only observation for metrics and logs; the default does nothing, so existing listeners are unaffected.
     */
    default void onCheckQuorumStepDown(int nodeId, long term) {
    }
}
