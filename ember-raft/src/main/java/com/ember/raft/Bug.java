package com.ember.raft;

/**
 * TEST-ONLY fault injection. Each constant switches on exactly one small, clearly marked wrong
 * branch in the core, so the simulator's checkers can be proven to catch real Raft bugs.
 * Phase 5 (the real runtime) must NEVER set anything but NONE.
 */
public enum Bug {
    NONE,
    /** Grant votes without comparing logs (breaks 5.4.1, the election restriction). */
    VOTE_WITHOUT_LOG_CHECK,
    /** Commit an entry from an older term by counting replicas (the Figure 8 mistake, 5.4.2). */
    COMMIT_OLD_TERM_BY_COUNTING,
    /** Forget votedFor when restarting (a node can vote twice in a term). */
    FORGET_VOTED_FOR_ON_RESTART,
    /** Send replies BEFORE making state durable. */
    ACK_BEFORE_SYNC,
    /** Truncate the follower's log on every non-empty AppendEntries, even when the entries match. */
    TRUNCATE_ON_EVERY_APPEND
}
