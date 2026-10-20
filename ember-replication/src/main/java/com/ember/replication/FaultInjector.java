package com.ember.replication;

/**
 * TEST HOOKS for fault injection (Phase 7). Every class that has a hook gets FaultInjector.NONE unless a test passes
 * something else, and each call site is written `if (faults.active()) { ... }`: with NONE the condition is the constant
 * false, so the JIT removes the hook and a normal broker pays nothing. There is deliberately no flag, system property or
 * endpoint that switches a fault on in a real broker.
 */
public interface FaultInjector {

    /** The default: nothing is ever injected. */
    FaultInjector NONE = new FaultInjector() {
    };

    /** False means "skip every hook". */
    default boolean active() {
        return false;
    }

    /** Extra time every Raft log fsync takes (a slow disk), in nanoseconds. */
    default long fsyncDelayNanos() {
        return 0;
    }

    /** Should this outgoing Raft message to the peer be dropped? */
    default boolean dropPeerMessage(int toNode) {
        return false;
    }

    /** Fixed extra time before an outgoing Raft message to the peer is sent, in milliseconds (keeps message order). */
    default long peerDelayMillis(int toNode) {
        return 0;
    }

    /** Parks the calling Raft worker while the broker is "paused" (frozen like SIGSTOP). Returns at once otherwise. */
    default void awaitNotPaused() throws InterruptedException {
    }
}
