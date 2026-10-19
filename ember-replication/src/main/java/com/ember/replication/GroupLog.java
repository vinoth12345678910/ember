package com.ember.replication;

import java.util.concurrent.CompletableFuture;

/**
 * What the GroupCoordinator needs from the "__groups" Raft group, and nothing more. In production this is the
 * GroupRuntime; in the coordinator's unit tests it is a fake that applies commands directly, so the tests
 * need neither threads nor real time.
 */
public interface GroupLog {

    /** This broker is the Raft leader of "__groups". */
    boolean isLeader();

    /** Leader that has applied an entry of its own term, i.e. whose applied state is complete. */
    boolean isLeaderReady();

    /** Current leader or -1. */
    int leaderId();

    /** The Raft term; a change of term while leading means "I may have been away: restart the soft state". */
    long term();

    /** Completes with the state machine's result once the command is committed AND applied here. */
    CompletableFuture<Object> propose(byte[] command, long timeoutMs);

    static GroupLog of(GroupRuntime runtime) {
        return new GroupLog() {
            @Override
            public boolean isLeader() {
                return runtime.isLeader();
            }

            @Override
            public boolean isLeaderReady() {
                return runtime.isLeaderReady();
            }

            @Override
            public int leaderId() {
                return runtime.leaderId();
            }

            @Override
            public long term() {
                return runtime.term();
            }

            @Override
            public CompletableFuture<Object> propose(byte[] command, long timeoutMs) {
                return runtime.propose(command, timeoutMs);
            }
        };
    }
}
