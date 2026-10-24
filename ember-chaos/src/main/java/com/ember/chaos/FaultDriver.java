package com.ember.chaos;

/**
 * How faults are applied to a cluster. Nodes are numbered 1..brokers(). A driver throws UnsupportedOperationException for a
 * fault it cannot apply (for example link blocking with real Docker containers); supports() tells the nemesis in advance.
 */
public interface FaultDriver {

    int brokers();

    /** The broker that currently leads the "__acks" group (ack queues), or -1 if unknown. */
    default int queueLeader() {
        return -1;
    }

    boolean supports(Fault fault);

    /** Dies like kill -9 (no goodbye). */
    void crash(int node) throws Exception;

    void restart(int node) throws Exception;

    /** Freezes the broker (SIGSTOP-like) without closing its connections. */
    void pause(int node) throws Exception;

    void resume(int node) throws Exception;

    /** Cuts the broker off from all the others (clients may still reach it, if the driver allows). */
    void isolate(int node) throws Exception;

    void heal() throws Exception;

    void blockLink(int a, int b) throws Exception;

    void unblockLink(int a, int b) throws Exception;

    void slowFsync(int node, long millis) throws Exception;

    void dropMessages(int node, double probability, long seed) throws Exception;

    void delayMessages(int node, long millis) throws Exception;

    /** Removes slow-fsync / drop / delay from the broker. */
    void clearSoftFaults(int node) throws Exception;

    /** Puts everything back: restart what was crashed, resume what was paused, heal the network, clear every soft fault. */
    void healAll() throws Exception;
}
