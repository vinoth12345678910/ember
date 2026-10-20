package com.ember.chaos;

/** Gathers the state of every replica at the end of a run, as REPLICA events in the history (property P9). */
public interface Collector {

    /** Waits (up to the timeout) until the replicas have caught up with each other. Returns false if they never did. */
    boolean awaitConverged(long timeoutMs) throws Exception;

    /** Data-log digests, group-state and topic-state digests. */
    void collect(HistoryRecorder recorder) throws Exception;
}
