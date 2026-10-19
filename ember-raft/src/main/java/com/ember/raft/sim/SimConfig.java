package com.ember.raft.sim;

import com.ember.raft.Bug;

import java.util.Map;
import java.util.TreeMap;

/**
 * Everything that defines one simulated run. Same SimConfig -> same run, byte for byte.
 * Node ids are 1..nodeCount.
 */
public record SimConfig(long seed, int nodeCount, long minLatencyMs, long maxLatencyMs, double dropProbability,
                        double duplicateProbability, Map<Integer, long[]> electionTimeouts, Bug bug,
                        int maxEntriesPerAppend, boolean checkQuorum) {

    public SimConfig {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("nodeCount must be >= 1");
        }
        if (minLatencyMs < 0 || maxLatencyMs < minLatencyMs) {
            throw new IllegalArgumentException("need 0 <= minLatencyMs <= maxLatencyMs");
        }
        electionTimeouts = new TreeMap<>(electionTimeouts);     // sorted: deterministic, and a private copy
    }

    /** The Phase 4 shape (CheckQuorum off). */
    public SimConfig(long seed, int nodeCount, long minLatencyMs, long maxLatencyMs, double dropProbability,
                     double duplicateProbability, Map<Integer, long[]> electionTimeouts, Bug bug, int maxEntriesPerAppend) {
        this(seed, nodeCount, minLatencyMs, maxLatencyMs, dropProbability, duplicateProbability, electionTimeouts, bug,
                maxEntriesPerAppend, false);
    }

    public static SimConfig of(long seed, int nodeCount) {
        return new SimConfig(seed, nodeCount, 5, 40, 0.0, 0.0, Map.of(), Bug.NONE, 64, false);
    }

    public SimConfig withCheckQuorum(boolean enabled) {
        return new SimConfig(seed, nodeCount, minLatencyMs, maxLatencyMs, dropProbability, duplicateProbability, electionTimeouts, bug, maxEntriesPerAppend, enabled);
    }

    public SimConfig withLatency(long min, long max) {
        return new SimConfig(seed, nodeCount, min, max, dropProbability, duplicateProbability, electionTimeouts, bug, maxEntriesPerAppend, checkQuorum);
    }

    public SimConfig withDrop(double p) {
        return new SimConfig(seed, nodeCount, minLatencyMs, maxLatencyMs, p, duplicateProbability, electionTimeouts, bug, maxEntriesPerAppend, checkQuorum);
    }

    public SimConfig withDuplicate(double p) {
        return new SimConfig(seed, nodeCount, minLatencyMs, maxLatencyMs, dropProbability, p, electionTimeouts, bug, maxEntriesPerAppend, checkQuorum);
    }

    /** min == max forces an exact timeout for that node (scripted tests). */
    public SimConfig withElectionTimeout(int node, long min, long max) {
        Map<Integer, long[]> copy = new TreeMap<>(electionTimeouts);
        copy.put(node, new long[]{min, max});
        return new SimConfig(seed, nodeCount, minLatencyMs, maxLatencyMs, dropProbability, duplicateProbability, copy, bug, maxEntriesPerAppend, checkQuorum);
    }

    public SimConfig withBug(Bug bug) {
        return new SimConfig(seed, nodeCount, minLatencyMs, maxLatencyMs, dropProbability, duplicateProbability, electionTimeouts, bug, maxEntriesPerAppend, checkQuorum);
    }

    public SimConfig withMaxEntriesPerAppend(int max) {
        return new SimConfig(seed, nodeCount, minLatencyMs, maxLatencyMs, dropProbability, duplicateProbability, electionTimeouts, bug, max, checkQuorum);
    }

    public SimConfig withSeed(long newSeed) {
        return new SimConfig(newSeed, nodeCount, minLatencyMs, maxLatencyMs, dropProbability, duplicateProbability, electionTimeouts, bug, maxEntriesPerAppend, checkQuorum);
    }
}
