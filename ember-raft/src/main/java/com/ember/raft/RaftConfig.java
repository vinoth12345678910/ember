package com.ember.raft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Static settings of one node. The cluster is FIXED (membership changes are a non-goal).
 * Defaults follow the paper's guidance: election timeout 150-300 ms, much larger than the heartbeat.
 */
public record RaftConfig(int nodeId, List<Integer> clusterNodeIds, long electionTimeoutMinMs,
                         long electionTimeoutMaxMs, long heartbeatIntervalMs, int maxEntriesPerAppend,
                         Bug injectedBug, int maxAppendBytes, boolean checkQuorum) {

    public RaftConfig {
        List<Integer> sorted = new ArrayList<>(clusterNodeIds);
        Collections.sort(sorted);
        for (int i = 1; i < sorted.size(); i++) {
            if (sorted.get(i).equals(sorted.get(i - 1))) {
                throw new IllegalArgumentException("duplicate node id " + sorted.get(i));
            }
        }
        if (!sorted.contains(nodeId)) {
            throw new IllegalArgumentException("node " + nodeId + " is not in the cluster " + sorted);
        }
        if (electionTimeoutMinMs > electionTimeoutMaxMs) {
            throw new IllegalArgumentException("election timeout min > max");
        }
        if (heartbeatIntervalMs <= 0 || heartbeatIntervalMs >= electionTimeoutMinMs) {
            throw new IllegalArgumentException("heartbeat interval must be > 0 and < the minimum election timeout");
        }
        if (maxEntriesPerAppend < 1) {
            throw new IllegalArgumentException("maxEntriesPerAppend must be >= 1");
        }
        if (maxAppendBytes < 1) {
            throw new IllegalArgumentException("maxAppendBytes must be >= 1");
        }
        if (injectedBug == null) {
            throw new IllegalArgumentException("injectedBug must not be null (use Bug.NONE)");
        }
        clusterNodeIds = List.copyOf(sorted);
    }

    /** The Phase 4 shape (no byte cap beyond the default, CheckQuorum off). */
    public RaftConfig(int nodeId, List<Integer> clusterNodeIds, long electionTimeoutMinMs, long electionTimeoutMaxMs,
                      long heartbeatIntervalMs, int maxEntriesPerAppend, Bug injectedBug) {
        this(nodeId, clusterNodeIds, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs,
                maxEntriesPerAppend, injectedBug, 1024 * 1024, false);
    }

    public static RaftConfig of(int nodeId, List<Integer> clusterNodeIds) {
        return new RaftConfig(nodeId, clusterNodeIds, 150, 300, 50, 64, Bug.NONE);
    }

    public RaftConfig withMaxAppendBytes(int bytes) {
        return new RaftConfig(nodeId, clusterNodeIds, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs,
                maxEntriesPerAppend, injectedBug, bytes, checkQuorum);
    }

    /** A leader that has not heard from a majority within electionTimeoutMaxMs steps down (term unchanged). */
    public RaftConfig withCheckQuorum(boolean enabled) {
        return new RaftConfig(nodeId, clusterNodeIds, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs,
                maxEntriesPerAppend, injectedBug, maxAppendBytes, enabled);
    }

    public RaftConfig withElectionTimeout(long min, long max) {
        return new RaftConfig(nodeId, clusterNodeIds, min, max, heartbeatIntervalMs, maxEntriesPerAppend, injectedBug, maxAppendBytes, checkQuorum);
    }

    public RaftConfig withHeartbeat(long heartbeatMs) {
        return new RaftConfig(nodeId, clusterNodeIds, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatMs,
                maxEntriesPerAppend, injectedBug, maxAppendBytes, checkQuorum);
    }

    public RaftConfig withMaxEntriesPerAppend(int max) {
        return new RaftConfig(nodeId, clusterNodeIds, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs,
                max, injectedBug, maxAppendBytes, checkQuorum);
    }

    public RaftConfig withBug(Bug bug) {
        return new RaftConfig(nodeId, clusterNodeIds, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs,
                maxEntriesPerAppend, bug, maxAppendBytes, checkQuorum);
    }

    /** Number of nodes that make a majority (paper 5.2): more than half of the whole cluster. */
    public int majority() {
        return clusterNodeIds.size() / 2 + 1;
    }
}
