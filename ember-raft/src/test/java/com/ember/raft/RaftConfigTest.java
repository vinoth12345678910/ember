package com.ember.raft;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftConfigTest {

    @Test
    void defaultsAndSortedCluster() {
        RaftConfig c = RaftConfig.of(2, List.of(3, 1, 2));
        assertEquals(List.of(1, 2, 3), c.clusterNodeIds());
        assertEquals(150, c.electionTimeoutMinMs());
        assertEquals(300, c.electionTimeoutMaxMs());
        assertEquals(50, c.heartbeatIntervalMs());
        assertEquals(64, c.maxEntriesPerAppend());
        assertEquals(Bug.NONE, c.injectedBug());
    }

    @Test
    void majorityWorksForOddAndEvenClusters() {
        assertEquals(1, RaftConfig.of(1, List.of(1)).majority());
        assertEquals(2, RaftConfig.of(1, List.of(1, 2)).majority());
        assertEquals(2, RaftConfig.of(1, List.of(1, 2, 3)).majority());
        assertEquals(3, RaftConfig.of(1, List.of(1, 2, 3, 4)).majority());
        assertEquals(3, RaftConfig.of(1, List.of(1, 2, 3, 4, 5)).majority());
    }

    @Test
    void validation() {
        assertThrows(IllegalArgumentException.class, () -> RaftConfig.of(9, List.of(1, 2, 3)));
        assertThrows(IllegalArgumentException.class, () -> RaftConfig.of(1, List.of(1, 1, 2)));
        assertThrows(IllegalArgumentException.class, () -> RaftConfig.of(1, List.of(1, 2)).withElectionTimeout(300, 150));
        assertThrows(IllegalArgumentException.class, () -> RaftConfig.of(1, List.of(1, 2)).withHeartbeat(150));
        assertThrows(IllegalArgumentException.class, () -> RaftConfig.of(1, List.of(1, 2)).withHeartbeat(0));
        assertThrows(IllegalArgumentException.class, () -> RaftConfig.of(1, List.of(1, 2)).withMaxEntriesPerAppend(0));
        assertDoesNotThrow(() -> RaftConfig.of(1, List.of(1, 2)).withElectionTimeout(100, 100));
    }

    @Test
    void theClusterListIsImmutable() {
        RaftConfig c = RaftConfig.of(1, List.of(1, 2, 3));
        assertThrows(UnsupportedOperationException.class, () -> c.clusterNodeIds().add(4));
    }

    @Test
    void messagesCopyTheirEntryLists() {
        java.util.ArrayList<LogEntry> list = new java.util.ArrayList<>(List.of(LogEntry.noop(1)));
        RaftMessage.AppendEntries ae = new RaftMessage.AppendEntries(1, 1, 0, 0, list, 0);
        list.clear();
        assertEquals(1, ae.entries().size());
    }
}
