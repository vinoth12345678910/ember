package com.ember.replication;

import com.ember.raft.LogEntry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MetadataStateMachineTest {

    private static LogEntry create(String name, int partitions) {
        return LogEntry.command(1, Commands.createTopic(name, partitions));
    }

    @Test
    void createDuplicateAndListener() {
        List<String> created = new ArrayList<>();
        MetadataStateMachine sm = new MetadataStateMachine((n, p) -> created.add(n + ":" + p), 256);
        assertNull(sm.apply(1, LogEntry.noop(1)));
        assertEquals(MetadataStateMachine.CreateResult.CREATED, sm.apply(2, create("orders", 4)));
        assertEquals(MetadataStateMachine.CreateResult.EXISTS, sm.apply(3, create("orders", 9)));
        assertEquals(MetadataStateMachine.CreateResult.CREATED, sm.apply(4, create("a-b", 1)));
        assertEquals(List.of("orders:4", "a-b:1"), created);
        assertEquals(Map.of("a-b", 1, "orders", 4), sm.topics());
        assertEquals(4, sm.partitionCount("orders"));
        assertNull(sm.partitionCount("nope"));
    }

    @Test
    void invalidCreatesAreDecidedDeterministicallyAndIgnored() {
        List<String> created = new ArrayList<>();
        MetadataStateMachine sm = new MetadataStateMachine((n, p) -> created.add(n), 8);
        assertEquals(MetadataStateMachine.CreateResult.INVALID, sm.apply(1, create("__hidden", 1)));
        assertEquals(MetadataStateMachine.CreateResult.INVALID, sm.apply(2, create("bad name", 1)));
        assertEquals(MetadataStateMachine.CreateResult.INVALID, sm.apply(3, create("t", 0)));
        assertEquals(MetadataStateMachine.CreateResult.INVALID, sm.apply(4, create("t", 9)));
        assertTrue(created.isEmpty());
        assertTrue(sm.topics().isEmpty());
    }

    @Test
    void replayingTheSameEntriesRebuildsTheSameTopicsAndRecreatesTheGroups() {
        List<LogEntry> log = List.of(LogEntry.noop(1), create("a", 2), create("b", 3), create("a", 5));
        for (int run = 0; run < 2; run++) {
            List<String> created = new ArrayList<>();
            MetadataStateMachine sm = new MetadataStateMachine((n, p) -> created.add(n + ":" + p), 256);
            long i = 1;
            for (LogEntry e : log) sm.apply(i++, e);
            assertEquals(Map.of("a", 2, "b", 3), sm.topics());
            assertEquals(List.of("a:2", "b:3"), created);
        }
    }
}
