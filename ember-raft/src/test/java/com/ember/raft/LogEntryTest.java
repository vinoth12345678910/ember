package com.ember.raft;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LogEntryTest {

    @Test
    void equalityComparesContentNotArrayIdentity() {
        LogEntry a = LogEntry.command(2, new byte[]{1, 2, 3});
        LogEntry b = LogEntry.command(2, new byte[]{1, 2, 3});
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, LogEntry.command(3, new byte[]{1, 2, 3}));
        assertNotEquals(a, LogEntry.command(2, new byte[]{1, 2, 4}));
        assertNotEquals(LogEntry.noop(2), LogEntry.command(2, new byte[0]));
    }

    @Test
    void theArrayIsCopiedOnTheWayInAndOut() {
        byte[] original = {1, 2, 3};
        LogEntry e = LogEntry.command(1, original);
        original[0] = 99;
        assertEquals(1, e.command()[0]);
        e.command()[1] = 99;
        assertEquals(2, e.command()[1]);
    }

    @Test
    void aNullCommandBecomesEmpty() {
        assertEquals(0, new LogEntry(1, EntryType.NOOP, null).command().length);
    }

    @Test
    void theHashIsStableAcrossJvmsBecauseItDoesNotUseEnumIdentity() {
        // a fixed expected value: if someone switches to Enum.hashCode() this changes between runs
        assertEquals(31 * (31 * 7 + 1) + java.util.Arrays.hashCode(new byte[]{9}),
                LogEntry.command(7, new byte[]{9}).hashCode());
    }
}
