package com.ember.raft;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryRaftStorageTest {

    private static LogEntry e(long term, String cmd) {
        return LogEntry.command(term, cmd.getBytes());
    }

    @Test
    void anEmptyLogHasIndexZeroAndTermZero() {
        InMemoryRaftStorage s = new InMemoryRaftStorage();
        assertEquals(0, s.lastIndex());
        assertEquals(0, s.lastTerm());
        assertEquals(0, s.termAt(0));
        assertEquals(0, s.currentTerm());
        assertEquals(-1, s.votedFor());
        assertTrue(s.entries(1, 10).isEmpty());
    }

    @Test
    void unsyncedWritesAreLostOnCrash() {
        InMemoryRaftStorage s = new InMemoryRaftStorage();
        s.setTermAndVote(3, 2);
        s.append(List.of(e(3, "a"), e(3, "b")));
        assertTrue(s.hasUnsyncedWrites());
        assertEquals(2, s.lastIndex());                // the node itself sees its pending writes

        s.crash();
        assertEquals(0, s.currentTerm());
        assertEquals(-1, s.votedFor());
        assertEquals(0, s.lastIndex());
        assertFalse(s.hasUnsyncedWrites());
    }

    @Test
    void syncedWritesSurviveACrashAndLaterUnsyncedOnesDoNot() {
        InMemoryRaftStorage s = new InMemoryRaftStorage();
        s.setTermAndVote(1, 1);
        s.append(List.of(e(1, "kept")));
        s.sync();
        s.setTermAndVote(2, -1);
        s.append(List.of(e(2, "lost")));
        s.crash();

        assertEquals(1, s.currentTerm());
        assertEquals(1, s.votedFor());
        assertEquals(1, s.lastIndex());
        assertEquals("kept", new String(s.entryAt(1).command()));
    }

    @Test
    void truncateIsAlsoOnlyDurableAfterSync() {
        InMemoryRaftStorage s = new InMemoryRaftStorage();
        s.append(List.of(e(1, "a"), e(1, "b"), e(1, "c")));
        s.sync();
        s.truncateFrom(2);
        assertEquals(1, s.lastIndex());
        s.crash();
        assertEquals(3, s.lastIndex());               // the truncation never reached the disk
        s.truncateFrom(2);
        s.sync();
        s.crash();
        assertEquals(1, s.lastIndex());
    }

    @Test
    void truncateFromJustPastTheEndIsANoOpAndFurtherIsRejected() {
        InMemoryRaftStorage s = new InMemoryRaftStorage();
        s.append(List.of(e(1, "a")));
        s.truncateFrom(2);
        assertEquals(1, s.lastIndex());
        assertThrows(IllegalArgumentException.class, () -> s.truncateFrom(3));
        assertThrows(IllegalArgumentException.class, () -> s.truncateFrom(0));
    }

    @Test
    void termAtAndEntryAtRespectBounds() {
        InMemoryRaftStorage s = new InMemoryRaftStorage();
        s.append(List.of(e(1, "a"), e(4, "b")));
        assertEquals(1, s.termAt(1));
        assertEquals(4, s.termAt(2));
        assertEquals(4, s.lastTerm());
        assertThrows(IllegalArgumentException.class, () -> s.termAt(3));
        assertThrows(IllegalArgumentException.class, () -> s.entryAt(0));
    }

    @Test
    void entriesReturnsAtMostMaxCountFromTheGivenIndex() {
        InMemoryRaftStorage s = new InMemoryRaftStorage();
        s.append(List.of(e(1, "a"), e(1, "b"), e(2, "c"), e(2, "d")));
        assertEquals(List.of(e(1, "b"), e(2, "c")), s.entries(2, 2));
        assertEquals(List.of(e(2, "d")), s.entries(4, 10));
        assertTrue(s.entries(5, 10).isEmpty());
        assertTrue(s.entries(1, 0).isEmpty());
    }

    @Test
    void returnedListsAreCopiesNotViews() {
        InMemoryRaftStorage s = new InMemoryRaftStorage();
        s.append(List.of(e(1, "a"), e(1, "b")));
        List<LogEntry> got = s.entries(1, 10);
        s.truncateFrom(1);
        assertEquals(2, got.size());                  // truncating the log did not change what we were given
    }

    @Test
    void thePrePopulatedConstructorStartsDurable() {
        InMemoryRaftStorage s = new InMemoryRaftStorage(5, 3, List.of(e(1, "a"), e(5, "b")));
        assertEquals(5, s.currentTerm());
        assertEquals(3, s.votedFor());
        assertEquals(2, s.lastIndex());
        assertFalse(s.hasUnsyncedWrites());
        s.append(List.of(e(5, "c")));
        s.crash();
        assertEquals(2, s.lastIndex());
        assertEquals(5, s.durableTerm());
        assertEquals(3, s.durableVotedFor());
    }

    @Test
    void termAndVoteChangeTogether() {
        InMemoryRaftStorage s = new InMemoryRaftStorage();
        s.setTermAndVote(4, 2);
        s.sync();
        s.setTermAndVote(5, -1);
        s.crash();
        assertEquals(4, s.currentTerm());              // never "term 5 with the vote from term 4" or vice versa
        assertEquals(2, s.votedFor());
    }

    @Test
    void syncWithNothingToDoStillCountsAsABarrier() {
        InMemoryRaftStorage s = new InMemoryRaftStorage();
        s.sync();
        s.sync();
        assertEquals(2, s.syncCount());
    }
}
