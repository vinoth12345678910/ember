package com.ember.replication;

import com.ember.protocol.PartitionOffset;
import com.ember.protocol.PartitionRef;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GroupCommandsTest {

    private static final GroupCommands.SetMembership MEMBERSHIP = new GroupCommands.SetMembership("g", 2, 3, List.of(
            new GroupCommands.Member("m1", "c1", 10_000, List.of("a", "b"), List.of(new PartitionRef("a", 0), new PartitionRef("b", 1))),
            new GroupCommands.Member("m2", "c2", 5_000, List.of("a"), List.of())));

    private static final GroupCommands.OffsetCommit COMMIT = new GroupCommands.OffsetCommit("g", "m1", 3,
            List.of(new PartitionOffset("a", 0, 42), new PartitionOffset("b", 1, 0)));

    @Test
    void membershipRoundTrips() {
        byte[] bytes = GroupCommands.encode(MEMBERSHIP);
        assertEquals(GroupCommands.SET_MEMBERSHIP, GroupCommands.type(bytes));
        assertEquals(MEMBERSHIP, GroupCommands.decodeSetMembership(bytes));
        GroupCommands.SetMembership empty = new GroupCommands.SetMembership("g", 0, 1, List.of());
        assertEquals(empty, GroupCommands.decodeSetMembership(GroupCommands.encode(empty)));
    }

    @Test
    void commitRoundTrips() {
        byte[] bytes = GroupCommands.encode(COMMIT);
        assertEquals(GroupCommands.OFFSET_COMMIT, GroupCommands.type(bytes));
        assertEquals(COMMIT, GroupCommands.decodeOffsetCommit(bytes));
    }

    @Test
    void everyTruncationIsRejected() {
        byte[] m = GroupCommands.encode(MEMBERSHIP);
        for (int len = 0; len < m.length; len++) {
            byte[] cut = Arrays.copyOf(m, len);
            assertThrows(IllegalArgumentException.class, () -> GroupCommands.decodeSetMembership(cut), "membership cut at " + len);
        }
        byte[] c = GroupCommands.encode(COMMIT);
        for (int len = 0; len < c.length; len++) {
            byte[] cut = Arrays.copyOf(c, len);
            assertThrows(IllegalArgumentException.class, () -> GroupCommands.decodeOffsetCommit(cut), "commit cut at " + len);
        }
    }

    @Test
    void trailingBytesAndWrongTypesAreRejected() {
        byte[] m = Arrays.copyOf(GroupCommands.encode(MEMBERSHIP), GroupCommands.encode(MEMBERSHIP).length + 1);
        assertThrows(IllegalArgumentException.class, () -> GroupCommands.decodeSetMembership(m));
        assertThrows(IllegalArgumentException.class, () -> GroupCommands.decodeOffsetCommit(GroupCommands.encode(MEMBERSHIP)));
        assertThrows(IllegalArgumentException.class, () -> GroupCommands.decodeSetMembership(GroupCommands.encode(COMMIT)));
        assertThrows(IllegalArgumentException.class, () -> GroupCommands.type(new byte[0]));
    }

    @Test
    void negativeAndHugeCountsAreRejectedWithoutAllocating() {
        for (int bad : new int[]{-1, 10_001, Integer.MAX_VALUE}) {
            byte[] c = GroupCommands.encode(new GroupCommands.OffsetCommit("g", "m", 1, List.of()));
            ByteBuffer.wrap(c).putInt(c.length - 4, bad);
            assertThrows(IllegalArgumentException.class, () -> GroupCommands.decodeOffsetCommit(c));
        }
    }
}
