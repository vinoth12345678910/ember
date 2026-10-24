package com.ember.replication;

import com.ember.protocol.PartitionOffset;
import com.ember.protocol.PartitionRef;
import com.ember.raft.LogEntry;
import org.junit.jupiter.api.Test;

import java.util.*;

import static com.ember.replication.GroupStateMachine.Result.*;
import static org.junit.jupiter.api.Assertions.*;

class GroupStateMachineTest {

    private final GroupStateMachine sm = new GroupStateMachine();
    private long index = 0;

    private static GroupCommands.Member member(String id, String... partitions) {
        List<PartitionRef> assigned = new ArrayList<>();
        for (String p : partitions) {
            String[] parts = p.split("-");
            assigned.add(new PartitionRef(parts[0], Integer.parseInt(parts[1])));
        }
        return new GroupCommands.Member(id, "client-" + id, 10_000, List.of("t"), assigned);
    }

    private Object membership(String group, int base, GroupCommands.Member... members) {
        return sm.apply(++index, LogEntry.command(1, GroupCommands.encode(new GroupCommands.SetMembership(group, base, base + 1, List.of(members)))));
    }

    private Object commit(String group, String member, int generation, PartitionOffset... offsets) {
        return sm.apply(++index, LogEntry.command(1, GroupCommands.encode(new GroupCommands.OffsetCommit(group, member, generation, List.of(offsets)))));
    }

    private static PartitionOffset off(String topic, int partition, long offset) {
        return new PartitionOffset(topic, partition, offset);
    }

    @Test
    void creatingAGroupNeedsBaseGenerationZero() {
        assertEquals(OK, membership("g", 0, member("m1", "t-0", "t-1")));
        GroupStateMachine.GroupData g = sm.group("g");
        assertEquals(1, g.generation());
        assertEquals(List.of(new PartitionRef("t", 0), new PartitionRef("t", 1)), g.members().get("m1").assigned());
        assertEquals(STALE_GENERATION, membership("new", 5, member("m1")), "a group that does not exist has generation 0");
        assertNull(sm.group("new"));
    }

    @Test
    void changingMembershipBumpsTheGenerationAndReplacesMembers() {
        membership("g", 0, member("m1", "t-0", "t-1"));
        assertEquals(OK, membership("g", 1, member("m1", "t-0"), member("m2", "t-1")));
        GroupStateMachine.GroupData g = sm.group("g");
        assertEquals(2, g.generation());
        assertEquals(Set.of("m1", "m2"), g.members().keySet());
        assertEquals(OK, membership("g", 2));
        assertEquals(3, sm.group("g").generation());
        assertTrue(sm.group("g").members().isEmpty(), "an empty group is legal");
    }

    @Test
    void aStaleGenerationChangesNothing() {
        membership("g", 0, member("m1", "t-0"));
        commit("g", "m1", 1, off("t", 0, 5));
        Map<String, GroupStateMachine.GroupData> before = sm.snapshot();
        assertEquals(STALE_GENERATION, membership("g", 0, member("x", "t-0")));
        assertEquals(STALE_GENERATION, membership("g", 7, member("x", "t-0")));
        assertEquals(before, sm.snapshot());
    }

    @Test
    void offsetsSurviveMembershipChangesEvenToAnEmptyGroup() {
        membership("g", 0, member("m1", "t-0"));
        commit("g", "m1", 1, off("t", 0, 9));
        membership("g", 1);
        membership("g", 2, member("m9", "t-0"));
        assertEquals(9, sm.committedOffset("g", new PartitionRef("t", 0)));
        assertEquals(-1, sm.committedOffset("g", new PartitionRef("t", 1)));
        assertEquals(-1, sm.committedOffset("nope", new PartitionRef("t", 0)));
    }

    @Test
    void aValidCommitIsStoredAndLaterOnesOverwrite() {
        membership("g", 0, member("m1", "t-0", "t-1"));
        assertEquals(OK, commit("g", "m1", 1, off("t", 0, 5), off("t", 1, 7)));
        assertEquals(OK, commit("g", "m1", 1, off("t", 0, 6)));
        assertEquals(6, sm.committedOffset("g", new PartitionRef("t", 0)));
        assertEquals(7, sm.committedOffset("g", new PartitionRef("t", 1)));
    }

    @Test
    void commitRejectionsAndTheirPriority() {
        membership("g", 0, member("m1", "t-0"));
        assertEquals(UNKNOWN_MEMBER_ID, commit("nogroup", "m1", 1, off("t", 0, 1)));
        assertEquals(UNKNOWN_MEMBER_ID, commit("g", "ghost", 1, off("t", 0, 1)));
        assertEquals(ILLEGAL_GENERATION, commit("g", "m1", 0, off("t", 0, 1)));
        assertEquals(ILLEGAL_GENERATION, commit("g", "m1", 2, off("t", 0, 1)));
        assertEquals(PARTITION_NOT_ASSIGNED, commit("g", "m1", 1, off("t", 1, 1)));
        assertEquals(PARTITION_NOT_ASSIGNED, commit("g", "m1", 1, off("other", 0, 1)));
        assertEquals(INVALID, commit("g", "m1", 1, off("t", 0, -5)));
        assertEquals(-1, sm.committedOffset("g", new PartitionRef("t", 0)), "nothing was stored by any rejected commit");
    }

    @Test
    void aCommitIsAllOrNothing() {
        membership("g", 0, member("m1", "t-0"));
        assertEquals(PARTITION_NOT_ASSIGNED, commit("g", "m1", 1, off("t", 0, 5), off("t", 3, 9)));
        assertEquals(-1, sm.committedOffset("g", new PartitionRef("t", 0)), "the valid half must not be stored");
    }

    @Test
    void aZombieWithAnOldGenerationCannotOverwriteTheNewOwner() {
        membership("g", 0, member("A", "t-0"));
        assertEquals(OK, commit("g", "A", 1, off("t", 0, 10)));
        membership("g", 1, member("B", "t-0"));                         // A was replaced
        assertEquals(OK, commit("g", "B", 2, off("t", 0, 20)));
        assertEquals(UNKNOWN_MEMBER_ID, commit("g", "A", 1, off("t", 0, 999)), "A is gone");
        membership("g", 2, member("A", "t-1"), member("B", "t-0"));      // A returns, with another partition
        assertEquals(ILLEGAL_GENERATION, commit("g", "A", 1, off("t", 1, 999)));
        assertEquals(20, sm.committedOffset("g", new PartitionRef("t", 0)));
    }

    @Test
    void inconsistentMembershipCommandsAreRejectedDeterministically() {
        assertEquals(INVALID, sm.apply(1, LogEntry.command(1, GroupCommands.encode(new GroupCommands.SetMembership("g", 0, 5, List.of())))), "new != base + 1");
        assertEquals(INVALID, membership("g", 0, member("m1", "t-0"), member("m1", "t-1")), "same member twice");
        assertEquals(INVALID, membership("g", 0, member("m1", "t-0"), member("m2", "t-0")), "a partition with two owners");
        assertTrue(sm.snapshot().isEmpty());
    }

    @Test
    void malformedBytesAreInvalidNotFatal() {
        assertEquals(INVALID, sm.apply(1, LogEntry.command(1, new byte[]{GroupCommands.OFFSET_COMMIT, 0, 1})));
        assertNull(sm.apply(2, LogEntry.noop(1)));
        assertThrows(IllegalStateException.class, () -> sm.apply(3, LogEntry.command(1, new byte[]{99})));
    }

    @Test
    void replayingTheLogIntoAFreshMachineGivesTheSameState() {
        List<LogEntry> log = new ArrayList<>();
        log.add(LogEntry.noop(1));
        log.add(LogEntry.command(1, GroupCommands.encode(new GroupCommands.SetMembership("g", 0, 1, List.of(member("m1", "t-0", "t-1"))))));
        log.add(LogEntry.command(1, GroupCommands.encode(new GroupCommands.OffsetCommit("g", "m1", 1, List.of(off("t", 0, 3))))));
        log.add(LogEntry.command(2, GroupCommands.encode(new GroupCommands.SetMembership("g", 1, 2, List.of(member("m1", "t-0"), member("m2", "t-1"))))));
        log.add(LogEntry.command(2, GroupCommands.encode(new GroupCommands.OffsetCommit("g", "m2", 2, List.of(off("t", 1, 8))))));
        log.add(LogEntry.command(2, GroupCommands.encode(new GroupCommands.SetMembership("other", 0, 1, List.of()))));
        GroupStateMachine a = new GroupStateMachine();
        GroupStateMachine b = new GroupStateMachine();
        for (int i = 0; i < log.size(); i++) a.apply(i + 1, log.get(i));
        for (int i = 0; i < log.size(); i++) b.apply(i + 1, log.get(i));
        assertEquals(a.snapshot(), b.snapshot());
        assertEquals(2, a.snapshot().size());
        assertEquals(8, a.committedOffset("g", new PartitionRef("t", 1)));
    }

    @Test
    void applyIsDeterministicOverThousandsOfRandomCommandSequences() {
        for (int seed = 0; seed < 1000; seed++) {
            Random rnd = new Random(seed);
            List<LogEntry> log = new ArrayList<>();
            for (int i = 0, n = 5 + rnd.nextInt(40); i < n; i++) {
                String group = "g" + rnd.nextInt(3);
                if (rnd.nextInt(3) == 0) {
                    List<GroupCommands.Member> members = new ArrayList<>();
                    int count = rnd.nextInt(4);
                    for (int m = 0; m < count; m++) {
                        members.add(member("m" + rnd.nextInt(4), "t-" + (m * 2), "t-" + (m * 2 + 1)));
                    }
                    int base = rnd.nextInt(5);
                    log.add(LogEntry.command(1, GroupCommands.encode(new GroupCommands.SetMembership(group, base, base + 1, members))));
                } else if (rnd.nextInt(10) == 0) {
                    log.add(LogEntry.noop(1));
                } else {
                    log.add(LogEntry.command(1, GroupCommands.encode(new GroupCommands.OffsetCommit(group, "m" + rnd.nextInt(4), rnd.nextInt(5),
                            List.of(off("t", rnd.nextInt(8), rnd.nextInt(100)))))));
                }
            }
            GroupStateMachine a = new GroupStateMachine();
            GroupStateMachine b = new GroupStateMachine();
            for (int i = 0; i < log.size(); i++) {
                Object ra = a.apply(i + 1, log.get(i));
                Object rb = b.apply(i + 1, log.get(i));
                assertEquals(ra, rb, "seed " + seed + " entry " + i);
            }
            assertEquals(a.snapshot(), b.snapshot(), "seed " + seed);
        }
    }

    @Test
    void aRevokedCommitMayStoreUnownedPartitionsButNeverOnesAnotherMemberOwns() {
        membership("g", 0, member("m1", "t-0"), member("m2", "t-1"));
        // t-2 is owned by nobody (mid-handoff)
        Object ok = sm.apply(++index, LogEntry.command(1, GroupCommands.encodeRevoked(new GroupCommands.OffsetCommit("g", "m1", 1, List.of(off("t", 2, 40))))));
        assertEquals(OK, ok);
        assertEquals(40, sm.committedOffset("g", new PartitionRef("t", 2)));
        Object refused = sm.apply(++index, LogEntry.command(1, GroupCommands.encodeRevoked(new GroupCommands.OffsetCommit("g", "m1", 1, List.of(off("t", 1, 7))))));
        assertEquals(PARTITION_NOT_ASSIGNED, refused, "t-1 belongs to m2");
        assertEquals(PARTITION_NOT_ASSIGNED, commit("g", "m1", 1, off("t", 3, 1)), "the ordinary commit stays strict");
        Object stale = sm.apply(++index, LogEntry.command(1, GroupCommands.encodeRevoked(new GroupCommands.OffsetCommit("g", "m1", 0, List.of(off("t", 2, 99))))));
        assertEquals(ILLEGAL_GENERATION, stale, "generation fencing still applies");
    }
}
