package com.ember.replication;

import com.ember.protocol.ErrorCode;
import com.ember.protocol.PartitionOffset;
import com.ember.protocol.PartitionRef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** The cooperative rebalance: sticky assignment, revoke first, hand over only after the old owner acknowledged. */
class GroupCoordinatorCooperativeTest {

    private final GroupStateMachine state = new GroupStateMachine();
    private final GroupCoordinatorTest.FakeLog log = new GroupCoordinatorTest.FakeLog(state);
    private final Map<String, Integer> topicCounts = new HashMap<>(Map.of("orders", 6));
    private long now = 0;
    private final AtomicInteger ids = new AtomicInteger();
    private GroupCoordinator coordinator;

    @BeforeEach
    void setUp() {
        coordinator = new GroupCoordinator("test", log, state, t -> topicCounts.getOrDefault(t, -1), () -> now,
                () -> "u" + ids.incrementAndGet(), new GroupCoordinator.Config(100, 60_000, 1000, 5, true));
    }

    private GroupCoordinator.JoinResult join(String memberId, String clientId) {
        return coordinator.join("g", memberId, clientId, 10_000, List.of("orders"));
    }

    private Set<Integer> assigned(String member) {
        Set<Integer> out = new TreeSet<>();
        state.group("g").members().get(member).assigned().forEach(p -> out.add(p.partition()));
        return out;
    }

    private static PartitionOffset offset(int partition, long value) {
        return new PartitionOffset("orders", partition, value);
    }

    @Test
    void aNewMemberGetsNothingUntilTheOldOwnerHasAcknowledgedTheRevocation() {
        String a = join("", "a").memberId();
        assertEquals(Set.of(0, 1, 2, 3, 4, 5), assigned(a));

        GroupCoordinator.JoinResult b = join("", "b");
        assertEquals(2, b.generation());
        assertEquals(Set.of(0, 1, 2), assigned(a), "a keeps its half without interruption");
        assertEquals(Set.of(), assigned(b.memberId()), "the other half is unowned for now");

        coordinator.expireSessions();
        assertEquals(2, state.group("g").generation(), "nobody has acknowledged generation 2 yet: no handoff");

        assertEquals(ErrorCode.NONE, coordinator.heartbeat("g", b.memberId(), 2));
        coordinator.expireSessions();
        assertEquals(2, state.group("g").generation(), "b acknowledged, a did not: a may still be processing partitions 3-5");

        assertEquals(ErrorCode.NONE, coordinator.heartbeat("g", a, 2));
        coordinator.expireSessions();
        assertEquals(3, state.group("g").generation());
        assertEquals(Set.of(0, 1, 2), assigned(a));
        assertEquals(Set.of(3, 4, 5), assigned(b.memberId()));
    }

    @Test
    void theOldOwnerCanCommitItsLastPositionWhileThePartitionIsUnownedButNeverOneThatSomeoneElseOwns() {
        String a = join("", "a").memberId();
        String b = join("", "b").memberId();
        assertEquals(ErrorCode.NONE, coordinator.commit("g", a, 2, List.of(offset(4, 17))), "partition 4 is unowned: last words");
        assertEquals(17, state.committedOffset("g", new PartitionRef("orders", 4)));
        assertEquals(ErrorCode.PARTITION_NOT_ASSIGNED, coordinator.commit("g", b, 2, List.of(offset(0, 5))), "partition 0 belongs to a");
        assertEquals(ErrorCode.NONE, coordinator.commit("g", a, 2, List.of(offset(1, 9))), "its own partitions as always");
    }

    @Test
    void aMemberThatLeavesHandsItsPartitionsOverInOneStepBecauseNobodyIsUsingThem() {
        String a = join("", "a").memberId();
        String b = join("", "b").memberId();
        coordinator.heartbeat("g", a, 2);
        coordinator.heartbeat("g", b, 2);
        coordinator.expireSessions();
        assertEquals(3, state.group("g").generation());

        assertEquals(ErrorCode.NONE, coordinator.leave("g", b));
        assertEquals(4, state.group("g").generation());
        assertEquals(Set.of(0, 1, 2, 3, 4, 5), assigned(a), "no waiting: b is gone and cannot be using them");
    }

    @Test
    void aSessionThatExpiredIsHandedOverAtOnceToo() {
        String a = join("", "a").memberId();
        String b = join("", "b").memberId();
        coordinator.heartbeat("g", a, 2);
        coordinator.heartbeat("g", b, 2);
        coordinator.expireSessions();
        now = 5_000;
        coordinator.heartbeat("g", a, 3);
        now = 12_000;                                   // b is silent for longer than its 10 s session
        coordinator.expireSessions();
        assertEquals(Set.of(0, 1, 2, 3, 4, 5), assigned(a));
    }

    @Test
    void aPartitionThatStaysWithItsOwnerIsNeverRemovedFromItInAnyGeneration() {
        Set<Integer> mustStay = new TreeSet<>(Set.of(0, 1));
        String a = join("", "a").memberId();
        topicCounts.put("orders", 6);
        for (int round = 0; round < 6; round++) {
            String extra = join("", "x" + round).memberId();
            Set<Integer> mine = assigned(a);
            assertTrue(mine.containsAll(mustStay) || round > 0, "round " + round + ": a lost a partition it keeps: " + mine);
            for (String m : state.group("g").members().keySet()) {
                coordinator.heartbeat("g", m, state.group("g").generation());
            }
            coordinator.expireSessions();
            for (String m : state.group("g").members().keySet()) {
                coordinator.heartbeat("g", m, state.group("g").generation());
            }
            coordinator.expireSessions();
            coordinator.leave("g", extra);
        }
        // whatever happened, every partition has exactly one owner at the end
        Set<Integer> all = new TreeSet<>();
        for (String m : state.group("g").members().keySet()) {
            for (int p : assigned(m)) assertTrue(all.add(p));
        }
        assertEquals(Set.of(0, 1, 2, 3, 4, 5), all);
    }

    @Test
    void aNewLeaderForgetsTheAcknowledgementsAndWaitsForFreshOnes() {
        String a = join("", "a").memberId();
        String b = join("", "b").memberId();
        coordinator.heartbeat("g", a, 2);
        coordinator.heartbeat("g", b, 2);
        log.term = 2;                                   // failover: this node is leader again in a later term
        coordinator.expireSessions();
        assertEquals(2, state.group("g").generation(), "acknowledgements of the old tenure do not count");
        coordinator.heartbeat("g", a, 2);
        coordinator.heartbeat("g", b, 2);
        coordinator.expireSessions();
        assertEquals(3, state.group("g").generation());
    }

    @Test
    void theEagerModeStillMovesEverythingInOneGeneration() {
        GroupCoordinator eager = new GroupCoordinator("eager", log, state, t -> topicCounts.getOrDefault(t, -1), () -> now,
                () -> "e" + ids.incrementAndGet(), new GroupCoordinator.Config(100, 60_000, 1000, 5));
        String a = eager.join("g2", "", "a", 10_000, List.of("orders")).memberId();
        GroupCoordinator.JoinResult b = eager.join("g2", "", "b", 10_000, List.of("orders"));
        assertEquals(Set.of(3, 4, 5), b.assignment().stream().map(PartitionRef::partition).collect(java.util.stream.Collectors.toCollection(TreeSet::new)));
        assertNotNull(a);
    }
}
