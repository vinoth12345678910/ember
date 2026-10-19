package com.ember.replication;

import com.ember.protocol.DescribeGroupResponse;
import com.ember.protocol.ErrorCode;
import com.ember.protocol.PartitionOffset;
import com.ember.protocol.PartitionRef;
import com.ember.raft.LogEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/** No threads and no real time: a manual clock and a fake log that applies commands straight to a real state machine. */
class GroupCoordinatorTest {

    /** Applies every proposal immediately; a hook lets a test slip a competing command in first. */
    static final class FakeLog implements GroupLog {
        final GroupStateMachine state;
        boolean leader = true, ready = true;
        int leaderId = 1;
        long term = 1;
        final List<byte[]> proposals = new ArrayList<>();
        Consumer<byte[]> beforeApply = c -> { };
        RuntimeException failWith;
        long index;

        FakeLog(GroupStateMachine state) {
            this.state = state;
        }

        @Override public boolean isLeader() { return leader; }
        @Override public boolean isLeaderReady() { return ready; }
        @Override public int leaderId() { return leaderId; }
        @Override public long term() { return term; }

        @Override
        public CompletableFuture<Object> propose(byte[] command, long timeoutMs) {
            proposals.add(command);
            if (failWith != null) {
                return CompletableFuture.failedFuture(failWith);
            }
            beforeApply.accept(command);
            return CompletableFuture.completedFuture(state.apply(++index, LogEntry.command(1, command)));
        }

        void applyDirectly(byte[] command) {
            state.apply(++index, LogEntry.command(1, command));
        }
    }

    private final GroupStateMachine state = new GroupStateMachine();
    private final FakeLog log = new FakeLog(state);
    private final Map<String, Integer> topicCounts = new HashMap<>(Map.of("orders", 6));
    private long now = 0;
    private final AtomicInteger ids = new AtomicInteger();
    private GroupCoordinator coordinator;

    @BeforeEach
    void setUp() {
        coordinator = new GroupCoordinator("test", log, state, t -> topicCounts.getOrDefault(t, -1), () -> now,
                () -> "u" + ids.incrementAndGet(), new GroupCoordinator.Config(100, 60_000, 1000, 5));
    }

    private GroupCoordinator.JoinResult join(String memberId, String clientId, int session, String... topics) {
        return coordinator.join("g", memberId, clientId, session, List.of(topics));
    }

    private static Set<Integer> partitionsOf(GroupCoordinator.JoinResult r) {
        Set<Integer> out = new TreeSet<>();
        r.assignment().forEach(p -> out.add(p.partition()));
        return out;
    }

    @Test
    void theFirstMemberJoinsGetsGenerationOneAndEveryPartition() {
        GroupCoordinator.JoinResult r = join("", "a", 10_000, "orders");
        assertEquals(ErrorCode.NONE, r.error());
        assertEquals(1, r.generation());
        assertEquals("a-u1", r.memberId());
        assertEquals(Set.of(0, 1, 2, 3, 4, 5), partitionsOf(r));
    }

    @Test
    void aSecondMemberStartsANewGenerationAndSplitsThePartitions() {
        GroupCoordinator.JoinResult first = join("", "a", 10_000, "orders");
        GroupCoordinator.JoinResult second = join("", "b", 10_000, "orders");
        assertEquals(2, second.generation());
        assertEquals(Set.of(3, 4, 5), partitionsOf(second));
        GroupStateMachine.GroupData g = state.group("g");
        assertEquals(Set.of(0, 1, 2), new TreeSet<>(g.members().get(first.memberId()).assigned().stream().map(PartitionRef::partition).toList()));
        assertEquals(ErrorCode.REBALANCE_IN_PROGRESS, coordinator.heartbeat("g", first.memberId(), 1), "the first member finds out on its next heartbeat");
    }

    @Test
    void rejoiningWithTheSameSubscriptionDoesNotStartAGeneration() {
        GroupCoordinator.JoinResult a = join("", "a", 10_000, "orders");
        GroupCoordinator.JoinResult b = join("", "b", 10_000, "orders");
        int proposals = log.proposals.size();
        GroupCoordinator.JoinResult again = join(a.memberId(), "a", 10_000, "orders");
        assertEquals(ErrorCode.NONE, again.error());
        assertEquals(2, again.generation(), "it just picks up the current assignment");
        assertEquals(Set.of(0, 1, 2), partitionsOf(again));
        assertEquals(proposals, log.proposals.size(), "nothing was proposed");
        assertEquals(2, state.group("g").generation());
        assertNotNull(b);
    }

    @Test
    void aChangedSubscriptionOrSessionIsARealChange() {
        GroupCoordinator.JoinResult a = join("", "a", 10_000, "orders");
        assertEquals(2, join(a.memberId(), "a", 10_000, "orders", "extra").generation());
        assertEquals(3, join(a.memberId(), "a", 20_000, "orders", "extra").generation());
    }

    @Test
    void anUnknownMemberIdMustStartOverWithAnEmptyOne() {
        assertEquals(ErrorCode.UNKNOWN_MEMBER_ID, join("ghost-1", "ghost", 10_000, "orders").error());
        assertNull(state.group("g"));
    }

    @Test
    void heartbeatsAnswerAccordingToTheGeneration() {
        GroupCoordinator.JoinResult a = join("", "a", 10_000, "orders");
        assertEquals(ErrorCode.NONE, coordinator.heartbeat("g", a.memberId(), 1));
        assertEquals(ErrorCode.ILLEGAL_GENERATION, coordinator.heartbeat("g", a.memberId(), 2), "nobody may be ahead of the leader");
        join("", "b", 10_000, "orders");
        assertEquals(ErrorCode.REBALANCE_IN_PROGRESS, coordinator.heartbeat("g", a.memberId(), 1));
        assertEquals(ErrorCode.UNKNOWN_MEMBER_ID, coordinator.heartbeat("g", "ghost", 2));
        assertEquals(ErrorCode.UNKNOWN_MEMBER_ID, coordinator.heartbeat("nogroup", a.memberId(), 1));
        int proposals = log.proposals.size();
        coordinator.heartbeat("g", a.memberId(), 2);
        assertEquals(proposals, log.proposals.size(), "heartbeats never touch Raft");
    }

    @Test
    void sessionExpiryRemovesOnlyTheSilentMember() {
        GroupCoordinator.JoinResult a = join("", "a", 1000, "orders");
        GroupCoordinator.JoinResult b = join("", "b", 1000, "orders");
        now = 800;
        assertEquals(ErrorCode.NONE, coordinator.heartbeat("g", b.memberId(), 2));
        now = 1500;                                                    // a: 1500 since its join, b: 700 since its heartbeat
        coordinator.expireSessions();
        GroupStateMachine.GroupData g = state.group("g");
        assertEquals(3, g.generation());
        assertEquals(Set.of(b.memberId()), g.members().keySet());
        assertEquals(6, g.members().get(b.memberId()).assigned().size(), "the survivor takes everything");
        assertEquals(ErrorCode.UNKNOWN_MEMBER_ID, coordinator.heartbeat("g", a.memberId(), 2));
        assertEquals(1, coordinator.rebalances() - 2, "one rebalance caused by the expiry (the two joins counted before)");
    }

    @Test
    void twoMembersExpiringTogetherAreRemovedByOneCommand() {
        join("", "a", 1000, "orders");
        join("", "b", 1000, "orders");
        join("", "c", 1000, "orders");
        int proposals = log.proposals.size();
        now = 5000;
        coordinator.expireSessions();
        assertEquals(proposals + 1, log.proposals.size(), "ONE command for all expired members");
        assertEquals(4, state.group("g").generation(), "and only one generation was spent");
        assertTrue(state.group("g").members().isEmpty());
        coordinator.expireSessions();
        assertEquals(proposals + 1, log.proposals.size(), "nothing left to expire");
    }

    @Test
    void membersWithDifferentTimeoutsExpireIndependently() {
        GroupCoordinator.JoinResult shortLived = join("", "short", 200, "orders");
        GroupCoordinator.JoinResult longLived = join("", "long", 10_000, "orders");
        now = 300;
        coordinator.expireSessions();
        assertEquals(Set.of(longLived.memberId()), state.group("g").members().keySet());
        assertNotNull(shortLived);
    }

    @Test
    void leavingRemovesTheMemberAndRecomputesTheAssignment() {
        GroupCoordinator.JoinResult a = join("", "a", 10_000, "orders");
        GroupCoordinator.JoinResult b = join("", "b", 10_000, "orders");
        assertEquals(ErrorCode.NONE, coordinator.leave("g", a.memberId()));
        GroupStateMachine.GroupData g = state.group("g");
        assertEquals(3, g.generation());
        assertEquals(6, g.members().get(b.memberId()).assigned().size());
        assertEquals(ErrorCode.UNKNOWN_MEMBER_ID, coordinator.leave("g", a.memberId()));
        assertEquals(ErrorCode.UNKNOWN_MEMBER_ID, coordinator.leave("nogroup", "x"));
        assertEquals(ErrorCode.NONE, coordinator.leave("g", b.memberId()));
        assertEquals(DescribeGroupResponse.EMPTY, coordinator.describe("g").body().state());
    }

    @Test
    void aJoinThatLosesTheCompareAndSetRecomputesAndConverges() {
        join("", "a", 10_000, "orders");                                        // generation 1
        boolean[] injected = {false};
        log.beforeApply = command -> {
            if (!injected[0]) {                                                  // a competing join commits first
                injected[0] = true;
                GroupStateMachine.GroupData g = state.group("g");
                Map<String, GroupCommands.Member> members = new TreeMap<>(g.members());
                members.put("rival-1", new GroupCommands.Member("rival-1", "rival", 10_000, List.of("orders"), List.of()));
                log.applyDirectly(GroupCommands.encode(new GroupCommands.SetMembership("g", 1, 2, new ArrayList<>(members.values()))));
            }
        };
        GroupCoordinator.JoinResult b = join("", "b", 10_000, "orders");
        assertEquals(ErrorCode.NONE, b.error());
        assertEquals(3, b.generation(), "rival took generation 2, our retry built generation 3 on top of it");
        assertEquals(3, state.group("g").members().size(), "a, rival and b all present");
        assertEquals(3, log.proposals.size(), "join a, first b attempt (stale), second b attempt");
    }

    @Test
    void ifTheCompareAndSetKeepsLosingTheJoinGivesUpWithRebalanceInProgress() {
        join("", "a", 10_000, "orders");
        int[] generation = {1};
        log.beforeApply = command -> {
            GroupStateMachine.GroupData g = state.group("g");
            log.applyDirectly(GroupCommands.encode(new GroupCommands.SetMembership("g", g.generation(), g.generation() + 1, new ArrayList<>(g.members().values()))));
            generation[0]++;
        };
        assertEquals(ErrorCode.REBALANCE_IN_PROGRESS, join("", "b", 10_000, "orders").error());
        assertEquals(5, log.proposals.size() - 1, "exactly the configured number of attempts");
    }

    @Test
    void aNewLeaderGivesEveryMemberAFullSessionOfGrace() {
        join("", "a", 1000, "orders");                                          // heartbeat at t=0, term 1
        now = 900;
        log.term = 2;                                                            // failover: this node leads again in a new term
        coordinator.expireSessions();                                            // first look at the new term: the clock starts HERE
        assertEquals(1, state.group("g").generation(), "900 ms of silence is within the session anyway");
        now = 1500;                                                              // 1500 since the real last heartbeat, 600 since takeover
        coordinator.expireSessions();
        assertEquals(1, state.group("g").generation(), "not expired right after the failover");
        assertEquals(1, state.group("g").members().size());
        now = 2000;                                                              // a full timeout (1100 ms) after takeover
        coordinator.expireSessions();
        assertEquals(2, state.group("g").generation());
        assertTrue(state.group("g").members().isEmpty());
    }

    @Test
    void sessionTimeoutsOutsideTheAllowedRangeAreRejected() {
        assertEquals(ErrorCode.INVALID_SESSION_TIMEOUT, join("", "a", 99, "orders").error());
        assertEquals(ErrorCode.INVALID_SESSION_TIMEOUT, join("", "a", 60_001, "orders").error());
        assertEquals(ErrorCode.NONE, join("", "a", 100, "orders").error());
        assertEquals(ErrorCode.NONE, join("", "b", 60_000, "orders").error());
    }

    @Test
    void joiningWithoutTopicsIsRejected() {
        assertEquals(ErrorCode.INVALID_REQUEST, join("", "a", 10_000).error());
    }

    @Test
    void aTopicThatDoesNotExistYetGivesAnEmptyAssignmentNotAnError() {
        GroupCoordinator.JoinResult r = join("", "a", 10_000, "future-topic");
        assertEquals(ErrorCode.NONE, r.error());
        assertEquals(List.of(), r.assignment());
    }

    @Test
    void everyCallIsRefusedWhenThisBrokerIsNotTheReadyLeader() {
        log.leader = false;
        log.leaderId = 2;
        assertEquals(ErrorCode.NOT_COORDINATOR, join("", "a", 10_000, "orders").error());
        assertEquals(ErrorCode.NOT_COORDINATOR, coordinator.heartbeat("g", "m", 1));
        assertEquals(ErrorCode.NOT_COORDINATOR, coordinator.leave("g", "m"));
        assertEquals(ErrorCode.NOT_COORDINATOR, coordinator.commit("g", "m", 1, List.of()));
        assertEquals(ErrorCode.NOT_COORDINATOR, coordinator.fetchOffsets("g", List.of()).error());
        assertEquals(ErrorCode.NOT_COORDINATOR, coordinator.describe("g").error());
        log.leaderId = -1;
        assertEquals(ErrorCode.COORDINATOR_NOT_AVAILABLE, join("", "a", 10_000, "orders").error());
        log.leader = true;
        log.ready = false;
        assertEquals(ErrorCode.COORDINATOR_NOT_AVAILABLE, join("", "a", 10_000, "orders").error());
        assertTrue(log.proposals.isEmpty());
    }

    @Test
    void offsetsCanBeCommittedFetchedAndAreFencedByGeneration() {
        GroupCoordinator.JoinResult a = join("", "a", 10_000, "orders");
        PartitionOffset p0 = new PartitionOffset("orders", 0, 42);
        assertEquals(ErrorCode.NONE, coordinator.commit("g", a.memberId(), 1, List.of(p0)));
        GroupCoordinator.FetchResult f = coordinator.fetchOffsets("g", List.of(new PartitionRef("orders", 0), new PartitionRef("orders", 1)));
        assertEquals(List.of(p0, new PartitionOffset("orders", 1, -1)), f.offsets());

        GroupCoordinator.JoinResult b = join("", "b", 10_000, "orders");          // generation 2: a keeps 0..2, b gets 3..5
        assertEquals(ErrorCode.ILLEGAL_GENERATION, coordinator.commit("g", a.memberId(), 1, List.of(new PartitionOffset("orders", 0, 99))), "a zombie with the old generation");
        assertEquals(ErrorCode.PARTITION_NOT_ASSIGNED, coordinator.commit("g", a.memberId(), 2, List.of(new PartitionOffset("orders", 4, 1))));
        assertEquals(ErrorCode.UNKNOWN_MEMBER_ID, coordinator.commit("g", "ghost", 2, List.of(p0)));
        assertEquals(ErrorCode.NONE, coordinator.commit("g", b.memberId(), 2, List.of(new PartitionOffset("orders", 4, 7))));
        assertEquals(42, coordinator.fetchOffsets("g", List.of(new PartitionRef("orders", 0))).offsets().get(0).offset(), "the zombie changed nothing");
    }

    @Test
    void theApplyTimeCheckStillFencesWhenThePreCheckPassed() {
        GroupCoordinator.JoinResult a = join("", "a", 10_000, "orders");
        log.beforeApply = command -> {                                           // a rebalance commits while the commit is in flight
            if (GroupCommands.type(command) == GroupCommands.OFFSET_COMMIT) {
                GroupStateMachine.GroupData g = state.group("g");
                log.applyDirectly(GroupCommands.encode(new GroupCommands.SetMembership("g", g.generation(), g.generation() + 1,
                        List.of(new GroupCommands.Member("other", "o", 10_000, List.of("orders"), List.of(new PartitionRef("orders", 0)))))));
            }
        };
        assertEquals(ErrorCode.UNKNOWN_MEMBER_ID, coordinator.commit("g", a.memberId(), 1, List.of(new PartitionOffset("orders", 0, 5))));
        assertEquals(-1, state.committedOffset("g", new PartitionRef("orders", 0)));
    }

    @Test
    void describeShowsMembersAssignmentsAndOffsets() {
        assertEquals(DescribeGroupResponse.UNKNOWN, coordinator.describe("g").body().state());
        GroupCoordinator.JoinResult a = join("", "a", 10_000, "orders");
        coordinator.commit("g", a.memberId(), 1, List.of(new PartitionOffset("orders", 2, 11)));
        DescribeGroupResponse d = coordinator.describe("g").body();
        assertEquals(DescribeGroupResponse.STABLE, d.state());
        assertEquals(1, d.generation());
        assertEquals(1, d.members().size());
        assertEquals(6, d.members().get(0).assignment().size());
        assertEquals(List.of(new PartitionOffset("orders", 2, 11)), d.offsets());
    }

    @Test
    void raftFailuresAreMappedToRetriableErrors() {
        log.failWith = new GroupRuntime.NotLeaderException(2);
        assertEquals(ErrorCode.NOT_COORDINATOR, join("", "a", 10_000, "orders").error());
        log.failWith = new GroupRuntime.ProposeTimeoutException();
        assertEquals(ErrorCode.REQUEST_TIMED_OUT, join("", "a", 10_000, "orders").error());
        log.failWith = new GroupRuntime.OverloadedException();
        assertEquals(ErrorCode.BROKER_OVERLOADED, join("", "a", 10_000, "orders").error());
        log.failWith = null;
        assertEquals(ErrorCode.NONE, join("", "a", 10_000, "orders").error());
    }

    @Test
    void theExpiryTickIsAQuarterOfTheShortestSessionInUseAtLeast50Ms() {
        assertEquals(50, coordinator.tickIntervalMs(), "no members: min session 100 ms / 4 = 25, floored at 50");
        join("", "a", 8000, "orders");
        assertEquals(2000, coordinator.tickIntervalMs());
        join("", "b", 1200, "orders");
        assertEquals(300, coordinator.tickIntervalMs());
        join("", "c", 100, "orders");
        assertEquals(50, coordinator.tickIntervalMs());
    }
}
