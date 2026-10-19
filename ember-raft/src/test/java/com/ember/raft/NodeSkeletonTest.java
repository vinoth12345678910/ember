package com.ember.raft;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/** Step 2: timers, nextTimerMs, the outbox order and restart behaviour. */
class NodeSkeletonTest {

    @Test
    void aNewNodeIsAFollowerWithNothingCommitted() {
        NodeHarness h = NodeHarness.cluster(1, 3);
        assertEquals(Role.FOLLOWER, h.node.role());
        assertEquals(0, h.node.currentTerm());
        assertEquals(-1, h.node.votedFor());
        assertEquals(-1, h.node.leaderId());
        assertEquals(0, h.node.commitIndex());
        assertEquals(0, h.node.lastApplied());
        assertTrue(h.sent.isEmpty());
    }

    @Test
    void electionTimeoutsAreAlwaysInsideMinMax() {
        RaftConfig config = RaftConfig.of(1, List.of(1, 2, 3)).withElectionTimeout(150, 300);
        int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
        Random random = new Random(7);
        for (int i = 0; i < 1000; i++) {
            RaftNode n = new RaftNode(config, new InMemoryRaftStorage(), (to, m) -> { }, new NodeHarnessListener(), random, 0);
            long timeout = n.nextTimerMs();
            assertTrue(timeout >= 150 && timeout <= 300, "timeout " + timeout);
            min = (int) Math.min(min, timeout);
            max = (int) Math.max(max, timeout);
        }
        assertTrue(min < 160 && max > 290, "the draws should cover the range, got " + min + ".." + max);
    }

    @Test
    void everyNewElectionAttemptDrawsAFreshDeadlineInsideTheRange() {
        NodeHarness h = NodeHarness.cluster(1, 3);
        java.util.Set<Long> gaps = new java.util.HashSet<>();
        long t = h.node.nextTimerMs();
        for (int i = 0; i < 200; i++) {
            h.advanceTo(t);                                  // election attempt: a new random deadline
            long next = h.node.nextTimerMs();
            assertTrue(next - t >= 150 && next - t <= 300, "gap " + (next - t));
            gaps.add(next - t);
            t = next;
        }
        assertTrue(gaps.size() > 20, "the deadlines should be randomized, saw only " + gaps.size() + " distinct gaps");
    }

    @Test
    void nextTimerMsIsTheElectionDeadlineAndNothingHappensBeforeIt() {
        NodeHarness h = NodeHarness.cluster(1, 3);
        long deadline = h.node.nextTimerMs();
        h.advanceTo(deadline - 1);
        assertEquals(Role.FOLLOWER, h.node.role());
        assertTrue(h.sent.isEmpty());
        assertEquals(deadline, h.node.nextTimerMs());
        h.advanceTo(deadline);
        assertEquals(Role.CANDIDATE, h.node.role());
        assertTrue(h.node.nextTimerMs() > deadline);
    }

    @Test
    void aMessageIsNeverSentBeforeStorageWasSynced() {
        NodeHarness h = NodeHarness.cluster(1, 3);
        h.fireNextTimer();                                   // becomes a candidate: term 1, vote for self, 2 RequestVotes
        assertEquals(2, h.sent.size());
        assertTrue(h.unsyncedSends.isEmpty(), h.unsyncedSends.toString());
        int syncAt = h.ops.indexOf("sync");
        int firstSend = h.ops.indexOf("send(2,RequestVote)");
        assertTrue(syncAt >= 0 && syncAt < firstSend, h.ops.toString());
        assertEquals(List.of("setTermAndVote(1,1)", "sync", "send(2,RequestVote)", "send(3,RequestVote)"), h.ops);
    }

    @Test
    void withTheAckBeforeSyncBugTheHarnessSeesTheViolation() {
        RaftConfig bad = RaftConfig.of(1, List.of(1, 2, 3)).withBug(Bug.ACK_BEFORE_SYNC);
        NodeHarness h = new NodeHarness(bad);
        h.fireNextTimer();
        assertFalse(h.unsyncedSends.isEmpty(), "the recording transport must notice a send before sync");
    }

    @Test
    void aRestartKeepsTermVoteAndLogButResetsEverythingElse() {
        InMemoryRaftStorage disk = new InMemoryRaftStorage(4, 2, List.of(NodeHarness.cmd(1, "a"), NodeHarness.cmd(4, "b")));
        NodeHarness h = NodeHarness.cluster(1, 3, disk);
        assertEquals(4, h.node.currentTerm());
        assertEquals(2, h.node.votedFor());
        assertEquals(2, h.node.lastLogIndex());
        assertEquals(4, h.node.lastLogTerm());
        h.fireNextTimer();                                   // candidate in term 5
        assertEquals(Role.CANDIDATE, h.node.role());

        h.restart();
        assertEquals(Role.FOLLOWER, h.node.role());
        assertEquals(5, h.node.currentTerm());               // the term bump was synced before anything was sent
        assertEquals(1, h.node.votedFor());
        assertEquals(2, h.node.lastLogIndex());
        assertEquals(0, h.node.commitIndex());
        assertEquals(0, h.node.lastApplied());
        assertEquals(-1, h.node.leaderId());
        assertTrue(h.node.nextTimerMs() > h.now);            // a fresh election deadline
    }

    @Test
    void roleChangesAreReportedOnlyWhenTheyHappen() {
        NodeHarness h = NodeHarness.cluster(1, 3);
        h.fireNextTimer();
        assertEquals(List.of("CANDIDATE@1/leader=-1"), h.roleChanges);
        h.fireNextTimer();                                   // another attempt: still a candidate, now term 2
        assertEquals(List.of("CANDIDATE@1/leader=-1"), h.roleChanges);
    }

    /** A listener that ignores everything, for tests that only look at timers. */
    private static final class NodeHarnessListener implements RaftListener {
        public void onRoleChange(int nodeId, Role role, long term, int leaderId) { }
        public void onCommitted(int nodeId, long index, LogEntry entry) { }
    }
}
