package com.ember.raft;

import com.ember.raft.RaftMessage.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Phase 5 core extensions: maxAppendBytes and CheckQuorum, at node level. */
class CheckQuorumAndByteCapTest {

    private static LogEntry big(long term, int bytes) {
        return LogEntry.command(term, new byte[bytes]);
    }

    private static NodeHarness leader(RaftConfig cfg, InMemoryRaftStorage disk) {
        NodeHarness h = new NodeHarness(cfg, disk, 1);
        h.fireNextTimer();
        long term = h.node.currentTerm();
        for (int peer = 2, votes = 1; votes < cfg.majority(); peer++, votes++) {
            h.receive(peer, new RequestVoteResponse(term, true));
        }
        assertEquals(Role.LEADER, h.node.role());
        h.sent.clear();
        return h;
    }

    // ---------------------------------------------------------------- maxAppendBytes

    @Test
    void anAppendEntriesStopsBeforeTheByteCapIsExceeded() {
        List<LogEntry> log = new ArrayList<>();
        for (int i = 0; i < 10; i++) log.add(big(1, 1000));             // 10 entries of ~1 KB
        RaftConfig cfg = RaftConfig.of(1, List.of(1, 2, 3)).withMaxAppendBytes(3_500);
        NodeHarness h = new NodeHarness(cfg, new InMemoryRaftStorage(1, -1, log), 1);
        h.fireNextTimer();
        h.receive(2, new RequestVoteResponse(2, true));
        h.sent.clear();
        h.receive(2, new AppendEntriesResponse(2, false, 0, 1, 0));    // follower has nothing: resume at index 1
        AppendEntries ae = (AppendEntries) h.sentTo(2).get(0).message();
        assertEquals(3, ae.entries().size(), "3 x (1000+16) = 3048 fits, a 4th (4064) would not");
    }

    @Test
    void anEntryBiggerThanTheCapIsStillSentAlone() {
        List<LogEntry> log = List.of(big(1, 10_000), big(1, 10), big(1, 10));
        RaftConfig cfg = RaftConfig.of(1, List.of(1, 2, 3)).withMaxAppendBytes(100);
        NodeHarness h = new NodeHarness(cfg, new InMemoryRaftStorage(1, -1, log), 1);
        h.fireNextTimer();
        h.receive(2, new RequestVoteResponse(2, true));
        h.receive(2, new AppendEntriesResponse(2, false, 0, 1, 0));    // follower has nothing: resume at index 1
        AppendEntries ae = (AppendEntries) h.sentTo(2).get(h.sentTo(2).size() - 1).message();
        assertEquals(1, ae.entries().size());
        assertEquals(10_000, ae.entries().get(0).command().length);
    }

    @Test
    void theEntryCountCapStillAppliesWhenBytesAreSmall() {
        List<LogEntry> log = new ArrayList<>();
        for (int i = 0; i < 100; i++) log.add(big(1, 1));
        RaftConfig cfg = RaftConfig.of(1, List.of(1, 2, 3)).withMaxEntriesPerAppend(5);
        NodeHarness h = new NodeHarness(cfg, new InMemoryRaftStorage(1, -1, log), 1);
        h.fireNextTimer();
        h.receive(2, new RequestVoteResponse(2, true));
        for (AppendEntries ae : h.sentOfType(AppendEntries.class)) {
            assertTrue(ae.entries().size() <= 5);
        }
    }

    @Test
    void aFollowerFarBehindIsCaughtUpInCappedMessages() {
        List<LogEntry> log = new ArrayList<>();
        for (int i = 0; i < 40; i++) log.add(big(1, 1000));
        RaftConfig cfg = RaftConfig.of(1, List.of(1, 2, 3)).withMaxAppendBytes(5_000);
        NodeHarness leader = new NodeHarness(cfg, new InMemoryRaftStorage(1, -1, log), 1);
        NodeHarness follower = new NodeHarness(RaftConfig.of(2, List.of(1, 2, 3)), new InMemoryRaftStorage(1, -1, List.of()), 2);
        leader.fireNextTimer();
        leader.receive(3, new RequestVoteResponse(leader.node.currentTerm(), true));
        leader.sent.clear();
        leader.receive(2, new AppendEntriesResponse(leader.node.currentTerm(), false, 0, 1, 0));   // start from index 1
        int messages = 0;
        for (int guard = 0; guard < 200; guard++) {
            List<NodeHarness.Sent> toFollower = new ArrayList<>();
            for (NodeHarness.Sent s : leader.sentTo(2)) {
                if (s.message() instanceof AppendEntries) toFollower.add(s);
            }
            leader.sent.clear();
            if (toFollower.isEmpty()) break;
            for (NodeHarness.Sent s : toFollower) {
                assertTrue(((AppendEntries) s.message()).entries().size() <= 5, "4 big entries plus the tiny no-op in the last message");
                messages++;
                follower.receive(1, s.message());
            }
            for (NodeHarness.Sent s : new ArrayList<>(follower.sent)) leader.receive(2, s.message());
            follower.sent.clear();
        }
        assertEquals(41, follower.node.lastLogIndex());
        assertTrue(messages >= 10, "40 entries in messages of at most 4 need at least 10 messages, got " + messages);
    }

    // ---------------------------------------------------------------- CheckQuorum

    private static RaftConfig quorumConfig(int n, boolean on) {
        List<Integer> ids = new ArrayList<>();
        for (int i = 1; i <= n; i++) ids.add(i);
        return RaftConfig.of(1, ids).withCheckQuorum(on);
    }

    @Test
    void aLeaderThatKeepsHearingFromAMajorityKeepsLeading() {
        NodeHarness h = leader(quorumConfig(3, true), new InMemoryRaftStorage());
        for (int i = 0; i < 20; i++) {
            long t = h.node.nextTimerMs();
            h.receiveAt(t - 1, 2, new AppendEntriesResponse(h.node.currentTerm(), true, 1, 0, 0));   // node 2 answers every round
            h.advanceTo(t);
            assertEquals(Role.LEADER, h.node.role());
        }
    }

    @Test
    void aLeaderThatHearsNobodyStepsDownWithoutChangingItsTerm() {
        NodeHarness h = leader(quorumConfig(3, true), new InMemoryRaftStorage());
        long term = h.node.currentTerm();
        long start = h.now;
        while (h.node.role() == Role.LEADER && h.now < start + 5_000) {
            h.fireNextTimer();
        }
        assertEquals(Role.FOLLOWER, h.node.role());
        assertEquals(term, h.node.currentTerm(), "stepping down is a local observation, not a new election");
        assertEquals(1, h.node.votedFor(), "the vote record stays");
        assertEquals(-1, h.node.leaderId());
        assertTrue(h.now - start > 300, "it must wait for electionTimeoutMax (300 ms) before resigning");
        assertTrue(h.now - start <= 300 + 50, "and resign at the first heartbeat after that: " + (h.now - start));
        assertTrue(h.node.nextTimerMs() > h.now, "it has an election deadline again");
    }

    @Test
    void oneAnswerFromAFiveNodeClusterIsNotAMajority() {
        NodeHarness h = leader(quorumConfig(5, true), new InMemoryRaftStorage());
        long start = h.now;
        while (h.node.role() == Role.LEADER && h.now < start + 5_000) {
            h.receiveAt(h.now, 2, new AppendEntriesResponse(h.node.currentTerm(), true, 1, 0, 0));   // only node 2 answers: 2 of 5
            h.fireNextTimer();
        }
        assertEquals(Role.FOLLOWER, h.node.role());
    }

    @Test
    void twoAnswersFromAFiveNodeClusterAreAMajorityWithTheLeaderItself() {
        NodeHarness h = leader(quorumConfig(5, true), new InMemoryRaftStorage());
        for (int i = 0; i < 20; i++) {
            h.receiveAt(h.now, 2, new AppendEntriesResponse(h.node.currentTerm(), true, 1, 0, 0));
            h.receiveAt(h.now, 3, new AppendEntriesResponse(h.node.currentTerm(), true, 1, 0, 0));
            h.fireNextTimer();
            assertEquals(Role.LEADER, h.node.role());
        }
    }

    @Test
    void aSingleNodeLeaderNeverStepsDown() {
        NodeHarness h = leader(quorumConfig(1, true), new InMemoryRaftStorage());
        h.advanceTo(h.now + 100_000);
        assertEquals(Role.LEADER, h.node.role());
    }

    @Test
    void aFreshLeaderGetsAGracePeriod() {
        NodeHarness h = leader(quorumConfig(3, true), new InMemoryRaftStorage());
        h.fireNextTimer();                                           // the very first heartbeat after the election
        assertEquals(Role.LEADER, h.node.role());
    }

    @Test
    void withTheOptionOffAnIsolatedLeaderLeadsForever() {
        NodeHarness h = leader(quorumConfig(3, false), new InMemoryRaftStorage());
        h.advanceTo(h.now + 100_000);
        assertEquals(Role.LEADER, h.node.role());
    }

    @Test
    void aStaleLeaderMessageCountsForNothingAfterSteppingDown() {
        NodeHarness h = leader(quorumConfig(3, true), new InMemoryRaftStorage());
        long start = h.now;
        while (h.node.role() == Role.LEADER && h.now < start + 5_000) h.fireNextTimer();
        long term = h.node.currentTerm();
        h.receive(2, new AppendEntriesResponse(term, true, 1, 0, 0));    // a late answer
        assertEquals(Role.FOLLOWER, h.node.role());
        assertEquals(term, h.node.currentTerm());
    }
}
