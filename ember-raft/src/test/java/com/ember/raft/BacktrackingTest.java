package com.ember.raft;

import com.ember.raft.RaftMessage.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** The accelerated-backtracking hint needs far fewer round trips than decrement-by-one on a long divergent log. */
class BacktrackingTest {

    private static List<LogEntry> entries(long term, int count, int firstId) {
        List<LogEntry> out = new ArrayList<>();
        for (int i = 0; i < count; i++) out.add(NodeHarness.cmd(term, "t" + term + "-" + (firstId + i)));
        return out;
    }

    /** Runs a leader and a follower against each other; returns how many AppendEntries the follower received. */
    private static int roundTrips(boolean hint) {
        // common prefix: 50 entries of term 1. The follower then has a stale tail of 150 entries of term 2
        // (written by a deposed leader). The new leader (term 4) has 250 entries of term 3 after the prefix.
        List<LogEntry> prefix = entries(1, 50, 1);
        List<LogEntry> leaderLog = new ArrayList<>(prefix);
        leaderLog.addAll(entries(3, 250, 1000));
        List<LogEntry> followerLog = new ArrayList<>(prefix);
        followerLog.addAll(entries(2, 150, 2000));

        RaftConfig leaderConfig = RaftConfig.of(1, List.of(1, 2, 3)).withMaxEntriesPerAppend(500);
        NodeHarness leader = new NodeHarness(leaderConfig, new InMemoryRaftStorage(3, -1, leaderLog), 1);
        NodeHarness follower = new NodeHarness(RaftConfig.of(2, List.of(1, 2, 3)).withMaxEntriesPerAppend(500),
                new InMemoryRaftStorage(2, -1, followerLog), 2);
        leader.node.setConflictHintEnabledForTesting(hint);

        leader.fireNextTimer();                                              // term 4
        leader.receive(3, new RequestVoteResponse(leader.node.currentTerm(), true));
        assertEquals(Role.LEADER, leader.node.role());

        int appendEntriesSeen = 0;
        for (int guard = 0; guard < 2000; guard++) {
            List<NodeHarness.Sent> toFollower = new ArrayList<>(leader.sentTo(2));
            leader.sent.clear();
            if (toFollower.isEmpty()) {
                break;
            }
            for (NodeHarness.Sent s : toFollower) {
                appendEntriesSeen++;
                follower.receive(1, s.message());
            }
            List<NodeHarness.Sent> back = new ArrayList<>(follower.sent);
            follower.sent.clear();
            for (NodeHarness.Sent s : back) {
                leader.receive(2, s.message());
            }
        }
        assertEquals(leaderLog.size() + 1, follower.node.lastLogIndex(), "the follower ended with the leader's whole log (+ no-op)");
        for (int i = 1; i <= leaderLog.size(); i++) {
            assertEquals(leaderLog.get(i - 1), follower.disk.pendingLog().get(i - 1));
        }
        return appendEntriesSeen;
    }

    @Test
    void theHintRepairsALongDivergentLogInAHandfulOfRoundTrips() {
        int withHint = roundTrips(true);
        assertTrue(withHint <= 4, "with the hint: " + withHint + " round trips");
    }

    @Test
    void decrementByOneNeedsAboutOneRoundTripPerDivergentEntry() {
        int plain = roundTrips(false);
        assertTrue(plain >= 200, "without the hint: " + plain + " round trips");
    }

    @Test
    void theHintIsAtLeastFiftyTimesFasterHere() {
        assertTrue(roundTrips(false) >= 50 * roundTrips(true));
    }

    @Test
    void whenTheLeaderHasTheConflictingTermItResumesRightAfterItsLastEntryOfThatTerm() {
        // leader log terms: 1 1 2 2 2 3 ; follower (term 2 at index 5) says "I have term 2 from index 3"
        List<LogEntry> log = new ArrayList<>();
        for (long t : new long[]{1, 1, 2, 2, 2, 3}) log.add(NodeHarness.cmd(t, "x"));
        NodeHarness h = new NodeHarness(RaftConfig.of(1, List.of(1, 2, 3)), new InMemoryRaftStorage(3, -1, log), 1);
        h.fireNextTimer();
        h.receive(3, new RequestVoteResponse(h.node.currentTerm(), true));
        h.sent.clear();
        h.receive(2, new AppendEntriesResponse(4, false, 0, 3, 2));
        AppendEntries retry = (AppendEntries) h.sentTo(2).get(0).message();
        assertEquals(5, retry.prevLogIndex(), "the leader's last entry of term 2 is index 5, so resume after it");
    }

    @Test
    void whenTheLeaderLacksTheConflictingTermItJumpsToTheFollowersFirstIndexOfThatTerm() {
        List<LogEntry> log = new ArrayList<>();
        for (long t : new long[]{1, 1, 3, 3}) log.add(NodeHarness.cmd(t, "x"));
        NodeHarness h = new NodeHarness(RaftConfig.of(1, List.of(1, 2, 3)), new InMemoryRaftStorage(3, -1, log), 1);
        h.fireNextTimer();
        h.receive(3, new RequestVoteResponse(h.node.currentTerm(), true));
        h.sent.clear();
        h.receive(2, new AppendEntriesResponse(4, false, 0, 2, 2));          // follower's term 2 starts at index 2; we never had term 2
        AppendEntries retry = (AppendEntries) h.sentTo(2).get(0).message();
        assertEquals(1, retry.prevLogIndex());
    }

    @Test
    void aDuplicatedFailureResponseNeverMovesNextIndexBelowWhatIsKnownToMatch() {
        List<LogEntry> log = entries(1, 10, 1);
        NodeHarness h = new NodeHarness(RaftConfig.of(1, List.of(1, 2, 3)), new InMemoryRaftStorage(1, -1, log), 1);
        h.fireNextTimer();
        h.receive(3, new RequestVoteResponse(h.node.currentTerm(), true));
        h.receive(2, new AppendEntriesResponse(h.node.currentTerm(), true, 8, 0, 0));
        h.sent.clear();
        h.receive(2, new AppendEntriesResponse(h.node.currentTerm(), false, 0, 3, 0));   // an old failure shows up late
        assertTrue(h.sentTo(2).isEmpty(), "node 2 is known to match up to 8; the stale failure changes nothing");
    }
}
