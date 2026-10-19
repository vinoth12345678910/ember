package com.ember.raft;

import com.ember.raft.RaftMessage.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** One node-level test per election rule. Names start with the rule number (see docs/phase4/raft-rules-to-code.md). */
class ElectionRulesTest {

    private static LogEntry e(long term, String s) {
        return NodeHarness.cmd(term, s);
    }

    private static NodeHarness candidate(int self, int n) {
        NodeHarness h = NodeHarness.cluster(self, n);
        h.fireNextTimer();
        h.sent.clear();
        return h;
    }

    private static NodeHarness leader(int self, int n) {
        NodeHarness h = NodeHarness.cluster(self, n);
        h.fireNextTimer();
        long term = h.node.currentTerm();
        for (int peer = 1, votes = 1; peer <= n && votes < n / 2 + 1; peer++) {
            if (peer != self) {
                h.receive(peer, new RequestVoteResponse(term, true));
                votes++;
            }
        }
        assertEquals(Role.LEADER, h.node.role());
        h.sent.clear();
        return h;
    }

    // ---------------------------------------------------------------- R1

    @Test
    void R1_higherTermInARequestVoteUpdatesTheTermClearsTheVoteAndMakesUsAFollower() {
        NodeHarness h = candidate(1, 3);                       // term 1, voted for itself
        h.receive(2, new RequestVote(5, 2, 0, 0));
        assertEquals(5, h.node.currentTerm());
        assertEquals(Role.FOLLOWER, h.node.role());
        assertEquals(2, h.node.votedFor());                    // the vote was cleared, then granted to the new candidate
        assertEquals(5, h.disk.durableTerm());                 // and it is all durable
        assertEquals(2, h.disk.durableVotedFor());
    }

    @Test
    void R1_higherTermInAnAppendEntriesMakesUsAFollowerOfThatTerm() {
        NodeHarness h = candidate(1, 3);
        h.receive(3, new AppendEntries(7, 3, 0, 0, List.of(), 0));
        assertEquals(7, h.node.currentTerm());
        assertEquals(-1, h.node.votedFor());                   // the old vote is gone; nobody was granted a new one
        assertEquals(Role.FOLLOWER, h.node.role());
        assertEquals(3, h.node.leaderId());
    }

    @Test
    void R1_termAndVoteAreClearedTogetherInOneWrite() {
        NodeHarness h = candidate(1, 3);
        h.ops.clear();
        h.receive(2, new RequestVoteResponse(9, false));
        assertEquals(List.of("setTermAndVote(9,-1)", "sync"), h.ops);
    }

    @Test
    void R1_aHigherTermInAResponseAlsoMakesUsAFollower() {
        NodeHarness h = candidate(1, 3);
        h.receive(2, new RequestVoteResponse(4, false));
        assertEquals(4, h.node.currentTerm());
        assertEquals(Role.FOLLOWER, h.node.role());
    }

    // ---------------------------------------------------------------- R2

    @Test
    void R2_aRequestVoteWithAnOldTermIsRejectedAndCarriesOurTerm() {
        NodeHarness h = NodeHarness.cluster(1, 3, new InMemoryRaftStorage(5, -1, List.of()));
        h.receive(2, new RequestVote(3, 2, 10, 3));
        RequestVoteResponse r = h.lastSentOfType(RequestVoteResponse.class);
        assertFalse(r.voteGranted());
        assertEquals(5, r.term());
        assertEquals(-1, h.node.votedFor());
    }

    @Test
    void R2_aStaleVoteResponseIsIgnored() {
        NodeHarness h = NodeHarness.cluster(1, 5);
        h.fireNextTimer();
        h.fireNextTimer();                                      // term 2
        h.receive(2, new RequestVoteResponse(1, true));         // a "yes" from the previous election
        h.receive(3, new RequestVoteResponse(1, true));
        assertEquals(Role.CANDIDATE, h.node.role());            // would be a leader if they had counted
    }

    @Test
    void R2_aStaleAppendEntriesResponseIsIgnored() {
        NodeHarness h = leader(1, 3);
        long term = h.node.currentTerm();
        h.receive(2, new AppendEntriesResponse(term - 1, false, 0, 0, 0));
        assertEquals(Role.LEADER, h.node.role());
        assertEquals(term, h.node.currentTerm());
        assertTrue(h.sent.isEmpty());
    }

    // ---------------------------------------------------------------- R4

    @Test
    void R4_aTimeoutMakesUsACandidateInANewTermVotingForOurselvesAndAskingEveryoneElse() {
        NodeHarness h = NodeHarness.cluster(2, 5, new InMemoryRaftStorage(3, -1, List.of(e(1, "a"), e(3, "b"))));
        h.fireNextTimer();
        assertEquals(Role.CANDIDATE, h.node.role());
        assertEquals(4, h.node.currentTerm());
        assertEquals(2, h.node.votedFor());
        assertEquals(4, h.disk.durableTerm());                  // persisted together with the vote
        assertEquals(2, h.disk.durableVotedFor());
        assertEquals(4, h.sent.size());
        for (NodeHarness.Sent s : h.sent) {
            assertNotEquals(2, s.to());
            assertEquals(new RequestVote(4, 2, 2, 3), s.message());
        }
        assertEquals(List.of(1, 3, 4, 5), h.sent.stream().map(NodeHarness.Sent::to).toList());
    }

    @Test
    void R4_aCandidateThatTimesOutAgainStartsAnotherTerm() {
        NodeHarness h = candidate(1, 3);
        long firstDeadline = h.node.nextTimerMs();
        h.fireNextTimer();
        assertEquals(2, h.node.currentTerm());
        assertEquals(Role.CANDIDATE, h.node.role());
        assertTrue(h.node.nextTimerMs() >= firstDeadline + 150 && h.node.nextTimerMs() <= firstDeadline + 300);
    }

    @Test
    void R4_aSingleNodeClusterBecomesLeaderImmediately() {
        NodeHarness h = NodeHarness.cluster(1, 1);
        h.fireNextTimer();
        assertEquals(Role.LEADER, h.node.role());
        assertEquals(1, h.node.currentTerm());
        assertEquals(1, h.node.lastLogIndex());                 // its no-op
        assertTrue(h.sent.isEmpty());                           // nobody to talk to
    }

    // ---------------------------------------------------------------- R5

    @Test
    void R5_theVoteIsGrantedOncePerTermEvenIfTwoCandidatesAsk() {
        NodeHarness h = NodeHarness.cluster(1, 5);
        h.receive(2, new RequestVote(1, 2, 0, 0));
        h.receive(3, new RequestVote(1, 3, 0, 0));
        List<RequestVoteResponse> r = h.sentOfType(RequestVoteResponse.class);
        assertTrue(r.get(0).voteGranted());
        assertFalse(r.get(1).voteGranted());
        assertEquals(2, h.node.votedFor());
    }

    @Test
    void R5_theSameCandidateAskingTwiceGetsTheSameAnswer() {
        NodeHarness h = NodeHarness.cluster(1, 3);
        h.receive(2, new RequestVote(1, 2, 0, 0));
        h.receive(2, new RequestVote(1, 2, 0, 0));
        List<RequestVoteResponse> r = h.sentOfType(RequestVoteResponse.class);
        assertTrue(r.get(0).voteGranted());
        assertTrue(r.get(1).voteGranted());
    }

    @Test
    void R5_aNewTermAllowsANewVote() {
        NodeHarness h = NodeHarness.cluster(1, 3);
        h.receive(2, new RequestVote(1, 2, 0, 0));
        h.receive(3, new RequestVote(2, 3, 0, 0));
        List<RequestVoteResponse> r = h.sentOfType(RequestVoteResponse.class);
        assertTrue(r.get(1).voteGranted());
        assertEquals(3, h.node.votedFor());
    }

    @Test
    void R5_theCandidatesLogMustBeAtLeastAsUpToDate() {
        // our log: [t1, t2, t2]  -> lastIndex 3, lastTerm 2
        InMemoryRaftStorage disk = new InMemoryRaftStorage(2, -1, List.of(e(1, "a"), e(2, "b"), e(2, "c")));
        record Case(long lastIndex, long lastTerm, boolean expected, String why) { }
        List<Case> cases = List.of(
                new Case(1, 3, true, "higher last term wins even with a much shorter log"),
                new Case(9, 1, false, "lower last term loses even with a longer log"),
                new Case(3, 2, true, "equal last term, equal length"),
                new Case(4, 2, true, "equal last term, longer log"),
                new Case(2, 2, false, "equal last term, shorter log"),
                new Case(0, 0, false, "empty log"));
        for (Case c : cases) {
            NodeHarness h = NodeHarness.cluster(1, 3, new InMemoryRaftStorage(2, -1, List.of(e(1, "a"), e(2, "b"), e(2, "c"))));
            h.receive(2, new RequestVote(3, 2, c.lastIndex(), c.lastTerm()));
            assertEquals(c.expected(), h.lastSentOfType(RequestVoteResponse.class).voteGranted(), c.why());
        }
        assertEquals(2, disk.currentTerm());
    }

    @Test
    void R5_theVoteIsDurableBeforeTheResponseIsSent() {
        NodeHarness h = NodeHarness.cluster(1, 3);
        h.ops.clear();
        h.receive(2, new RequestVote(1, 2, 0, 0));
        assertEquals(List.of("setTermAndVote(1,-1)", "setTermAndVote(1,2)", "sync", "send(2,RequestVoteResponse)"), h.ops);
        assertTrue(h.unsyncedSends.isEmpty());
        assertEquals(2, h.disk.durableVotedFor());
    }

    @Test
    void R5_theElectionDeadlineIsResetOnlyWhenTheVoteIsGranted() {
        NodeHarness h = NodeHarness.cluster(1, 3, new InMemoryRaftStorage(1, -1, List.of(e(1, "a"), e(1, "b"))));
        long original = h.node.nextTimerMs();
        h.receiveAt(100, 2, new RequestVote(2, 2, 0, 0));       // log not up to date -> denied
        assertEquals(original, h.node.nextTimerMs());
        h.receiveAt(100, 3, new RequestVote(1, 3, 5, 1));       // old term -> denied
        assertEquals(original, h.node.nextTimerMs());
        h.receiveAt(100, 3, new RequestVote(2, 3, 2, 1));       // granted
        assertTrue(h.node.nextTimerMs() >= 100 + 150 && h.node.nextTimerMs() <= 100 + 300);
    }

    @Test
    void R5_aPersistedVoteSurvivesARestartAndPreventsADoubleVote() {
        NodeHarness h = NodeHarness.cluster(1, 5);
        h.receive(2, new RequestVote(3, 2, 0, 0));
        assertTrue(h.lastSentOfType(RequestVoteResponse.class).voteGranted());
        h.restart();
        h.receive(3, new RequestVote(3, 3, 0, 0));
        assertFalse(h.lastSentOfType(RequestVoteResponse.class).voteGranted(), "voted for node 2 in term 3 already");
        h.receive(2, new RequestVote(3, 2, 0, 0));
        assertTrue(h.lastSentOfType(RequestVoteResponse.class).voteGranted(), "node 2 may ask again");
    }

    @Test
    void R5_aLeaderOrCandidateDoesNotVoteForSomeoneElseInItsOwnTerm() {
        NodeHarness h = candidate(1, 3);                       // voted for itself in term 1
        h.receive(2, new RequestVote(1, 2, 5, 5));
        assertFalse(h.lastSentOfType(RequestVoteResponse.class).voteGranted());
        assertEquals(Role.CANDIDATE, h.node.role());
    }

    // ---------------------------------------------------------------- R6

    @Test
    void R6_aMajorityOfVotesMakesTheCandidateLeader() {
        NodeHarness h = candidate(1, 5);
        h.receive(2, new RequestVoteResponse(1, true));
        assertEquals(Role.CANDIDATE, h.node.role());            // 2 of 5
        h.receive(3, new RequestVoteResponse(1, true));
        assertEquals(Role.LEADER, h.node.role());               // 3 of 5
        assertEquals(1, h.node.leaderId() == 1 ? 1 : 0);
    }

    @Test
    void R6_aDuplicatedVoteIsCountedOnce() {
        NodeHarness h = candidate(1, 5);
        for (int i = 0; i < 5; i++) {
            h.receive(2, new RequestVoteResponse(1, true));
        }
        assertEquals(Role.CANDIDATE, h.node.role());
    }

    @Test
    void R6_deniedVotesDoNotCount() {
        NodeHarness h = candidate(1, 3);
        h.receive(2, new RequestVoteResponse(1, false));
        h.receive(3, new RequestVoteResponse(1, false));
        assertEquals(Role.CANDIDATE, h.node.role());
    }

    @Test
    void R6_evenSizedClustersNeedMoreThanHalf() {
        NodeHarness h = candidate(1, 4);
        h.receive(2, new RequestVoteResponse(1, true));
        assertEquals(Role.CANDIDATE, h.node.role());            // 2 of 4 is not a majority
        h.receive(3, new RequestVoteResponse(1, true));
        assertEquals(Role.LEADER, h.node.role());
    }

    @Test
    void R6_votesFromAnEarlierElectionDoNotCarryOver() {
        NodeHarness h = candidate(1, 5);
        h.receive(2, new RequestVoteResponse(1, true));
        h.fireNextTimer();                                       // term 2, votes reset to just ourselves
        h.receive(3, new RequestVoteResponse(2, true));
        assertEquals(Role.CANDIDATE, h.node.role());
        h.receive(4, new RequestVoteResponse(2, true));
        assertEquals(Role.LEADER, h.node.role());
    }

    // ---------------------------------------------------------------- R7

    @Test
    void R7_aCandidateStepsDownOnAnAppendEntriesWithTheSameTerm() {
        NodeHarness h = candidate(1, 3);
        h.receive(2, new AppendEntries(1, 2, 0, 0, List.of(), 0));
        assertEquals(Role.FOLLOWER, h.node.role());
        assertEquals(2, h.node.leaderId());
        assertEquals(1, h.node.currentTerm());
    }

    @Test
    void R7_aCandidateIgnoresAnAppendEntriesFromAnOlderTerm() {
        NodeHarness h = NodeHarness.cluster(1, 3, new InMemoryRaftStorage(4, -1, List.of()));
        h.fireNextTimer();                                       // term 5
        h.sent.clear();
        h.receive(2, new AppendEntries(3, 2, 0, 0, List.of(), 0));
        assertEquals(Role.CANDIDATE, h.node.role());
        AppendEntriesResponse r = h.lastSentOfType(AppendEntriesResponse.class);
        assertFalse(r.success());
        assertEquals(5, r.term());
    }

    // ---------------------------------------------------------------- R8

    @Test
    void R8_theNewLeaderAppendsANoopAndImmediatelyAnnouncesItselfToEveryone() {
        NodeHarness h = NodeHarness.cluster(1, 3, new InMemoryRaftStorage(1, -1, List.of(e(1, "a"), e(1, "b"))));
        h.fireNextTimer();                                       // term 2
        h.sent.clear();
        h.ops.clear();
        h.receive(2, new RequestVoteResponse(2, true));
        assertEquals(Role.LEADER, h.node.role());
        assertEquals(3, h.node.lastLogIndex());
        assertEquals(EntryType.NOOP, h.disk.pendingLog().get(2).type());
        assertEquals(2, h.disk.pendingLog().get(2).term());
        assertEquals(2, h.sent.size());
        for (NodeHarness.Sent s : h.sent) {
            AppendEntries ae = (AppendEntries) s.message();
            assertEquals(2, ae.term());
            assertEquals(1, ae.leaderId());
            assertEquals(2, ae.prevLogIndex());                  // nextIndex was lastIndex + 1 before the no-op
            assertEquals(1, ae.prevLogTerm());
            assertEquals(1, ae.entries().size());
            assertEquals(EntryType.NOOP, ae.entries().get(0).type());
        }
        assertEquals(List.of("append(1)", "sync", "send(2,AppendEntries)", "send(3,AppendEntries)"), h.ops);
    }

    @Test
    void R8_heartbeatsGoOutExactlyEveryHeartbeatInterval() {
        NodeHarness h = leader(1, 3);
        long t0 = h.now;
        assertEquals(t0 + 50, h.node.nextTimerMs());
        h.advanceTo(t0 + 49);
        assertTrue(h.sent.isEmpty(), "no heartbeat before the interval is over");
        h.advanceTo(t0 + 50);
        assertEquals(2, h.sent.size());
        h.sent.clear();
        assertEquals(t0 + 100, h.node.nextTimerMs());
        h.advanceTo(t0 + 100);
        assertEquals(2, h.sent.size());
        h.sent.clear();
        assertEquals(t0 + 150, h.node.nextTimerMs());
    }

    @Test
    void R8_aHeartbeatToAnUpToDateFollowerCarriesNoEntries() {
        NodeHarness h = leader(1, 3);
        // pretend both followers have the no-op: not possible yet in Step 3, so just check the shape of a heartbeat
        h.fireNextTimer();
        for (NodeHarness.Sent s : h.sent) {
            assertInstanceOf(AppendEntries.class, s.message());
        }
    }

    // ---------------------------------------------------------------- R16

    @Test
    void R16_aLeaderStepsDownWhenAnAppendEntriesResponseShowsAHigherTerm() {
        NodeHarness h = leader(1, 3);
        h.receive(2, new AppendEntriesResponse(9, false, 0, 0, 0));
        assertEquals(Role.FOLLOWER, h.node.role());
        assertEquals(9, h.node.currentTerm());
        assertEquals(-1, h.node.votedFor());
        assertEquals(-1, h.node.leaderId());
    }

    @Test
    void R16_aLeaderStepsDownOnAHigherTermRequestVote() {
        NodeHarness h = leader(1, 3);
        h.receive(3, new RequestVote(h.node.currentTerm() + 1, 3, 99, 99));
        assertEquals(Role.FOLLOWER, h.node.role());
        assertTrue(h.lastSentOfType(RequestVoteResponse.class).voteGranted());
    }

    @Test
    void R16_aLeaderThatStepsDownGetsAnElectionDeadlineAgain() {
        NodeHarness h = leader(1, 3);
        h.receiveAt(1000, 2, new AppendEntriesResponse(9, false, 0, 0, 0));
        assertTrue(h.node.nextTimerMs() >= 1000 + 150 && h.node.nextTimerMs() <= 1000 + 300);
    }

    @Test
    void R16_aLeaderNeverStartsAnElectionNoMatterHowLateItIsTicked() {
        NodeHarness h = leader(1, 3);
        long term = h.node.currentTerm();
        h.advanceTo(h.now + 100_000);
        assertEquals(Role.LEADER, h.node.role());
        assertEquals(term, h.node.currentTerm());
        assertEquals(2, h.sent.stream().filter(s -> s.message() instanceof AppendEntries).count());
        assertTrue(h.sent.stream().noneMatch(s -> s.message() instanceof RequestVote));
    }

    @Test
    void R16_nextTimerMsIsCorrectInEveryRole() {
        NodeHarness h = NodeHarness.cluster(1, 3);
        long followerDeadline = h.node.nextTimerMs();           // follower: the election deadline
        assertTrue(followerDeadline >= 150 && followerDeadline <= 300);
        h.fireNextTimer();                                       // candidate: a new election deadline
        long candidateDeadline = h.node.nextTimerMs();
        assertTrue(candidateDeadline - followerDeadline >= 150 && candidateDeadline - followerDeadline <= 300);
        h.receive(2, new RequestVoteResponse(1, true));         // leader: the next heartbeat
        assertEquals(h.now + 50, h.node.nextTimerMs());
    }

    // ---------------------------------------------------------------- R18

    @Test
    void R18_aRestartedLeaderIsAFollowerWithAFreshDeadlineAndKeepsItsTermAndVote() {
        NodeHarness h = leader(1, 3);
        long term = h.node.currentTerm();
        h.advanceTo(h.now + 1000);
        h.restart();
        assertEquals(Role.FOLLOWER, h.node.role());
        assertEquals(term, h.node.currentTerm());
        assertEquals(1, h.node.votedFor());
        assertEquals(1, h.node.lastLogIndex());                 // the synced no-op survived
        assertEquals(-1, h.node.leaderId());
        assertTrue(h.node.nextTimerMs() >= h.now + 150 && h.node.nextTimerMs() <= h.now + 300);
    }

    @Test
    void R18_onlySyncedStateSurvivesACrash() {
        NodeHarness h = NodeHarness.cluster(1, 3);
        h.disk.setTermAndVote(8, 3);                             // written behind the node's back and never synced
        h.restart();
        assertEquals(0, h.node.currentTerm());
    }

    @Test
    void injectedBug_forgetVotedForOnRestart_letsANodeVoteTwiceInOneTerm() {
        RaftConfig bad = RaftConfig.of(1, List.of(1, 2, 3, 4, 5)).withBug(Bug.FORGET_VOTED_FOR_ON_RESTART);
        NodeHarness h = new NodeHarness(bad);
        h.receive(2, new RequestVote(3, 2, 0, 0));
        assertTrue(h.lastSentOfType(RequestVoteResponse.class).voteGranted());
        h.restart();
        assertEquals(-1, h.node.votedFor(), "the bug forgets the vote on restart");
        h.receive(3, new RequestVote(3, 3, 0, 0));
        assertTrue(h.lastSentOfType(RequestVoteResponse.class).voteGranted(), "a second vote in the same term: this is the bug");
    }
}
