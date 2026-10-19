package com.ember.raft;

import com.ember.raft.RaftMessage.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** One node-level test per replication rule (R3, R9-R15, R17). */
class ReplicationRulesTest {

    private static LogEntry e(long term, String s) {
        return NodeHarness.cmd(term, s);
    }

    private static List<LogEntry> log(long... terms) {
        List<LogEntry> out = new ArrayList<>();
        for (int i = 0; i < terms.length; i++) {
            out.add(e(terms[i], "e" + (i + 1)));
        }
        return out;
    }

    /** A follower of node 2 (term 1) holding the given terms as its log. */
    private static NodeHarness follower(long... terms) {
        return NodeHarness.cluster(1, 3, new InMemoryRaftStorage(1, -1, log(terms)));
    }

    private static AppendEntriesResponse reply(NodeHarness h) {
        return h.lastSentOfType(AppendEntriesResponse.class);
    }

    /** Makes node 1 leader of the next term over a pre-populated log. */
    private static NodeHarness leader(int n, InMemoryRaftStorage disk) {
        NodeHarness h = NodeHarness.cluster(1, n, disk);
        h.fireNextTimer();
        long term = h.node.currentTerm();
        for (int peer = 2, votes = 1; votes < n / 2 + 1; peer++, votes++) {
            h.receive(peer, new RequestVoteResponse(term, true));
        }
        assertEquals(Role.LEADER, h.node.role());
        h.sent.clear();
        return h;
    }

    // ---------------------------------------------------------------- R3

    @Test
    void R3_committedEntriesAreAppliedInOrderExactlyOnce() {
        NodeHarness h = follower(1, 1, 1);
        h.receive(2, new AppendEntries(1, 2, 2, 1, List.of(), 2));
        assertEquals(List.of(1L, 2L), h.committedIndexes);
        h.receive(2, new AppendEntries(1, 2, 2, 1, List.of(), 2));          // the same commit index again
        assertEquals(List.of(1L, 2L), h.committedIndexes);
        h.receive(2, new AppendEntries(1, 2, 3, 1, List.of(), 3));
        assertEquals(List.of(1L, 2L, 3L), h.committedIndexes);
        assertEquals(e(1, "e1"), h.committedEntries.get(0));
        assertEquals(3, h.node.lastApplied());
        assertEquals(h.node.commitIndex(), h.node.lastApplied());
    }

    @Test
    void R3_callbacksOnlyHappenAfterTheStateIsSynced() {
        NodeHarness h = follower();
        h.receive(2, new AppendEntries(1, 2, 0, 0, List.of(e(1, "a")), 1));
        assertEquals(List.of(1L), h.committedIndexes);
        assertTrue(h.unsyncedSends.isEmpty());
        assertFalse(h.disk.hasUnsyncedWrites());
    }

    // ---------------------------------------------------------------- R9, R10

    @Test
    void R9_anAppendEntriesFromAnOldTermIsRejectedAndChangesNothing() {
        NodeHarness h = NodeHarness.cluster(1, 3, new InMemoryRaftStorage(5, -1, log(1, 5)));
        long deadline = h.node.nextTimerMs();
        h.receiveAt(10, 2, new AppendEntries(4, 2, 2, 5, List.of(e(4, "x")), 2));
        AppendEntriesResponse r = reply(h);
        assertFalse(r.success());
        assertEquals(5, r.term());
        assertEquals(2, h.node.lastLogIndex());
        assertEquals(0, h.node.commitIndex());
        assertEquals(deadline, h.node.nextTimerMs(), "a stale leader must not reset the election timer");
        assertEquals(-1, h.node.leaderId());
    }

    @Test
    void R10_aValidAppendEntriesResetsTheElectionDeadlineAndRecordsTheLeader() {
        NodeHarness h = follower();
        long first = h.node.nextTimerMs();
        h.receiveAt(first - 1, 2, new AppendEntries(1, 2, 0, 0, List.of(), 0));
        assertEquals(2, h.node.leaderId());
        assertTrue(h.node.nextTimerMs() >= first - 1 + 150 && h.node.nextTimerMs() <= first - 1 + 300);
        h.advanceTo(first);                                                  // the old deadline passes: no election
        assertEquals(Role.FOLLOWER, h.node.role());
        assertEquals(1, h.node.currentTerm());
    }

    // ---------------------------------------------------------------- R11

    @Test
    void R11_aLogThatIsTooShortRepliesFalseWithAHintPastItsEnd() {
        NodeHarness h = follower(1, 1);
        h.receive(2, new AppendEntries(1, 2, 5, 1, List.of(e(1, "x")), 0));
        AppendEntriesResponse r = reply(h);
        assertFalse(r.success());
        assertEquals(3, r.conflictIndex());
        assertEquals(0, r.conflictTerm());
        assertEquals(2, h.node.lastLogIndex());
    }

    @Test
    void R11_aTermMismatchAtPrevLogIndexRepliesFalseWithTheConflictingTermAndItsFirstIndex() {
        NodeHarness h = follower(1, 2, 2, 2, 3);
        h.receive(2, new AppendEntries(3, 2, 4, 3, List.of(), 0));        // we hold term 2 at index 4
        AppendEntriesResponse r = reply(h);
        assertFalse(r.success());
        assertEquals(2, r.conflictTerm());
        assertEquals(2, r.conflictIndex());                                  // term 2 starts at index 2 in our log
        assertEquals(5, h.node.lastLogIndex(), "nothing is deleted on a failed consistency check");
    }

    @Test
    void R11_prevLogIndexZeroAlwaysMatches() {
        NodeHarness h = follower();
        h.receive(2, new AppendEntries(1, 2, 0, 0, List.of(e(1, "a")), 0));
        assertTrue(reply(h).success());
        assertEquals(1, h.node.lastLogIndex());
    }

    @Test
    void R11_successReportsTheHighestVerifiedIndex() {
        NodeHarness h = follower(1, 1);
        h.receive(2, new AppendEntries(1, 2, 1, 1, List.of(e(1, "e2"), e(1, "x3")), 0));
        AppendEntriesResponse r = reply(h);
        assertTrue(r.success());
        assertEquals(3, r.matchIndex());
    }

    // ---------------------------------------------------------------- R12

    @Test
    void R12_conflictingEntriesAndEverythingAfterThemAreReplaced() {
        NodeHarness h = follower(1, 1, 2, 2, 2);                              // an uncommitted tail from an old leader
        h.receive(2, new AppendEntries(3, 2, 2, 1, List.of(e(3, "n3"), e(3, "n4")), 0));
        assertTrue(reply(h).success());
        assertEquals(4, h.node.lastLogIndex());
        assertEquals(List.of(1L, 1L, 3L, 3L), h.disk.pendingLog().stream().map(LogEntry::term).toList());
        assertTrue(h.ops.contains("truncateFrom(3)"));
    }

    @Test
    void R12_entriesThatAlreadyMatchAreNeverTruncatedByADelayedOrDuplicatedMessage() {
        NodeHarness h = follower(1, 1, 1, 1, 1);                              // we already hold 5 good entries
        h.ops.clear();
        h.receive(2, new AppendEntries(1, 2, 1, 1, List.of(e(1, "e2"), e(1, "e3")), 0));   // an old message arrives late
        assertTrue(reply(h).success());
        assertEquals(5, h.node.lastLogIndex(), "the good entries after index 3 must survive");
        assertTrue(h.ops.stream().noneMatch(o -> o.startsWith("truncateFrom")), h.ops.toString());
        assertTrue(h.ops.stream().noneMatch(o -> o.startsWith("append")), "nothing new to append");
        h.receive(2, new AppendEntries(1, 2, 1, 1, List.of(e(1, "e2"), e(1, "e3")), 0));   // and a duplicate of it
        assertEquals(5, h.node.lastLogIndex());
    }

    @Test
    void R12_onlyTheMissingEntriesAreAppended() {
        NodeHarness h = follower(1, 1, 1);
        h.ops.clear();
        h.receive(2, new AppendEntries(1, 2, 1, 1, List.of(e(1, "e2"), e(1, "e3"), e(1, "n4"), e(1, "n5")), 0));
        assertEquals(5, h.node.lastLogIndex());
        assertTrue(h.ops.contains("append(2)"), h.ops.toString());
    }

    @Test
    void R12_aConflictInTheMiddleOfAMessageTruncatesFromThatEntryOn() {
        NodeHarness h = follower(1, 1, 1, 1);
        h.receive(2, new AppendEntries(2, 2, 1, 1, List.of(e(1, "e2"), e(2, "n3")), 0));
        assertEquals(List.of(1L, 1L, 2L), h.disk.pendingLog().stream().map(LogEntry::term).toList());
    }

    @Test
    void R12_theWritesAreDurableBeforeTheReply() {
        NodeHarness h = follower();
        h.receive(2, new AppendEntries(1, 2, 0, 0, List.of(e(1, "a"), e(1, "b")), 0));
        assertTrue(h.unsyncedSends.isEmpty());
        assertEquals(2, h.disk.durableLog().size());
    }

    // ---------------------------------------------------------------- R13

    @Test
    void R13_leaderCommitIsCappedByTheLastNewEntryOfThisMessage() {
        NodeHarness h = follower(1, 1, 1, 1, 1);                              // a long, possibly stale log
        h.receive(2, new AppendEntries(1, 2, 1, 1, List.of(e(1, "e2"), e(1, "e3")), 10));
        assertEquals(3, h.node.commitIndex(), "min(10, prev 1 + 2 entries) = 3, not 5 and not 10");
        assertEquals(List.of(1L, 2L, 3L), h.committedIndexes);
    }

    @Test
    void R13_aHeartbeatCommitsOnlyUpToPrevLogIndex() {
        NodeHarness h = follower(1, 1, 1, 1);
        h.receive(2, new AppendEntries(1, 2, 2, 1, List.of(), 4));
        assertEquals(2, h.node.commitIndex());
    }

    @Test
    void R13_theCommitIndexNeverDecreases() {
        NodeHarness h = follower(1, 1, 1);
        h.receive(2, new AppendEntries(1, 2, 3, 1, List.of(), 3));
        assertEquals(3, h.node.commitIndex());
        h.receive(2, new AppendEntries(1, 2, 1, 1, List.of(), 1));            // a delayed message with a smaller commit
        assertEquals(3, h.node.commitIndex());
    }

    @Test
    void R13_aFailedConsistencyCheckNeverAdvancesTheCommitIndex() {
        NodeHarness h = follower(1);
        h.receive(2, new AppendEntries(1, 2, 7, 1, List.of(), 7));
        assertFalse(reply(h).success());
        assertEquals(0, h.node.commitIndex());
    }

    // ---------------------------------------------------------------- R14

    @Test
    void R14_aFailureMovesNextIndexBackUsingTheHintAndResendsAtOnce() {
        NodeHarness h = leader(3, new InMemoryRaftStorage(1, -1, log(1, 1, 1, 1, 1)));   // leader of term 2, no-op at index 6
        h.fireNextTimer();                                                   // heartbeat
        AppendEntries hb = (AppendEntries) h.sentTo(2).get(0).message();
        assertEquals(5, hb.prevLogIndex());
        h.sent.clear();
        h.receive(2, new AppendEntriesResponse(2, false, 0, 3, 0));         // "my log ends at 2"
        AppendEntries retry = (AppendEntries) h.sentTo(2).get(0).message();
        assertEquals(2, retry.prevLogIndex());
        assertEquals(4, retry.entries().size());                             // entries 3..6
        assertEquals(1, h.sentTo(2).size());
        assertTrue(h.sentTo(3).isEmpty(), "only the peer that failed is retried");
    }

    @Test
    void R14_withoutTheHintAFailureStepsBackByOne() {
        NodeHarness h = leader(3, new InMemoryRaftStorage(1, -1, log(1, 1, 1, 1, 1)));
        h.node.setConflictHintEnabledForTesting(false);
        h.receive(2, new AppendEntriesResponse(2, false, 0, 1, 0));
        assertEquals(4, ((AppendEntries) h.sentTo(2).get(0).message()).prevLogIndex());
        h.sent.clear();
        h.receive(2, new AppendEntriesResponse(2, false, 0, 1, 0));
        assertEquals(3, ((AppendEntries) h.sentTo(2).get(0).message()).prevLogIndex());
    }

    @Test
    void R14_nextIndexNeverGoesBelowOne() {
        NodeHarness h = leader(3, new InMemoryRaftStorage());
        for (int i = 0; i < 5; i++) {
            h.receive(2, new AppendEntriesResponse(1, false, 0, 1, 0));      // "I have nothing": hint says start at 1
        }
        h.sent.clear();
        h.fireNextTimer();
        assertEquals(0, ((AppendEntries) h.sentTo(2).get(0).message()).prevLogIndex(), "prevLogIndex 0 is the lowest possible");
        h.node.setConflictHintEnabledForTesting(false);
        for (int i = 0; i < 5; i++) {
            h.receive(2, new AppendEntriesResponse(1, false, 0, 0, 0));      // plain decrement must also stop at 1
        }
        h.sent.clear();
        h.fireNextTimer();
        assertEquals(0, ((AppendEntries) h.sentTo(2).get(0).message()).prevLogIndex());
    }

    @Test
    void R14_aSuccessAdvancesMatchIndexAndStaleSuccessesNeverMoveItBack() {
        NodeHarness h = leader(5, new InMemoryRaftStorage(1, -1, log(1, 1)));
        h.receive(2, new AppendEntriesResponse(2, true, 3, 0, 0));
        h.receive(2, new AppendEntriesResponse(2, true, 1, 0, 0));          // a late response
        h.sent.clear();
        h.fireNextTimer();                                                   // heartbeat shows nextIndex of node 2
        AppendEntries hb = (AppendEntries) h.sentTo(2).get(0).message();
        assertEquals(3, hb.prevLogIndex(), "nextIndex stays matchIndex + 1");
        assertTrue(hb.entries().isEmpty());
    }

    @Test
    void R14_aFollowerThatIsStillBehindIsSentTheNextBatchWithoutWaitingForTheHeartbeat() {
        InMemoryRaftStorage disk = new InMemoryRaftStorage(1, -1, log(1, 1, 1, 1, 1, 1));
        NodeHarness h = NodeHarness.cluster(1, 3, disk);
        h.fireNextTimer();
        long term = h.node.currentTerm();
        h.receive(2, new RequestVoteResponse(term, true));
        h.sent.clear();
        h.receive(2, new AppendEntriesResponse(term, true, 2, 0, 0));       // acked up to 2 of 7
        AppendEntries next = (AppendEntries) h.sentTo(2).get(0).message();
        assertEquals(2, next.prevLogIndex());
        assertEquals(5, next.entries().size());
    }

    @Test
    void R14_maxEntriesPerAppendBoundsEveryMessage() {
        List<LogEntry> big = new ArrayList<>();
        for (int i = 0; i < 200; i++) big.add(e(1, "x" + i));
        RaftConfig cfg = RaftConfig.of(1, List.of(1, 2, 3)).withMaxEntriesPerAppend(64);
        NodeHarness h = new NodeHarness(cfg, new InMemoryRaftStorage(1, -1, big), 1);
        h.fireNextTimer();
        h.receive(2, new RequestVoteResponse(h.node.currentTerm(), true));
        h.receive(2, new AppendEntriesResponse(2, true, 0, 0, 0));
        assertFalse(h.sentOfType(AppendEntries.class).isEmpty());
        for (AppendEntries ae : h.sentOfType(AppendEntries.class)) {
            assertTrue(ae.entries().size() <= 64);
        }
    }

    // ---------------------------------------------------------------- R15

    @Test
    void R15_theLeaderCommitsAnEntryOfItsOwnTermOnceAMajorityHasIt() {
        NodeHarness h = leader(3, new InMemoryRaftStorage());                // no-op at index 1, term 1
        assertEquals(0, h.node.commitIndex());
        h.receive(2, new AppendEntriesResponse(1, true, 1, 0, 0));
        assertEquals(1, h.node.commitIndex());
        assertEquals(List.of(1L), h.committedIndexes);
        assertEquals(EntryType.NOOP, h.committedEntries.get(0).type());
    }

    @Test
    void R15_aMajorityIncludesTheLeaderItself() {
        NodeHarness h = leader(5, new InMemoryRaftStorage());
        h.receive(2, new AppendEntriesResponse(1, true, 1, 0, 0));
        assertEquals(0, h.node.commitIndex(), "leader + 1 follower = 2 of 5");
        h.receive(3, new AppendEntriesResponse(1, true, 1, 0, 0));
        assertEquals(1, h.node.commitIndex(), "leader + 2 followers = 3 of 5");
    }

    @Test
    void R15_anOldTermEntryIsNotCommittedByCountingReplicas_butIndirectlyOnceACurrentTermEntrySitsAboveIt() {
        // Figure 8 in miniature: index 1 holds an entry of term 1; we lead term 2 with a no-op at index 2.
        NodeHarness h = leader(3, new InMemoryRaftStorage(1, -1, log(1)));
        assertEquals(2, h.node.currentTerm());
        h.receive(2, new AppendEntriesResponse(2, true, 1, 0, 0));          // node 2 holds the OLD entry: now 2 of 3 have index 1
        assertEquals(0, h.node.commitIndex(), "an entry of term 1 must not be committed by counting replicas");
        assertTrue(h.committedIndexes.isEmpty());
        h.receive(2, new AppendEntriesResponse(2, true, 2, 0, 0));          // the term-2 no-op reaches a majority
        assertEquals(2, h.node.commitIndex(), "now both are committed");
        assertEquals(List.of(1L, 2L), h.committedIndexes);
    }

    @Test
    void R15_withTheInjectedBugTheOldTermEntryIsCommittedByCounting() {
        RaftConfig bad = RaftConfig.of(1, List.of(1, 2, 3)).withBug(Bug.COMMIT_OLD_TERM_BY_COUNTING);
        NodeHarness h = new NodeHarness(bad, new InMemoryRaftStorage(1, -1, log(1)), 1);
        h.fireNextTimer();
        h.receive(2, new RequestVoteResponse(2, true));
        h.receive(2, new AppendEntriesResponse(2, true, 1, 0, 0));
        assertEquals(1, h.node.commitIndex(), "this is the Figure 8 bug: the checkers must be able to catch it");
    }

    @Test
    void R15_theCommitIndexOnlyMovesForward() {
        NodeHarness h = leader(3, new InMemoryRaftStorage());
        h.receive(2, new AppendEntriesResponse(1, true, 1, 0, 0));
        h.receive(3, new AppendEntriesResponse(1, true, 1, 0, 0));
        h.receive(2, new AppendEntriesResponse(1, true, 0, 0, 0));          // reordered, stale
        assertEquals(1, h.node.commitIndex());
    }

    @Test
    void R15_aSingleNodeClusterCommitsByItself() {
        NodeHarness h = NodeHarness.cluster(1, 1);
        h.fireNextTimer();
        assertEquals(1, h.node.commitIndex(), "its own no-op");
        ProposeResult r = h.node.propose("x".getBytes(), h.now);
        assertEquals(2, r.index());
        assertEquals(2, h.node.commitIndex());
        assertEquals(List.of(1L, 2L), h.committedIndexes);
        assertEquals("x", new String(h.committedEntries.get(1).command()));
    }

    // ---------------------------------------------------------------- R17

    @Test
    void R17_aLeaderAppendsPersistsAndReplicatesACommand() {
        NodeHarness h = leader(3, new InMemoryRaftStorage());
        h.ops.clear();
        ProposeResult r = h.node.propose("hello".getBytes(), h.now);
        assertEquals(ProposeResult.Status.ACCEPTED, r.status());
        assertEquals(2, r.index());                                          // after the no-op
        assertEquals(1, r.term());
        assertEquals(1, r.leaderHint());
        assertEquals("hello", new String(h.disk.durableLog().get(1).command()));
        assertEquals(2, h.sentOfType(AppendEntries.class).size());
        for (AppendEntries ae : h.sentOfType(AppendEntries.class)) {
            assertEquals(2, ae.entries().size());                            // no-op not yet acked, so both are sent
            assertEquals("hello", new String(ae.entries().get(1).command()));
        }
        assertEquals("append(1)", h.ops.get(0));
        assertEquals("sync", h.ops.get(1));
        assertTrue(h.unsyncedSends.isEmpty());
    }

    @Test
    void R17_aFollowerAnswersNotLeaderWithItsBestHint() {
        NodeHarness h = follower();
        ProposeResult unknown = h.node.propose("x".getBytes(), 0);
        assertEquals(ProposeResult.Status.NOT_LEADER, unknown.status());
        assertEquals(-1, unknown.leaderHint());
        h.receive(2, new AppendEntries(1, 2, 0, 0, List.of(), 0));
        ProposeResult known = h.node.propose("x".getBytes(), 0);
        assertEquals(ProposeResult.Status.NOT_LEADER, known.status());
        assertEquals(2, known.leaderHint());
        assertEquals(0, h.node.lastLogIndex(), "a follower must not touch its log");
    }

    @Test
    void R17_aCandidateAnswersNotLeader() {
        NodeHarness h = NodeHarness.cluster(1, 3);
        h.fireNextTimer();
        assertEquals(ProposeResult.Status.NOT_LEADER, h.node.propose("x".getBytes(), h.now).status());
        assertEquals(0, h.node.lastLogIndex());
    }

    @Test
    void R17_aDeposedLeaderStopsAcceptingCommands() {
        NodeHarness h = leader(3, new InMemoryRaftStorage());
        h.receive(2, new AppendEntriesResponse(9, false, 0, 0, 0));
        assertEquals(ProposeResult.Status.NOT_LEADER, h.node.propose("x".getBytes(), h.now).status());
    }

    @Test
    void R17_aProposedCommandIsCommittedWhenAMajorityAcknowledgesIt() {
        NodeHarness h = leader(3, new InMemoryRaftStorage());
        h.receive(2, new AppendEntriesResponse(1, true, 1, 0, 0));
        ProposeResult r = h.node.propose("cmd".getBytes(), h.now);
        h.receive(3, new AppendEntriesResponse(1, true, r.index(), 0, 0));
        assertEquals(2, h.node.commitIndex());
        assertEquals("cmd", new String(h.committedEntries.get(1).command()));
    }

    @Test
    void theLeaderTellsFollowersTheCommitIndexInTheNextMessage() {
        NodeHarness h = leader(3, new InMemoryRaftStorage());
        h.receive(2, new AppendEntriesResponse(1, true, 1, 0, 0));
        h.sent.clear();
        h.fireNextTimer();
        for (AppendEntries ae : h.sentOfType(AppendEntries.class)) {
            assertEquals(1, ae.leaderCommit());
        }
    }
}
