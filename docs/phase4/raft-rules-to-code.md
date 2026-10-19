# Raft rules to code: Figure 2 and rules R1-R18

Every row names the paper reference, the method in `com.ember.raft.RaftNode` (or the class that implements it), and the tests that prove it. Tests live in `ember-raft/src/test/java/com/ember/raft/`.
`ER` = `ElectionRulesTest`, `RR` = `ReplicationRulesTest`, `NS` = `NodeSkeletonTest`, `BT` = `BacktrackingTest`, `SC` = `sim/ScriptedScenariosTest`, `CT` = `sim/CheckerTest`.

## Persistent and volatile state (Figure 2, top)

| Figure 2 item | Where | Proved by |
|---|---|---|
| `currentTerm`, `votedFor`, `log[]` persistent; written before responding | `RaftStorage` / `InMemoryRaftStorage`; the outbox in `RaftNode.flush()` (sync, then send) | `InMemoryRaftStorageTest`; `NS.aMessageIsNeverSentBeforeStorageWasSynced`; `ER.R5_theVoteIsDurableBeforeTheResponseIsSent`; `RR.R12_theWritesAreDurableBeforeTheReply`; `BugDetectionTest.acknowledgingBeforeSyncIsCaught` |
| `commitIndex`, `lastApplied` volatile | fields in `RaftNode`, rebuilt by the constructor | `ER.R18_*`, `NS.aRestartKeepsTermVoteAndLogButResetsEverythingElse` |
| `nextIndex[]`, `matchIndex[]` volatile, reinitialized on election | `RaftNode.becomeLeader` | `ER.R8_*` |

## Rules R1-R18

| Rule | Paper | Implemented in | Tests |
|---|---|---|---|
| **R1** newer term -> adopt it, clear vote, become follower, then process | Fig. 2 "All Servers" 2; 5.1 | `RaftNode.receive` + `adoptHigherTerm` | `ER.R1_*` (4 tests) |
| **R2** older-term requests rejected with our term; stale responses ignored | Fig. 2 RequestVote 1, AppendEntries 1; 5.1 | `onRequestVote`, `onAppendEntries`, guards in `onRequestVoteResponse`, `onAppendEntriesResponse` | `ER.R2_*` |
| **R3** apply `commitIndex > lastApplied` in order, once | Fig. 2 "All Servers" 1 | `applyCommitted` | `RR.R3_*`; `CT.applyOrderCatchesGapsAndRepeats` |
| **R4** timeout: term+1, vote self, new random deadline, ask everyone; single node wins at once | 5.2; Fig. 2 "Candidates" | `startElection`, `randomElectionTimeout` | `ER.R4_*`; `NS.electionTimeoutsAreAlwaysInsideMinMax`, `NS.everyNewElectionAttemptDrawsAFreshDeadline...` |
| **R5** vote once per term; candidate log at least as up to date; persist before replying; reset deadline only when granted | 5.2, 5.4.1; Fig. 2 RequestVote 2 | `onRequestVote` | `ER.R5_*` (8 tests); `BugDetectionTest.voteWithoutLogCheckIsCaught` |
| **R6** majority of votes (self included, each node once) -> leader | 5.2 | `onRequestVoteResponse` (`TreeSet` of voters), `RaftConfig.majority` | `ER.R6_*` |
| **R7** candidate steps down on AppendEntries with term >= its own | 5.2; Fig. 2 "Candidates" | `onAppendEntries` | `ER.R7_*` |
| **R8** new leader: init `nextIndex`/`matchIndex`, append no-op, AppendEntries to all at once, heartbeats | 5.3, 8; Fig. 2 "Leaders" | `becomeLeader`, `tick`, `broadcastAppendEntries`, `sendAppendEntries` | `ER.R8_*` |
| **R9** AppendEntries with old term -> false | Fig. 2 AppendEntries 1 | `onAppendEntries` | `RR.R9_*` |
| **R10** valid AppendEntries resets the election deadline, records the leader | 5.2 | `onAppendEntries` | `RR.R10_*` |
| **R11** no matching `prevLogIndex/prevLogTerm` -> false (+ conflict hint) | Fig. 2 AppendEntries 2; 5.3 | `onAppendEntries` | `RR.R11_*` |
| **R12** delete only on a real conflict, append what is missing; never truncate a match | Fig. 2 AppendEntries 3-4 | `onAppendEntries` | `RR.R12_*`; `BugDetectionTest.truncatingOnEveryAppendIsCaught` |
| **R13** `commitIndex = min(leaderCommit, last new entry)`, never decreasing | Fig. 2 AppendEntries 5 | `onAppendEntries` | `RR.R13_*` |
| **R14** success: `matchIndex = max(...)`, `nextIndex = match+1`; failure: back off (hint) and resend | Fig. 2 "Leaders" 3; 5.3 | `onAppendEntriesResponse`, `nextIndexFromHint` | `RR.R14_*`; `BT.*` (hint vs decrement) |
| **R15** commit N if majority `matchIndex >= N` and `log[N].term == currentTerm` | Fig. 2 "Leaders" 4; 5.4.2; Fig. 8 | `advanceCommitIndex` | `RR.R15_*`; `SC.figure8_*`; `BugDetectionTest.commitOldTermByCounting...` |
| **R16** leader steps down on a higher term; a leader has no election timer | Fig. 2 "All Servers" 2; 5.1 | `receive` (R1) + `tick` (leader branch) + `nextTimerMs` | `ER.R16_*` |
| **R17** `propose`: leader appends + replicates; others NOT_LEADER with hint | 5.3 | `propose` | `RR.R17_*` |
| **R18** term, vote, log persist; the rest is volatile; restart = follower with fresh deadline | Fig. 2 state; 5.2 | constructor; `InMemoryRaftStorage.crash()` | `ER.R18_*`; `NS.aRestartKeeps...`; `ER.R5_aPersistedVoteSurvivesARestart...` |

## The five safety properties (Figure 3) and where they are checked

| Property | Checker (`sim/InvariantChecker`) | Proof the checker can fail |
|---|---|---|
| Election Safety | `onRoleChange` (term -> leader map, never cleared) | `ElectionSafetyCheckerTest`; bug `FORGET_VOTED_FOR_ON_RESTART` |
| Leader Append-Only | `afterEvent` (log mirror) | `CT.leaderAppendOnly*` |
| Log Matching | `afterEvent` (prefix hashes) | `CT.logMatching*`; bug `TRUNCATE_ON_EVERY_APPEND` |
| Leader Completeness | `checkLeaderCompleteness` at election | `CT.leaderCompleteness*`; bugs `VOTE_WITHOUT_LOG_CHECK`, `ACK_BEFORE_SYNC`, Figure 8 |
| State Machine Safety | `onCommitted` (first value per index) | `CT.stateMachineSafety*`; bugs above |
| sanity: terms/commit never decrease, apply in order, `commitIndex <= lastIndex` | `afterEvent`, `onCommitted` | `ElectionSafetyCheckerTest`, `CT.applyOrder*`; bug `TRUNCATE_ON_EVERY_APPEND` trips `CommitIndexBounded` |
