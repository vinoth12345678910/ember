package com.ember.raft.sim;

import com.ember.raft.InMemoryRaftStorage;
import com.ember.raft.LogEntry;
import com.ember.raft.Role;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The safety laws of Figure 3, checked after every simulated event. The checker is an INDEPENDENT
 * observer: it rebuilds what it needs from the listener callbacks and from the nodes' storage, and
 * never calls into the logic it is judging.
 *
 * Every event touches exactly one node (the one that received a message, ticked, proposed, crashed
 * or restarted), so after each event only that node's state can have changed.
 */
final class InvariantChecker {

    /**
     * A copy of one node's log together with a hash of every PREFIX of it. Logs only change by
     * deleting a suffix and appending, so after an event we find the first changed index by scanning
     * back from the end; the work is proportional to what changed.
     */
    private static final class LogMirror {
        private final ArrayList<LogEntry> entries = new ArrayList<>();
        private long[] prefixHash = new long[16];

        int size() {
            return entries.size();
        }

        long termAt(int index) {          // 1-based
            return entries.get(index - 1).term();
        }

        long hashAt(int index) {
            return prefixHash[index - 1];
        }

        /** Brings the mirror up to date; returns the first index that changed (size()+1 if nothing did). */
        int update(InMemoryRaftStorage storage) {
            int newLen = (int) storage.lastIndex();
            int common = Math.min(entries.size(), newLen);
            while (common > 0) {
                LogEntry mine = entries.get(common - 1);
                LogEntry actual = storage.entryAt(common);
                if (mine == actual || mine.equals(actual)) {
                    break;
                }
                common--;
            }
            int firstChanged = common + 1;
            while (entries.size() > common) {
                entries.remove(entries.size() - 1);
            }
            for (int i = common + 1; i <= newLen; i++) {
                LogEntry e = storage.entryAt(i);
                long before = i == 1 ? 0 : prefixHash[i - 2];
                if (i > prefixHash.length) {
                    prefixHash = java.util.Arrays.copyOf(prefixHash, prefixHash.length * 2);
                }
                prefixHash[i - 1] = Long.rotateLeft(before, 5) * 0x9E3779B97F4A7C15L
                        + e.hashCode() * 0xBF58476D1CE4E5B9L + e.term() + i;
                entries.add(e);
            }
            return firstChanged;
        }
    }

    private final SimCluster cluster;
    private final int n;

    // ElectionSafety: term -> the one node that was ever leader in it (never cleared, crashes included)
    private final Map<Long, Integer> leaderOfTerm = new TreeMap<>();
    // highest term each node was ever seen with (survives crashes: that is the point)
    private final long[] highestTerm;
    // log mirrors (also for dead nodes: their durable log still counts)
    private final LogMirror[] mirrors;
    // LeaderAppendOnly: the term a node is currently leader in (0 = not leader), and whether it became leader in this very event
    private final long[] leaderTerm;
    private final boolean[] becameLeaderThisEvent;
    // StateMachineSafety / LeaderCompleteness: the first entry ever committed at each index (1-based => index-1)
    private final ArrayList<LogEntry> committed = new ArrayList<>();
    // per node lifetime
    private final long[] nextExpectedApply;
    private final long[] lastCommitIndex;

    InvariantChecker(SimCluster cluster) {
        this.cluster = cluster;
        this.n = cluster.nodeCount();
        this.highestTerm = new long[n + 1];
        this.mirrors = new LogMirror[n + 1];
        for (int i = 0; i <= n; i++) {
            mirrors[i] = new LogMirror();
        }
        this.leaderTerm = new long[n + 1];
        this.becameLeaderThisEvent = new boolean[n + 1];
        this.nextExpectedApply = new long[n + 1];
        this.lastCommitIndex = new long[n + 1];
    }

    Map<Long, Integer> leaderHistory() {
        return leaderOfTerm;
    }

    // ------------------------------------------------------------------ callbacks (arrive inside a node's call)

    void onRoleChange(int node, Role role, long term, int leaderId) {
        if (role != Role.LEADER) {
            leaderTerm[node] = 0;
            return;
        }
        Integer previous = leaderOfTerm.get(term);
        if (previous != null && previous != node) {
            throw cluster.violation("ElectionSafety", "two different leaders in term " + term + ": n" + previous
                    + " and n" + node);
        }
        leaderOfTerm.put(term, node);
        leaderTerm[node] = term;
        becameLeaderThisEvent[node] = true;
        checkLeaderCompleteness(node, term);
    }

    /** Figure 3 Leader Completeness: a new leader holds every entry that was committed before it was elected. */
    private void checkLeaderCompleteness(int node, long term) {
        InMemoryRaftStorage log = cluster.storage(node);
        for (int i = 1; i <= committed.size(); i++) {
            LogEntry expected = committed.get(i - 1);
            if (i > log.lastIndex()) {
                throw cluster.violation("LeaderCompleteness", "n" + node + " became leader of term " + term
                        + " with a log of length " + log.lastIndex() + " but index " + i + " (" + expected + ") was already committed");
            }
            if (!log.entryAt(i).equals(expected)) {
                throw cluster.violation("LeaderCompleteness", "n" + node + " became leader of term " + term
                        + " but its entry at index " + i + " is " + log.entryAt(i) + " while " + expected + " was already committed");
            }
        }
    }

    void onCommitted(int node, long index, LogEntry entry) {
        if (index != nextExpectedApply[node]) {
            throw cluster.violation("ApplyOrder", "n" + node + " applied index " + index + " but should have applied "
                    + nextExpectedApply[node] + " (exactly once, in order, no gaps)");
        }
        nextExpectedApply[node]++;
        if (index <= committed.size()) {
            LogEntry first = committed.get((int) index - 1);
            if (!first.equals(entry)) {
                throw cluster.violation("StateMachineSafety", "n" + node + " applied " + entry + " at index " + index
                        + " but another node applied " + first + " there");
            }
        } else if (index == committed.size() + 1) {
            committed.add(entry);
        } else {
            throw cluster.violation("ApplyOrder", "n" + node + " applied index " + index + " but only " + committed.size()
                    + " indexes were committed so far (a gap)");
        }
    }

    void onCrash(int node) {
        leaderTerm[node] = 0;
    }

    void onRestart(int node) {
        leaderTerm[node] = 0;
        nextExpectedApply[node] = 1;          // a restarted node re-applies from index 1
        lastCommitIndex[node] = 0;
    }

    // ------------------------------------------------------------------ after every event

    void afterEvent(int node) {
        long term = cluster.term(node);
        if (term < highestTerm[node]) {
            throw cluster.violation("TermMonotonic", "n" + node + " has currentTerm=" + term
                    + " but was already in term " + highestTerm[node]);
        }
        highestTerm[node] = term;

        LogMirror mirror = mirrors[node];
        int oldLen = mirror.size();
        int firstChanged = mirror.update(cluster.storage(node));

        // LeaderAppendOnly: a leader that was already leader before this event only ever appends
        if (leaderTerm[node] != 0 && !becameLeaderThisEvent[node] && firstChanged <= oldLen) {
            throw cluster.violation("LeaderAppendOnly", "leader n" + node + " (term " + leaderTerm[node]
                    + ") changed its log at index " + firstChanged + " but its log had " + oldLen + " entries");
        }
        becameLeaderThisEvent[node] = false;

        // LogMatching: compare only what changed against every other node
        for (int i = firstChanged; i <= mirror.size(); i++) {
            for (int other = 1; other <= n; other++) {
                if (other == node) {
                    continue;
                }
                LogMirror o = mirrors[other];
                if (o.size() >= i && o.termAt(i) == mirror.termAt(i) && o.hashAt(i) != mirror.hashAt(i)) {
                    throw cluster.violation("LogMatching", "n" + node + " and n" + other + " both have an entry with term "
                            + mirror.termAt(i) + " at index " + i + " but their logs differ up to that index");
                }
            }
        }

        if (cluster.isAlive(node)) {
            long commit = cluster.commitIndex(node);
            if (commit < lastCommitIndex[node]) {
                throw cluster.violation("CommitIndexMonotonic", "n" + node + " commitIndex went from "
                        + lastCommitIndex[node] + " back to " + commit);
            }
            if (commit > cluster.storage(node).lastIndex()) {
                throw cluster.violation("CommitIndexBounded", "n" + node + " commitIndex=" + commit
                        + " is beyond its last log index " + cluster.storage(node).lastIndex());
            }
            lastCommitIndex[node] = commit;
        }
    }

    /** For tests and reports. */
    List<LogEntry> committedEntries() {
        return List.copyOf(committed);
    }
}
