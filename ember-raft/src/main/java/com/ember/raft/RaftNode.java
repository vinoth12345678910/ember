package com.ember.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * One Raft server: a deterministic, single-threaded state machine. The CALLER drives it with
 * tick / receive / propose and must serialize the calls: the node is deliberately NOT thread-safe,
 * because locking would make behaviour depend on thread timing and failures could not be replayed.
 *
 * Every public entry point follows the OUTBOX pattern:
 *   1. handle the event (change state in memory, queue outgoing messages and callbacks),
 *   2. storage.sync() if anything persistent changed,
 *   3. only THEN send the queued messages and deliver the callbacks.
 * A node must never tell anybody about state it has not made durable (a vote, an appended entry),
 * otherwise a crash can make a promise disappear after somebody relied on it.
 */
public final class RaftNode {

    private record Outgoing(int to, RaftMessage message) {
    }

    private final RaftConfig config;
    private final RaftStorage storage;
    private final RaftTransport transport;
    private final RaftListener listener;
    private final Random random;

    // ---- volatile state (Figure 2): rebuilt after a restart (R18)
    private Role role = Role.FOLLOWER;
    private int leaderId = -1;
    private long commitIndex = 0;
    private long lastApplied = 0;
    private long electionDeadlineMs;

    // ---- the outbox
    private final List<Outgoing> outbox = new ArrayList<>();
    private final List<Runnable> notifications = new ArrayList<>();
    private boolean persistentChange = false;

    // role/leader last reported to the listener (so we only report real changes)
    private Role notifiedRole = null;
    private int notifiedLeader = -2;

    // ---- candidate state: a set, so a duplicated response from the same node counts once (R6)
    private final java.util.TreeSet<Integer> votesReceived = new java.util.TreeSet<>();

    // ---- leader state (Figure 2), indexed by position in config.clusterNodeIds()
    private final long[] nextIndex;
    private final long[] matchIndex;
    private long heartbeatDeadlineMs;
    /** CheckQuorum: when each peer last answered us (indexed like nextIndex). */
    private final long[] lastHeardMs;
    /** Accelerated backtracking (paper 5.3, end). A switch only so a test can compare it with decrement-by-one. */
    private boolean conflictHintEnabled = true;

    public RaftNode(RaftConfig config, RaftStorage storage, RaftTransport transport, RaftListener listener,
                    Random random, long nowMs) {
        this.config = config;
        this.storage = storage;
        this.transport = transport;
        this.listener = listener;
        this.random = random;
        this.nextIndex = new long[config.clusterNodeIds().size()];
        this.matchIndex = new long[config.clusterNodeIds().size()];
        this.lastHeardMs = new long[config.clusterNodeIds().size()];
        // R18: a (re)started node is a FOLLOWER with a fresh election deadline; term, vote and log come from storage.
        this.electionDeadlineMs = nowMs + randomElectionTimeout();
        if (config.injectedBug() == Bug.FORGET_VOTED_FOR_ON_RESTART && storage.votedFor() != -1) {
            // TEST-ONLY BUG: the vote is forgotten on restart, so the node can vote again in the same term.
            storage.setTermAndVote(storage.currentTerm(), -1);
            persistentChange = true;
        }
        flush();
    }

    // ------------------------------------------------------------------ public API

    /** Fires every timer that is due at nowMs. */
    public void tick(long nowMs) {
        if (role == Role.LEADER) {
            // R8: heartbeats during idle periods. R16: a leader has no election timer.
            if (nowMs >= heartbeatDeadlineMs) {
                if (config.checkQuorum() && !heardFromMajorityRecently(nowMs)) {
                    stepDownWithoutNewTerm(nowMs);          // cut off from the cluster: stop pretending to lead
                } else {
                    broadcastAppendEntries();
                    heartbeatDeadlineMs = nowMs + config.heartbeatIntervalMs();
                }
            }
        } else if (nowMs >= electionDeadlineMs) {
            startElection(nowMs);                         // R4
        }
        flush();
    }

    /** Handles one incoming message. */
    public void receive(int fromNodeId, RaftMessage message, long nowMs) {
        // R1 (Figure 2, "All Servers"): a newer term makes us a follower of that term BEFORE we look at the message.
        if (message.term() > storage.currentTerm()) {
            adoptHigherTerm(message.term(), nowMs);
        }
        switch (message) {
            case RaftMessage.RequestVote m -> onRequestVote(fromNodeId, m, nowMs);
            case RaftMessage.RequestVoteResponse m -> onRequestVoteResponse(fromNodeId, m, nowMs);
            case RaftMessage.AppendEntries m -> onAppendEntries(fromNodeId, m, nowMs);
            case RaftMessage.AppendEntriesResponse m -> onAppendEntriesResponse(fromNodeId, m, nowMs);
        }
        flush();
    }

    /** R17 (5.3): the leader appends the command to its own log and replicates it; anybody else says NOT_LEADER. */
    public ProposeResult propose(byte[] command, long nowMs) {
        if (role != Role.LEADER) {
            return new ProposeResult(ProposeResult.Status.NOT_LEADER, 0, 0, leaderId);
        }
        long term = storage.currentTerm();
        storage.append(List.of(LogEntry.command(term, command)));
        persistentChange = true;
        long index = storage.lastIndex();
        broadcastAppendEntries();
        advanceCommitIndex();                              // a single-node cluster commits right here
        flush();
        return new ProposeResult(ProposeResult.Status.ACCEPTED, index, term, config.nodeId());
    }

    /** Test hook: switch accelerated backtracking off to measure plain decrement-by-one. */
    void setConflictHintEnabledForTesting(boolean enabled) {
        this.conflictHintEnabled = enabled;
    }

    /** The earliest absolute time at which tick() must be called. */
    public long nextTimerMs() {
        return role == Role.LEADER ? heartbeatDeadlineMs : electionDeadlineMs;
    }

    public long currentTerm() {
        return storage.currentTerm();
    }

    public int votedFor() {
        return storage.votedFor();
    }

    public int nodeId() {
        return config.nodeId();
    }

    public Role role() {
        return role;
    }

    public int leaderId() {
        return leaderId;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long lastApplied() {
        return lastApplied;
    }

    public long lastLogIndex() {
        return storage.lastIndex();
    }

    public long lastLogTerm() {
        return storage.lastTerm();
    }

    // ------------------------------------------------------------------ election (R4; the rest is Step 3)

    /** R4 (5.2, Figure 2 "Rules for Servers: Followers/Candidates"). */
    private void startElection(long nowMs) {
        long term = storage.currentTerm() + 1;
        storage.setTermAndVote(term, config.nodeId());    // term and vote are written together; the vote is for ourselves
        persistentChange = true;
        role = Role.CANDIDATE;
        leaderId = -1;
        votesReceived.clear();
        votesReceived.add(config.nodeId());
        electionDeadlineMs = nowMs + randomElectionTimeout();   // a NEW random deadline for this attempt
        notifyRoleIfChanged();
        for (int peer : config.clusterNodeIds()) {
            if (peer != config.nodeId()) {
                queue(peer, new RaftMessage.RequestVote(term, config.nodeId(), storage.lastIndex(), storage.lastTerm()));
            }
        }
        if (votesReceived.size() >= config.majority()) {
            becomeLeader(nowMs);                            // a single-node cluster wins at once (R6)
        }
    }

    /** R1: persist the new term together with a cleared vote, and step down to follower. */
    private void adoptHigherTerm(long term, long nowMs) {
        storage.setTermAndVote(term, -1);
        persistentChange = true;
        boolean wasLeader = role == Role.LEADER;
        role = Role.FOLLOWER;
        leaderId = -1;
        votesReceived.clear();
        if (wasLeader) {
            electionDeadlineMs = nowMs + randomElectionTimeout();   // a leader had no deadline; a follower needs one
        }
        notifyRoleIfChanged();
    }

    /** R5 (5.2, 5.4.1; Figure 2 RequestVote receiver). */
    private void onRequestVote(int from, RaftMessage.RequestVote m, long nowMs) {
        long term = storage.currentTerm();
        boolean grant = false;
        if (m.term() >= term) {                             // R2: older terms are rejected below
            boolean notVotedForSomeoneElse = storage.votedFor() == -1 || storage.votedFor() == m.candidateId();
            // 5.4.1: the candidate's log must be at least as up to date as ours
            boolean candidateLogUpToDate = m.lastLogTerm() > storage.lastTerm()
                    || (m.lastLogTerm() == storage.lastTerm() && m.lastLogIndex() >= storage.lastIndex());
            if (config.injectedBug() == Bug.VOTE_WITHOUT_LOG_CHECK) {
                candidateLogUpToDate = true;                // TEST-ONLY BUG: skip the election restriction
            }
            grant = notVotedForSomeoneElse && candidateLogUpToDate;
            if (grant) {
                if (storage.votedFor() != m.candidateId()) {
                    storage.setTermAndVote(term, m.candidateId());   // durable before the reply leaves (outbox)
                    persistentChange = true;
                }
                electionDeadlineMs = nowMs + randomElectionTimeout();   // reset ONLY when the vote is granted
            }
        }
        queue(from, new RaftMessage.RequestVoteResponse(storage.currentTerm(), grant));
    }

    /** R6 (5.2). */
    private void onRequestVoteResponse(int from, RaftMessage.RequestVoteResponse m, long nowMs) {
        // R2: responses of another term (older, or a newer one that already made us a follower) are ignored
        if (role != Role.CANDIDATE || m.term() != storage.currentTerm()) {
            return;
        }
        if (m.voteGranted()) {
            votesReceived.add(from);
            if (votesReceived.size() >= config.majority()) {
                becomeLeader(nowMs);
            }
        }
    }

    /** R8 (5.3, 8): initialize leader state, append a no-op of the new term, announce ourselves at once. */
    private void becomeLeader(long nowMs) {
        role = Role.LEADER;
        leaderId = config.nodeId();
        votesReceived.clear();
        for (int i = 0; i < nextIndex.length; i++) {
            nextIndex[i] = storage.lastIndex() + 1;
            matchIndex[i] = 0;
        }
        storage.append(List.of(LogEntry.noop(storage.currentTerm())));   // Figure 8: commit something of OUR term
        persistentChange = true;
        broadcastAppendEntries();
        heartbeatDeadlineMs = nowMs + config.heartbeatIntervalMs();
        java.util.Arrays.fill(lastHeardMs, nowMs);          // CheckQuorum grace period: a new leader gets electionTimeoutMax to be heard
        notifyRoleIfChanged();
        advanceCommitIndex();                               // in a single-node cluster the no-op is committed at once
    }

    private void broadcastAppendEntries() {
        for (int peer : config.clusterNodeIds()) {
            if (peer != config.nodeId()) {
                sendAppendEntries(peer);
            }
        }
    }

    private void sendAppendEntries(int peer) {
        int pos = positionOf(peer);
        long next = nextIndex[pos];
        long prev = next - 1;
        queue(peer, new RaftMessage.AppendEntries(storage.currentTerm(), config.nodeId(), prev, storage.termAt(prev),
                limitByBytes(storage.entries(next, config.maxEntriesPerAppend())), commitIndex));
    }

    /**
     * Keeps entries while the total stays within maxAppendBytes; ALWAYS keeps the first one, otherwise an entry
     * bigger than the cap could never be replicated (the follower would be stuck forever).
     */
    private List<LogEntry> limitByBytes(List<LogEntry> candidates) {
        long total = 0;
        int count = 0;
        for (LogEntry e : candidates) {
            long size = e.rawCommand().length + 16L;       // payload plus a fixed overhead for term/type/length
            if (count > 0 && total + size > config.maxAppendBytes()) {
                break;
            }
            total += size;
            count++;
        }
        return count == candidates.size() ? candidates : candidates.subList(0, count);
    }

    /** CheckQuorum: have a majority (ourselves included) answered within electionTimeoutMax? */
    private boolean heardFromMajorityRecently(long nowMs) {
        int heard = 1;
        for (int i = 0; i < lastHeardMs.length; i++) {
            if (config.clusterNodeIds().get(i) != config.nodeId() && nowMs - lastHeardMs[i] <= config.electionTimeoutMaxMs()) {
                heard++;
            }
        }
        return heard >= config.majority();
    }

    /** Leader -> follower WITHOUT touching the term: a local observation, not a new election. */
    private void stepDownWithoutNewTerm(long nowMs) {
        role = Role.FOLLOWER;
        leaderId = -1;
        votesReceived.clear();
        electionDeadlineMs = nowMs + randomElectionTimeout();
        notifyRoleIfChanged();
    }

    private int positionOf(int nodeId) {
        return java.util.Collections.binarySearch(config.clusterNodeIds(), nodeId);
    }

    /** Receiver side of AppendEntries (Figure 2). */
    private void onAppendEntries(int from, RaftMessage.AppendEntries m, long nowMs) {
        long term = storage.currentTerm();
        if (m.term() < term) {                              // R9 / R2
            queue(from, new RaftMessage.AppendEntriesResponse(term, false, 0, 0, 0));
            return;
        }
        // R7: a candidate (or any non-follower) that hears from a leader of its own term or newer follows it
        role = Role.FOLLOWER;
        votesReceived.clear();
        leaderId = m.leaderId();
        electionDeadlineMs = nowMs + randomElectionTimeout();     // R10: a legitimate leader is alive
        notifyRoleIfChanged();

        // R11 (5.3 Log Matching): we must hold the entry that precedes the new ones
        long prev = m.prevLogIndex();
        if (prev > storage.lastIndex()) {                   // our log is too short
            queue(from, new RaftMessage.AppendEntriesResponse(term, false, 0, storage.lastIndex() + 1, 0));
            return;
        }
        if (storage.termAt(prev) != m.prevLogTerm()) {      // same index, different term
            long conflictTerm = storage.termAt(prev);
            long conflictIndex = prev;
            while (conflictIndex > 1 && storage.termAt(conflictIndex - 1) == conflictTerm) {
                conflictIndex--;                            // the first index of that term in OUR log
            }
            queue(from, new RaftMessage.AppendEntriesResponse(term, false, 0, conflictIndex, conflictTerm));
            return;
        }

        // R12: delete only on a real conflict, then append what we do not have yet
        List<LogEntry> entries = m.entries();
        int firstToAppend = 0;
        if (config.injectedBug() == Bug.TRUNCATE_ON_EVERY_APPEND && !entries.isEmpty()) {
            // TEST-ONLY BUG: "make room" by truncating even when the entries already match
            if (prev + 1 <= storage.lastIndex()) {
                storage.truncateFrom(prev + 1);
                persistentChange = true;
            }
        } else {
            long index = prev + 1;
            for (; firstToAppend < entries.size(); firstToAppend++, index++) {
                if (index > storage.lastIndex()) {
                    break;                                  // beyond our log: everything from here is new
                }
                if (storage.termAt(index) != entries.get(firstToAppend).term()) {
                    storage.truncateFrom(index);            // conflict: this entry and all that follow go
                    persistentChange = true;
                    break;
                }                                           // else it already matches: leave it alone
            }
        }
        if (firstToAppend < entries.size()) {
            storage.append(entries.subList(firstToAppend, entries.size()));
            persistentChange = true;
        }

        // R13: never beyond what THIS message proved we agree on
        long lastNew = prev + entries.size();
        if (m.leaderCommit() > commitIndex) {
            commitIndex = Math.max(commitIndex, Math.min(m.leaderCommit(), lastNew));
        }
        applyCommitted();
        queue(from, new RaftMessage.AppendEntriesResponse(term, true, lastNew, 0, 0));
    }

    /** Leader side: R14 (nextIndex/matchIndex) and R15 (commit). */
    private void onAppendEntriesResponse(int from, RaftMessage.AppendEntriesResponse m, long nowMs) {
        if (role != Role.LEADER || m.term() != storage.currentTerm()) {
            return;                                         // R2: stale (or a higher term already made us a follower)
        }
        int pos = positionOf(from);
        if (pos < 0) {
            return;
        }
        lastHeardMs[pos] = nowMs;                           // CheckQuorum: any answer in our term proves the peer is reachable
        if (m.success()) {
            matchIndex[pos] = Math.max(matchIndex[pos], m.matchIndex());   // never backwards: responses can be late
            nextIndex[pos] = matchIndex[pos] + 1;
            advanceCommitIndex();
            if (nextIndex[pos] <= storage.lastIndex()) {
                sendAppendEntries(from);                    // still behind: keep sending
            }
            return;
        }
        long old = nextIndex[pos];
        long candidate = conflictHintEnabled ? nextIndexFromHint(m) : old - 1;
        if (candidate >= old) {
            candidate = old - 1;                            // a hint that does not make progress: fall back to one step
        }
        long next = Math.max(Math.max(1, matchIndex[pos] + 1), candidate);
        if (next != old) {                                  // otherwise it was a stale failure: nothing to retry
            nextIndex[pos] = next;
            sendAppendEntries(from);
        }
    }

    /** Accelerated backtracking: where should we resume, given the follower's hint? */
    private long nextIndexFromHint(RaftMessage.AppendEntriesResponse m) {
        if (m.conflictTerm() == 0) {
            return m.conflictIndex();                       // follower's log is too short: resume right after its end
        }
        for (long i = storage.lastIndex(); i >= 1; i--) {
            long t = storage.termAt(i);
            if (t == m.conflictTerm()) {
                return i + 1;                               // we have that term: resume after our last entry of it
            }
            if (t < m.conflictTerm()) {
                break;                                      // terms only grow along a log: we have none
            }
        }
        return m.conflictIndex();                           // we do not have that term: skip all of it
    }

    /**
     * R15 (5.3, 5.4.2, Figure 8): the highest N stored on a majority AND belonging to the current term.
     * Older-term entries are never committed by counting replicas; they become committed indirectly.
     */
    private void advanceCommitIndex() {
        int size = nextIndex.length;
        long[] sorted = new long[size];
        for (int i = 0; i < size; i++) {
            sorted[i] = config.clusterNodeIds().get(i) == config.nodeId() ? storage.lastIndex() : matchIndex[i];
        }
        java.util.Arrays.sort(sorted);
        long candidate = sorted[size - config.majority()];  // the largest N that at least a majority holds
        if (candidate <= commitIndex) {
            return;
        }
        boolean currentTerm = storage.termAt(candidate) == storage.currentTerm();
        if (config.injectedBug() == Bug.COMMIT_OLD_TERM_BY_COUNTING) {
            currentTerm = true;                             // TEST-ONLY BUG: the Figure 8 mistake
        }
        if (currentTerm) {
            commitIndex = candidate;
            applyCommitted();
        }
    }

    /** R3 (Figure 2 "All Servers"): apply committed entries in order, one callback per index. */
    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            long index = lastApplied;
            LogEntry entry = storage.entryAt(index);
            notifications.add(() -> listener.onCommitted(config.nodeId(), index, entry));
        }
    }

    // ------------------------------------------------------------------ helpers

    /** A draw in [min, max], both inclusive. */
    private long randomElectionTimeout() {
        long span = config.electionTimeoutMaxMs() - config.electionTimeoutMinMs() + 1;
        return config.electionTimeoutMinMs() + (long) random.nextInt((int) span);
    }

    private void queue(int to, RaftMessage message) {
        outbox.add(new Outgoing(to, message));
    }

    private void notifyRoleIfChanged() {
        if (role != notifiedRole || leaderId != notifiedLeader) {
            notifiedRole = role;
            notifiedLeader = leaderId;
            Role r = role;
            long term = storage.currentTerm();
            int leader = leaderId;
            notifications.add(() -> listener.onRoleChange(config.nodeId(), r, term, leader));
        }
    }

    /**
     * Steps 2 and 3 of the outbox pattern: make state durable, THEN talk to the outside world.
     */
    private void flush() {
        if (config.injectedBug() == Bug.ACK_BEFORE_SYNC) {
            // TEST-ONLY BUG: tell everybody first, make it durable afterwards.
            deliver();
            syncIfNeeded();
            return;
        }
        syncIfNeeded();
        deliver();
    }

    private void syncIfNeeded() {
        if (persistentChange) {
            storage.sync();
            persistentChange = false;
        }
    }

    private void deliver() {
        List<Outgoing> toSend = new ArrayList<>(outbox);
        List<Runnable> toNotify = new ArrayList<>(notifications);
        outbox.clear();
        notifications.clear();
        // callbacks first: a role change reads better BEFORE the messages it causes (it matters only for traces)
        for (Runnable n : toNotify) {
            n.run();
        }
        for (Outgoing o : toSend) {
            transport.send(o.to(), o.message());
        }
    }
}
