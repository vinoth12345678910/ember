package com.ember.raft.sim;

import com.ember.raft.*;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.function.BooleanSupplier;

/**
 * A whole Raft cluster in one thread, with a virtual clock and a hostile network.
 * Deterministic by construction: one thread, (time, sequence) event ordering, nodes iterated in id
 * order, and randomness only from seeded java.util.Random streams.
 */
public final class SimCluster {

    /** Thrown from the transport to simulate losing power in the middle of a node's call. */
    private static final class SimCrash extends RuntimeException {
        SimCrash() {
            super("simulated power loss", null, false, false);
        }
    }

    private static final class Event implements Comparable<Event> {
        final long time;
        final long seq;
        final int from;
        final int to;
        final RaftMessage message;      // null for a scheduled action
        final Runnable action;

        Event(long time, long seq, int from, int to, RaftMessage message, Runnable action) {
            this.time = time;
            this.seq = seq;
            this.from = from;
            this.to = to;
            this.message = message;
            this.action = action;
        }

        @Override
        public int compareTo(Event o) {
            int c = Long.compare(time, o.time);
            return c != 0 ? c : Long.compare(seq, o.seq);
        }
    }

    /** Counters for the summary. */
    public static final class Stats {
        public long sent, delivered, dropped, duplicated, lostToPartition, lostToDeadNode, events;
    }

    private final SimConfig config;
    private final int n;
    private final List<Integer> ids;
    private long now = 0;
    private long seq = 0;
    private final PriorityQueue<Event> queue = new PriorityQueue<>();
    private final RaftNode[] nodes;
    private final InMemoryRaftStorage[] storages;
    private final Random[] nodeRandoms;
    private final Random netRandom;
    private final boolean[] alive;
    private final boolean[][] blocked;
    private final long[][] latencyOverride;
    private final boolean[] crashOnNextSend;
    private final long[] rebootAfterMs;          // >= 0: restart this many ms after a crash-on-next-send; -1: stay dead
    private final List<List<LogEntry>> applied = new ArrayList<>();
    private double rebootAfterVoteP;            // chaos: probability that a node loses power right after granting a vote
    private double dropP;
    private double dupP;
    private final Trace trace;
    private final InvariantChecker checker;
    private final Stats stats = new Stats();
    private final List<List<String>> roleHistory = new ArrayList<>();

    public SimCluster(SimConfig config) {
        this(config, java.util.Map.of(), false);
    }

    public SimCluster(SimConfig config, java.util.Map<Integer, InMemoryRaftStorage> initialStorages, boolean fullTrace) {
        this.config = config;
        this.n = config.nodeCount();
        this.ids = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            ids.add(i);
        }
        this.nodes = new RaftNode[n + 1];
        this.storages = new InMemoryRaftStorage[n + 1];
        this.nodeRandoms = new Random[n + 1];
        this.alive = new boolean[n + 1];
        this.blocked = new boolean[n + 1][n + 1];
        this.latencyOverride = new long[n + 1][n + 1];
        this.crashOnNextSend = new boolean[n + 1];
        this.rebootAfterMs = new long[n + 1];
        java.util.Arrays.fill(rebootAfterMs, -1);
        this.netRandom = new Random(mix(config.seed(), 0));
        this.dropP = config.dropProbability();
        this.dupP = config.duplicateProbability();
        this.trace = new Trace(2000, fullTrace);
        this.checker = new InvariantChecker(this);
        applied.add(new ArrayList<>());
        roleHistory.add(new ArrayList<>());
        for (int id = 1; id <= n; id++) {
            java.util.Arrays.fill(latencyOverride[id], -1);
            applied.add(new ArrayList<>());
            roleHistory.add(new ArrayList<>());
            storages[id] = initialStorages.containsKey(id) ? initialStorages.get(id) : new InMemoryRaftStorage();
            nodeRandoms[id] = new Random(mix(config.seed(), id));
        }
        for (int id = 1; id <= n; id++) {
            startNode(id);
        }
    }

    /** SplitMix-style mixing so per-node streams are unrelated to each other and to the network stream. */
    private static long mix(long seed, int salt) {
        long z = seed + 0x9E3779B97F4A7C15L * (salt + 1);
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    private RaftConfig raftConfigFor(int id) {
        RaftConfig rc = RaftConfig.of(id, ids).withBug(config.bug()).withMaxEntriesPerAppend(config.maxEntriesPerAppend())
                .withCheckQuorum(config.checkQuorum());
        long[] range = config.electionTimeouts().get(id);
        if (range != null) {
            rc = rc.withElectionTimeout(range[0], range[1]);
        }
        return rc;
    }

    private void startNode(int id) {
        alive[id] = true;
        applied.set(id, new ArrayList<>());
        checker.onRestart(id);
        invoke(id, () -> nodes[id] = new RaftNode(raftConfigFor(id), storages[id], new SimTransport(id),
                new SimListener(id), nodeRandoms[id], now));
    }

    // ------------------------------------------------------------------ the main loop

    /** Virtual time of the next thing that will happen, or Long.MAX_VALUE if nothing will. */
    public long nextEventTime() {
        long t = queue.isEmpty() ? Long.MAX_VALUE : queue.peek().time;
        for (int id = 1; id <= n; id++) {
            if (alive[id]) {
                t = Math.min(t, nodes[id].nextTimerMs());
            }
        }
        return t;
    }

    /** Processes exactly ONE event (a queued delivery/action first, else the earliest node timer). */
    public boolean step() {
        long queueTime = queue.isEmpty() ? Long.MAX_VALUE : queue.peek().time;
        long timerTime = Long.MAX_VALUE;
        int timerNode = -1;
        for (int id = 1; id <= n; id++) {
            if (alive[id] && nodes[id].nextTimerMs() < timerTime) {      // strict <: ties go to the lowest id
                timerTime = nodes[id].nextTimerMs();
                timerNode = id;
            }
        }
        if (queueTime == Long.MAX_VALUE && timerTime == Long.MAX_VALUE) {
            return false;
        }
        stats.events++;
        if (queueTime <= timerTime) {                                    // queued events before timers at the same instant
            Event ev = queue.poll();
            now = Math.max(now, ev.time);
            if (ev.action != null) {
                ev.action.run();
            } else {
                deliver(ev);
            }
        } else {
            now = Math.max(now, timerTime);
            final int id = timerNode;
            if (nodes[id].role() != Role.LEADER) {         // a due timer of a non-leader is an election timeout
                trace("n" + id + " election timeout -> CANDIDATE term=" + (nodes[id].currentTerm() + 1));
            }
            invoke(id, () -> nodes[id].tick(now));
        }
        return true;
    }

    /** Advances the virtual clock by ms, processing everything that happens on the way. */
    public void runFor(long ms) {
        long target = now + ms;
        while (nextEventTime() <= target) {
            step();
        }
        now = target;
    }

    /** Runs until the predicate holds (checked after every event) or maxMs of virtual time passed. */
    public boolean runUntil(BooleanSupplier predicate, long maxMs) {
        long limit = now + maxMs;
        if (predicate.getAsBoolean()) {
            return true;
        }
        while (nextEventTime() <= limit) {
            step();
            if (predicate.getAsBoolean()) {
                return true;
            }
        }
        now = limit;
        return predicate.getAsBoolean();
    }

    private void deliver(Event ev) {
        if (!alive[ev.to]) {
            stats.lostToDeadNode++;
            return;
        }
        if (blocked[ev.from][ev.to]) {
            stats.lostToPartition++;
            trace("LOST n" + ev.from + " -> n" + ev.to + " " + describe(ev.message) + " (link down)");
            return;
        }
        stats.delivered++;
        invoke(ev.to, () -> nodes[ev.to].receive(ev.from, ev.message, now));
    }

    /** Runs one call into a node; a simulated power loss unwinds it; then the invariants are checked. */
    private void invoke(int id, Runnable call) {
        try {
            call.run();
        } catch (SimCrash e) {
            applyCrash(id, "power loss while sending");
        }
        checker.afterEvent(id);
    }

    // ------------------------------------------------------------------ actions available to scenarios and tests

    public ProposeResult propose(int id, byte[] command) {
        if (!alive[id]) {
            return new ProposeResult(ProposeResult.Status.NOT_LEADER, 0, 0, -1);
        }
        ProposeResult[] result = new ProposeResult[1];
        invoke(id, () -> result[0] = nodes[id].propose(command, now));
        if (result[0] == null) {                       // the node lost power while replicating
            return new ProposeResult(ProposeResult.Status.NOT_LEADER, 0, 0, -1);
        }
        trace("n" + id + (result[0].accepted() ? " propose idx=" + result[0].index() + " term=" + result[0].term()
                : " propose rejected (not leader, hint=" + result[0].leaderHint() + ")"));
        return result[0];
    }

    /** Proposes to the alive leader with the highest term, if there is one. */
    public ProposeResult proposeToLeader(byte[] command) {
        int leader = currentLeader();
        if (leader < 0) {
            return new ProposeResult(ProposeResult.Status.NOT_LEADER, 0, 0, -1);
        }
        return propose(leader, command);
    }

    public void crash(int id) {
        if (!alive[id]) {
            return;
        }
        applyCrash(id, "crash");
        checker.afterEvent(id);
    }

    /** The node loses power at the moment it next sends a message (right after that message leaves). */
    public void crashOnNextSend(int id) {
        crashOnNextSend(id, -1);
    }

    /**
     * Same, and the node comes back rebootAfterMs later (a QUICK reboot: messages that were already on the wire
     * reach the new incarnation). -1 means it stays dead until somebody restarts it.
     */
    public void crashOnNextSend(int id, long rebootAfterMs) {
        crashOnNextSend[id] = alive[id];
        this.rebootAfterMs[id] = rebootAfterMs;
    }

    /** Cancels every pending "crash on next send" (the healing phase of a scenario). */
    public void clearCrashOnNextSend() {
        java.util.Arrays.fill(crashOnNextSend, false);
        java.util.Arrays.fill(rebootAfterMs, -1);
    }

    private void applyCrash(int id, String why) {
        alive[id] = false;
        crashOnNextSend[id] = false;
        nodes[id] = null;
        storages[id].crash();                          // unsynced writes are gone
        checker.onCrash(id);
        trace("n" + id + " " + why + " -> DEAD (unsynced writes lost)");
        if (rebootAfterMs[id] >= 0) {
            long delay = rebootAfterMs[id];
            rebootAfterMs[id] = -1;
            schedule(delay, () -> restart(id));
        }
    }

    public void restart(int id) {
        if (alive[id]) {
            return;
        }
        trace("n" + id + " restart");
        startNode(id);
        checker.afterEvent(id);
    }

    /** Nodes in the same group can talk; everybody else is cut off from them. Unlisted nodes form one more group. */
    public void partition(int[]... groups) {
        int[] groupOf = new int[n + 1];
        java.util.Arrays.fill(groupOf, groups.length);
        for (int g = 0; g < groups.length; g++) {
            for (int id : groups[g]) {
                groupOf[id] = g;
            }
        }
        for (int a = 1; a <= n; a++) {
            for (int b = 1; b <= n; b++) {
                blocked[a][b] = a != b && groupOf[a] != groupOf[b];
            }
        }
        StringBuilder sb = new StringBuilder("PARTITION");
        for (int[] g : groups) {
            sb.append(' ').append(java.util.Arrays.toString(g));
        }
        trace(sb.toString());
    }

    public void isolate(int id) {
        for (int other = 1; other <= n; other++) {
            if (other != id) {
                blocked[id][other] = true;
                blocked[other][id] = true;
            }
        }
        trace("ISOLATE n" + id);
    }

    public void heal() {
        for (boolean[] row : blocked) {
            java.util.Arrays.fill(row, false);
        }
        trace("HEAL");
    }

    public void setDrop(double p) {
        dropP = p;
    }

    public void setDuplicate(double p) {
        dupP = p;
    }

    /** Chaos knob: the probability that a node loses power right after sending a granted vote (then reboots quickly). */
    public void setRebootAfterVoteProbability(double p) {
        rebootAfterVoteP = p;
    }

    /** Scripted tests: force the one-way latency of a single link (e.g. to engineer a split vote). */
    public void setLinkLatency(int from, int to, long ms) {
        latencyOverride[from][to] = ms;
    }

    public void clearLinkLatencies() {
        for (long[] row : latencyOverride) {
            java.util.Arrays.fill(row, -1);
        }
    }

    public void schedule(long delayMs, Runnable action) {
        queue.add(new Event(now + delayMs, seq++, 0, 0, null, action));
    }

    // ------------------------------------------------------------------ read access

    public long now() {
        return now;
    }

    public int nodeCount() {
        return n;
    }

    public SimConfig config() {
        return config;
    }

    public boolean isAlive(int id) {
        return alive[id];
    }

    public RaftNode node(int id) {
        return nodes[id];
    }

    public Role role(int id) {
        return alive[id] ? nodes[id].role() : null;
    }

    public long term(int id) {
        return storages[id].currentTerm();
    }

    public long commitIndex(int id) {
        return alive[id] ? nodes[id].commitIndex() : -1;
    }

    public long lastLogIndex(int id) {
        return storages[id].lastIndex();
    }

    public InMemoryRaftStorage storage(int id) {
        return storages[id];
    }

    /** The log as the node itself sees it (pending); for a dead node that equals its durable log. */
    public List<LogEntry> log(int id) {
        return storages[id].pendingLog();
    }

    /** What the node applied during its current (or last) lifetime, in order. */
    public List<LogEntry> applied(int id) {
        return List.copyOf(applied.get(id));
    }

    public List<Integer> aliveLeaders() {
        List<Integer> out = new ArrayList<>();
        for (int id = 1; id <= n; id++) {
            if (alive[id] && nodes[id].role() == Role.LEADER) {
                out.add(id);
            }
        }
        return out;
    }

    /** The alive leader with the highest term, or -1. */
    public int currentLeader() {
        int best = -1;
        for (int id : aliveLeaders()) {
            if (best < 0 || nodes[id].currentTerm() > nodes[best].currentTerm()) {
                best = id;
            }
        }
        return best;
    }

    /** term -> the (single) node that became leader in it; every leader of the whole run is recorded. */
    public java.util.Map<Long, Integer> leaderHistory() {
        return java.util.Collections.unmodifiableMap(checker.leaderHistory());
    }

    public Stats stats() {
        return stats;
    }

    public Trace trace() {
        return trace;
    }

    long seed() {
        return config.seed();
    }

    List<Integer> ids() {
        return ids;
    }

    InvariantChecker checker() {
        return checker;
    }

    InvariantViolation violation(String invariant, String detail) {
        return new InvariantViolation(invariant, detail, config.seed(), trace.tail(60));
    }

    // ------------------------------------------------------------------ trace and plumbing

    void trace(String text) {
        trace.add(now, text);
    }

    private final class SimTransport implements RaftTransport {
        private final int from;

        SimTransport(int from) {
            this.from = from;
        }

        @Override
        public void send(int to, RaftMessage message) {
            stats.sent++;
            trace("n" + from + " -> n" + to + " " + describe(message));
            if (dropP > 0 && netRandom.nextDouble() < dropP) {
                stats.dropped++;
                trace("DROP n" + from + " -> n" + to);
            } else {
                scheduleDelivery(from, to, message);
                if (dupP > 0 && netRandom.nextDouble() < dupP) {
                    stats.duplicated++;
                    trace("DUP n" + from + " -> n" + to);
                    scheduleDelivery(from, to, message);
                }
            }
            if (rebootAfterVoteP > 0 && !crashOnNextSend[from] && message instanceof RaftMessage.RequestVoteResponse v
                    && v.voteGranted() && netRandom.nextDouble() < rebootAfterVoteP) {
                // a targeted fault: power loss at the most delicate moment (the vote has just left the node),
                // and a quick reboot, so requests that were already on the wire reach the new incarnation
                crashOnNextSend[from] = true;
                rebootAfterMs[from] = 1 + netRandom.nextInt(30);
            }
            if (crashOnNextSend[from]) {
                crashOnNextSend[from] = false;
                throw new SimCrash();
            }
        }
    }

    private void scheduleDelivery(int from, int to, RaftMessage message) {
        long latency = latencyOverride[from][to];
        if (latency < 0) {
            latency = config.minLatencyMs();
            if (config.maxLatencyMs() > config.minLatencyMs()) {
                latency += netRandom.nextInt((int) (config.maxLatencyMs() - config.minLatencyMs() + 1));
            }
        }
        queue.add(new Event(now + latency, seq++, from, to, message, null));
    }

    private final class SimListener implements RaftListener {
        private final int id;

        SimListener(int id) {
            this.id = id;
        }

        @Override
        public void onRoleChange(int nodeId, Role role, long term, int leaderId) {
            trace("n" + id + " " + role + " term=" + term + (role == Role.FOLLOWER && leaderId >= 0 ? " leader=n" + leaderId : ""));
            roleHistory.get(id).add(role + "@" + term);
            checker.onRoleChange(id, role, term, leaderId);
        }

        @Override
        public void onCommitted(int nodeId, long index, LogEntry entry) {
            applied.get(id).add(entry);
            trace("n" + id + " commit idx=" + index);
            checker.onCommitted(id, index, entry);
        }
    }

    /** A compact one-line description of a message, for the trace. */
    static String describe(RaftMessage m) {
        return switch (m) {
            case RaftMessage.RequestVote v -> "RequestVote(term=" + v.term() + ",lastIdx=" + v.lastLogIndex() + ",lastTerm=" + v.lastLogTerm() + ")";
            case RaftMessage.RequestVoteResponse v -> "RequestVoteResponse(term=" + v.term() + ",granted=" + v.voteGranted() + ")";
            case RaftMessage.AppendEntries a -> "AppendEntries(term=" + a.term() + ",prev=" + a.prevLogIndex() + "/" + a.prevLogTerm()
                    + ",n=" + a.entries().size() + ",commit=" + a.leaderCommit() + ")";
            case RaftMessage.AppendEntriesResponse a -> "AppendEntriesResponse(term=" + a.term() + "," + (a.success() ? "ok match=" + a.matchIndex()
                    : "fail hint=" + a.conflictIndex() + "/" + a.conflictTerm()) + ")";
        };
    }

    /** The roles this node went through, e.g. [CANDIDATE@1, LEADER@1]. */
    public List<String> roleHistoryOf(int id) {
        return List.copyOf(roleHistory.get(id));
    }
}
