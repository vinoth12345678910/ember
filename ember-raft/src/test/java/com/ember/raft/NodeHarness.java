package com.ember.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * A tiny manual harness for node-level tests: a recording transport, a storage that records its
 * operations, a listener that records callbacks, and a clock the test moves by hand.
 * Every send checks that the node had already synced its writes (the outbox rule).
 */
final class NodeHarness {

    record Sent(int to, RaftMessage message) {
    }

    /** Delegates to a real InMemoryRaftStorage and logs every operation. */
    static final class RecordingStorage implements RaftStorage {
        final InMemoryRaftStorage inner;
        final List<String> ops;

        RecordingStorage(InMemoryRaftStorage inner, List<String> ops) {
            this.inner = inner;
            this.ops = ops;
        }

        public long currentTerm() { return inner.currentTerm(); }
        public int votedFor() { return inner.votedFor(); }
        public void setTermAndVote(long term, int votedFor) { ops.add("setTermAndVote(" + term + "," + votedFor + ")"); inner.setTermAndVote(term, votedFor); }
        public long lastIndex() { return inner.lastIndex(); }
        public long lastTerm() { return inner.lastTerm(); }
        public long termAt(long index) { return inner.termAt(index); }
        public LogEntry entryAt(long index) { return inner.entryAt(index); }
        public List<LogEntry> entries(long fromIndex, int maxCount) { return inner.entries(fromIndex, maxCount); }
        public void append(List<LogEntry> entries) { ops.add("append(" + entries.size() + ")"); inner.append(entries); }
        public void truncateFrom(long index) { ops.add("truncateFrom(" + index + ")"); inner.truncateFrom(index); }
        public void sync() { ops.add("sync"); inner.sync(); }
    }

    final List<String> ops = new ArrayList<>();
    final InMemoryRaftStorage disk;
    final RecordingStorage storage;
    final List<Sent> sent = new ArrayList<>();
    final List<String> unsyncedSends = new ArrayList<>();
    final List<String> roleChanges = new ArrayList<>();
    final List<Long> committedIndexes = new ArrayList<>();
    final List<LogEntry> committedEntries = new ArrayList<>();
    final RaftConfig config;
    final long seed;
    RaftNode node;
    long now;

    NodeHarness(RaftConfig config) {
        this(config, new InMemoryRaftStorage(), 1);
    }

    NodeHarness(RaftConfig config, InMemoryRaftStorage disk, long seed) {
        this.config = config;
        this.disk = disk;
        this.storage = new RecordingStorage(disk, ops);
        this.seed = seed;
        this.now = 0;
        start();
    }

    /** n-node cluster with ids 1..n, this node is `self`. */
    static NodeHarness cluster(int self, int n) {
        List<Integer> ids = new ArrayList<>();
        for (int i = 1; i <= n; i++) ids.add(i);
        return new NodeHarness(RaftConfig.of(self, ids));
    }

    static NodeHarness cluster(int self, int n, InMemoryRaftStorage disk) {
        List<Integer> ids = new ArrayList<>();
        for (int i = 1; i <= n; i++) ids.add(i);
        return new NodeHarness(RaftConfig.of(self, ids), disk, 1);
    }

    /** (Re)creates the RaftNode on the same storage: this is what a restart is. */
    void start() {
        node = new RaftNode(config, storage, this::record, new RaftListener() {
            @Override
            public void onRoleChange(int nodeId, Role role, long term, int leaderId) {
                roleChanges.add(role + "@" + term + "/leader=" + leaderId);
            }

            @Override
            public void onCommitted(int nodeId, long index, LogEntry entry) {
                committedIndexes.add(index);
                committedEntries.add(entry);
            }
        }, new Random(seed), now);
    }

    void restart() {
        disk.crash();
        sent.clear();
        committedIndexes.clear();
        committedEntries.clear();
        start();
    }

    private void record(int to, RaftMessage message) {
        if (disk.hasUnsyncedWrites()) {
            unsyncedSends.add("sent " + message + " to " + to + " while storage had unsynced writes");
        }
        ops.add("send(" + to + "," + message.getClass().getSimpleName() + ")");
        sent.add(new Sent(to, message));
    }

    // ---- driving the node

    void advanceTo(long t) {
        now = t;
        node.tick(now);
    }

    /** Moves the clock exactly to the node's next timer and ticks. */
    void fireNextTimer() {
        advanceTo(node.nextTimerMs());
    }

    void receive(int from, RaftMessage m) {
        node.receive(from, m, now);
    }

    void receiveAt(long t, int from, RaftMessage m) {
        now = t;
        node.receive(from, m, now);
    }

    // ---- inspecting what happened

    List<Sent> drain() {
        List<Sent> copy = new ArrayList<>(sent);
        sent.clear();
        return copy;
    }

    @SuppressWarnings("unchecked")
    <T extends RaftMessage> List<T> sentOfType(Class<T> type) {
        List<T> out = new ArrayList<>();
        for (Sent s : sent) {
            if (type.isInstance(s.message())) out.add((T) s.message());
        }
        return out;
    }

    <T extends RaftMessage> T lastSentOfType(Class<T> type) {
        List<T> all = sentOfType(type);
        return all.isEmpty() ? null : all.get(all.size() - 1);
    }

    List<Sent> sentTo(int peer) {
        List<Sent> out = new ArrayList<>();
        for (Sent s : sent) {
            if (s.to() == peer) out.add(s);
        }
        return out;
    }

    static LogEntry cmd(long term, String s) {
        return LogEntry.command(term, s.getBytes());
    }
}
