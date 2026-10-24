package com.ember.client;

import com.ember.protocol.ErrorCode;
import com.ember.protocol.PartitionOffset;
import com.ember.protocol.PartitionRef;
import com.ember.storage.LogRecord;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A consumer that belongs to a GROUP: the coordinator divides the partitions of the subscribed topics among the live
 * members, and each member resumes from the group's last COMMITTED offset. Delivery is AT-LEAST-ONCE: process the
 * records, THEN commit. After a rebalance or a crash the new owner repeats whatever was not yet committed.
 *
 * Threads: poll(), commitSync() and close() belong to ONE user thread. A background heartbeat thread, with its OWN
 * connections, keeps the session alive while the user thread is busy processing. The price (documented in
 * docs/phase6/design.md): a stuck user thread would keep the membership alive for ever, so after maxPollIntervalMs
 * without a poll() the consumer leaves the group.
 */
public final class EmberGroupConsumer implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(EmberGroupConsumer.class.getName());
    private static final int MAX_JOIN_ATTEMPTS = 8;

    private final GroupConsumerConfig config;
    private final ClusterClient client;             // user thread: join, offsets, commit, fetch
    private final ClusterClient heartbeatClient;    // heartbeat thread only: its own connections
    private final Object lock = new Object();       // guards the group view below (also read by the heartbeat thread)

    /** The one place a poll talks to partition leaders; replaced in tests to make a fetch fail at an exact moment. */
    @FunctionalInterface
    interface Fetcher {
        FetchResult fetch(String topic, int partition, long offset, int maxBytes) throws IOException;
    }

    private Fetcher fetcher;

    private List<String> topics = List.of();
    private String memberId = "";
    private int generation = -1;
    private Set<PartitionRef> assignment = new TreeSet<>();
    private final Map<PartitionRef, Long> positions = new HashMap<>();
    private final Map<PartitionRef, Long> committed = new HashMap<>();
    private int rotation;

    private final AtomicBoolean rebalanceNeeded = new AtomicBoolean(true);    // true = (re)join before consuming
    private final AtomicBoolean heartbeatsPaused = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private volatile long lastPollMs = System.currentTimeMillis();
    private long lastAutoCommitMs = System.currentTimeMillis();
    private final Thread heartbeatThread;
    private boolean closed;
    private volatile int joins;                  // how many times this consumer completed a join (diagnostics, tests)

    public EmberGroupConsumer(GroupConsumerConfig config) {
        this.config = config;
        this.client = new ClusterClient(config.client());
        this.fetcher = client::fetch;
        this.heartbeatClient = new ClusterClient(config.client().withRetries(2, 50, 500));
        this.heartbeatThread = new Thread(this::heartbeatLoop, "ember-heartbeat-" + config.groupId() + "-" + config.clientId());
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    public void subscribe(String... topics) {
        if (topics.length == 0) {
            throw new IllegalArgumentException("subscribe to at least one topic");
        }
        synchronized (lock) {
            this.topics = List.of(topics);
            rebalanceNeeded.set(true);
        }
    }

    // ------------------------------------------------------------------ poll

    /** Returns records from the owned partitions, or an empty list if nothing arrived within the timeout. */
    public List<GroupRecord> poll(Duration timeout) throws IOException {
        long deadline = System.nanoTime() + timeout.toNanos();
        ensureOpen();
        lastPollMs = System.currentTimeMillis();
        while (true) {
            if (rebalanceNeeded.get()) {
                joinAndStart();
            }
            maybeAutoCommit();
            List<GroupRecord> out = fetchOnce();
            if (!out.isEmpty() || System.nanoTime() >= deadline) {
                return out;
            }
            parkMillis(Math.min(50, Math.max(1, (deadline - System.nanoTime()) / 1_000_000)));
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("consumer is closed");
        }
        synchronized (lock) {
            if (topics.isEmpty()) {
                throw new IllegalStateException("subscribe() first");
            }
        }
    }

    private void joinAndStart() throws IOException {
        boolean incremental = config.incrementalRejoin();
        if (!incremental) {
            synchronized (lock) {
                assignment = new TreeSet<>();             // stop owning anything until the new assignment is known
                positions.clear();
                committed.clear();
            }
        }
        for (int attempt = 0; attempt < MAX_JOIN_ATTEMPTS; attempt++) {
            String id;
            List<String> subscribed;
            synchronized (lock) {
                id = memberId;
                subscribed = topics;
            }
            // cleared BEFORE the join: a rebalance signal that arrives while joining must survive
            rebalanceNeeded.set(false);
            try {
                var joined = client.joinGroup(config.groupId(), id, config.clientId(), config.sessionTimeoutMs(), subscribed);
                if (incremental && !id.isEmpty() && id.equals(joined.memberId())) {
                    applyIncrementally(joined.generation(), joined.assignment());
                } else {
                    synchronized (lock) {
                        memberId = joined.memberId();
                        generation = joined.generation();
                        assignment = new TreeSet<>(joined.assignment());
                        positions.clear();
                        committed.clear();
                    }
                    initializePositions(joined.assignment());
                }
                joins++;
                lastAutoCommitMs = System.currentTimeMillis();
                return;
            } catch (EmberException e) {
                switch (e.errorCode()) {
                    case UNKNOWN_MEMBER_ID -> {
                        synchronized (lock) {
                            memberId = "";                // we were expired: start again as a new member
                            assignment = new TreeSet<>();
                            positions.clear();
                            committed.clear();
                        }
                    }
                    case REBALANCE_IN_PROGRESS -> {
                        // the group kept changing under our join; just try again
                    }
                    default -> {
                        rebalanceNeeded.set(true);
                        throw e;
                    }
                }
            } catch (IOException e) {
                rebalanceNeeded.set(true);
                throw e;
            }
        }
        rebalanceNeeded.set(true);
        throw new EmberException(ErrorCode.REBALANCE_IN_PROGRESS);
    }

    /**
     * The cooperative way to take a new assignment: partitions we keep are never touched (same position, no re-read of the
     * committed offset, nothing refetched); partitions taken away get their last position committed (the coordinator allows
     * that while they are unowned), and only the new ones read their start position from the group. The generation is adopted
     * LAST: the heartbeat thread then acknowledges it, which tells the coordinator that the revoked partitions are safe to hand over.
     */
    private void applyIncrementally(int newGeneration, List<PartitionRef> newAssignment) throws IOException {
        Set<PartitionRef> fresh = new TreeSet<>(newAssignment);
        List<PartitionOffset> lastWords = new ArrayList<>();
        List<PartitionRef> added = new ArrayList<>();
        String id;
        synchronized (lock) {
            id = memberId;
            for (PartitionRef p : assignment) {
                Long position = positions.get(p);
                if (!fresh.contains(p) && position != null && !position.equals(committed.get(p))) {
                    lastWords.add(new PartitionOffset(p.topic(), p.partition(), position));
                }
            }
            for (PartitionRef p : fresh) {
                if (!assignment.contains(p)) {
                    added.add(p);
                }
            }
        }
        if (!lastWords.isEmpty()) {
            try {
                client.offsetCommit(config.groupId(), id, newGeneration, lastWords);
            } catch (EmberException e) {
                LOG.log(System.Logger.Level.INFO, "last-words commit refused ({0}): the new owner repeats a little work", e.errorCode());
            }
        }
        synchronized (lock) {
            assignment.removeIf(p -> !fresh.contains(p));
            positions.keySet().removeIf(p -> !fresh.contains(p));
            committed.keySet().removeIf(p -> !fresh.contains(p));
            assignment.addAll(added);
        }
        initializePositions(added);
        synchronized (lock) {
            generation = newGeneration;                   // adopted last: the next heartbeat acknowledges it
        }
    }

    /** Committed offset if it is still inside the log, else the autoOffsetReset policy. */
    private void initializePositions(Collection<PartitionRef> partitions) throws IOException {
        List<PartitionRef> owned = new ArrayList<>(partitions);
        if (owned.isEmpty()) {
            return;
        }
        Map<PartitionRef, Long> fromGroup = new HashMap<>();
        for (PartitionOffset o : client.offsetFetch(config.groupId(), owned)) {
            fromGroup.put(new PartitionRef(o.topic(), o.partition()), o.offset());
        }
        Map<String, List<PartitionView>> views = new HashMap<>();
        for (PartitionRef p : owned) {
            if (!views.containsKey(p.topic())) {
                views.put(p.topic(), client.describe(p.topic()));          // null if the topic vanished
            }
        }
        for (PartitionRef p : owned) {
            long start = 0;
            long end = 0;
            List<PartitionView> topicView = views.get(p.topic());
            if (topicView != null && p.partition() < topicView.size()) {
                start = topicView.get(p.partition()).logStartOffset();
                end = topicView.get(p.partition()).logEndOffset();
            }
            long saved = fromGroup.getOrDefault(p, -1L);
            long position = saved >= 0 && saved >= start && saved <= end ? saved : resetPosition(p, start, end);
            synchronized (lock) {
                positions.put(p, position);
                if (saved >= 0 && position == saved) {
                    committed.put(p, saved);              // already in the group: no need to commit it again
                }
            }
        }
    }

    private long resetPosition(PartitionRef p, long logStart, long logEnd) {
        return switch (config.autoOffsetReset()) {
            case EARLIEST -> logStart;
            case LATEST -> logEnd;
            case NONE -> throw new EmberException(ErrorCode.OFFSET_OUT_OF_RANGE);
        };
    }

    private List<GroupRecord> fetchOnce() throws IOException {
        List<PartitionRef> owned;
        synchronized (lock) {
            owned = new ArrayList<>(assignment);
        }
        List<GroupRecord> out = new ArrayList<>();
        if (owned.isEmpty()) {
            return out;
        }
        int start = rotation++ % owned.size();
        int budget = config.maxPollBytes();
        for (int i = 0; i < owned.size() && budget > 0 && !rebalanceNeeded.get(); i++) {
            PartitionRef p = owned.get((start + i) % owned.size());
            long position;
            synchronized (lock) {
                Long v = positions.get(p);
                if (v == null) {
                    continue;
                }
                position = v;
            }
            FetchResult result;
            try {
                result = fetcher.fetch(p.topic(), p.partition(), position, budget);
            } catch (EmberException e) {
                if (e.errorCode() != ErrorCode.OFFSET_OUT_OF_RANGE) {
                    if (out.isEmpty()) {
                        throw e;
                    }
                    break;                                // return what earlier partitions gave; this one is retried on the next poll
                }
                try {
                    resetAfterOutOfRange(p);              // retention removed the data: apply the policy
                } catch (IOException ioe) {
                    if (out.isEmpty()) {
                        throw ioe;
                    }
                    break;
                }
                continue;
            } catch (IOException e) {
                if (out.isEmpty()) {
                    throw e;
                }
                // Positions of the partitions already read have moved. Throwing now would DROP those records and skip them for
                // ever, so hand them to the application; the failing partition is simply tried again on the next poll.
                break;
            }
            for (LogRecord r : result.records()) {
                out.add(new GroupRecord(p.topic(), p.partition(), r.offset(), r.key(), r.value(), r.timestamp()));
                budget -= r.value().length + (r.key() == null ? 0 : r.key().length) + 32;
            }
            synchronized (lock) {
                positions.put(p, result.nextFetchOffset());
            }
        }
        return out;
    }

    private void resetAfterOutOfRange(PartitionRef p) throws IOException {
        List<PartitionView> view = client.describe(p.topic());
        if (view == null || p.partition() >= view.size()) {
            throw new EmberException(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
        }
        long position = resetPosition(p, view.get(p.partition()).logStartOffset(), view.get(p.partition()).logEndOffset());
        synchronized (lock) {
            positions.put(p, position);
        }
    }

    // ------------------------------------------------------------------ commit

    private void maybeAutoCommit() {
        if (!config.autoCommit() || System.currentTimeMillis() - lastAutoCommitMs < config.autoCommitIntervalMs()) {
            return;
        }
        lastAutoCommitMs = System.currentTimeMillis();
        try {
            commitSync();
        } catch (CommitFailedException e) {
            LOG.log(System.Logger.Level.INFO, "auto-commit refused ({0}): rejoining", e.errorCode());
        } catch (IOException | RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "auto-commit failed, will retry: {0}", e.toString());
        }
    }

    /**
     * Commits the current position (the next offset to read) of every owned partition that moved since the last
     * commit. Throws CommitFailedException when this consumer is no longer the owner.
     *
     * @return true if a commit was SENT and acknowledged; false if there was nothing to commit (no position moved, or the
     *         consumer is between two generations), in which case the coordinator was not contacted at all
     */
    public boolean commitSync() throws IOException {
        String id;
        int gen;
        List<PartitionOffset> offsets = new ArrayList<>();
        synchronized (lock) {
            id = memberId;
            gen = generation;
            if (id.isEmpty() || rebalanceNeeded.get()) {
                return false;                             // nothing is owned right now
            }
            for (PartitionRef p : assignment) {
                Long position = positions.get(p);
                if (position != null && !position.equals(committed.get(p))) {
                    offsets.add(new PartitionOffset(p.topic(), p.partition(), position));
                }
            }
        }
        if (offsets.isEmpty()) {
            return false;                                 // no position moved since the last commit: nothing was sent
        }
        try {
            client.offsetCommit(config.groupId(), id, gen, offsets);
        } catch (EmberException e) {
            switch (e.errorCode()) {
                case ILLEGAL_GENERATION, UNKNOWN_MEMBER_ID, REBALANCE_IN_PROGRESS, PARTITION_NOT_ASSIGNED -> {
                    rebalanceNeeded.set(true);
                    throw new CommitFailedException(e.errorCode());
                }
                default -> throw e;
            }
        }
        synchronized (lock) {
            if (id.equals(memberId) && gen == generation) {
                for (PartitionOffset o : offsets) {
                    committed.put(new PartitionRef(o.topic(), o.partition()), o.offset());
                }
            }
        }
        return true;
    }

    // ------------------------------------------------------------------ heartbeat thread

    private void heartbeatLoop() {
        while (!stopped.get()) {
            parkMillis(config.heartbeatIntervalMs());
            if (stopped.get()) {
                return;
            }
            try {
                if (!heartbeatsPaused.get()) {
                    beatOnce();
                }
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "heartbeat loop: {0}", e.toString());
            }
        }
    }

    private void beatOnce() {
        String id;
        int gen;
        synchronized (lock) {
            id = memberId;
            gen = generation;
        }
        if (id.isEmpty() || rebalanceNeeded.get()) {
            return;
        }
        if (System.currentTimeMillis() - lastPollMs > config.maxPollIntervalMs()) {
            leaveBecauseStuck(id);
            return;
        }
        try {
            heartbeatClient.heartbeat(config.groupId(), id, gen);
        } catch (EmberException e) {
            switch (e.errorCode()) {
                case REBALANCE_IN_PROGRESS, UNKNOWN_MEMBER_ID, ILLEGAL_GENERATION -> rebalanceNeeded.set(true);   // poll() will rejoin
                default -> LOG.log(System.Logger.Level.WARNING, "heartbeat rejected: {0}", e.errorCode());
            }
        } catch (IOException e) {
            LOG.log(System.Logger.Level.INFO, "heartbeat could not reach the coordinator: {0}", e.toString());
        }
    }

    private void leaveBecauseStuck(String id) {
        LOG.log(System.Logger.Level.WARNING, "no poll() for {0} ms: leaving group {1}", config.maxPollIntervalMs(), config.groupId());
        try {
            heartbeatClient.leaveGroup(config.groupId(), id);
        } catch (IOException | RuntimeException ignored) {
            // the session will expire anyway
        }
        synchronized (lock) {
            memberId = "";
            assignment = new TreeSet<>();
            positions.clear();
            committed.clear();
        }
        rebalanceNeeded.set(true);
    }

    // ------------------------------------------------------------------ views

    public long position(String topic, int partition) {
        synchronized (lock) {
            Long p = positions.get(new PartitionRef(topic, partition));
            if (p == null) {
                throw new IllegalStateException("not assigned: " + topic + "-" + partition);
            }
            return p;
        }
    }

    /**
     * Moves the position of an owned partition, for example back to the first record the application did NOT process
     * after it stopped in the middle of a batch. Only what the position says is ever committed, so this is how
     * "process, then commit" stays true when you stop early.
     */
    public void seek(String topic, int partition, long offset) {
        PartitionRef p = new PartitionRef(topic, partition);
        synchronized (lock) {
            if (!assignment.contains(p)) {
                throw new IllegalStateException("not assigned: " + topic + "-" + partition);
            }
            positions.put(p, offset);
        }
    }

    public Set<PartitionRef> assignment() {
        synchronized (lock) {
            return new TreeSet<>(assignment);
        }
    }

    public int generation() {
        synchronized (lock) {
            return generation;
        }
    }

    /** How many times this consumer has (re)joined: a record is only ever redelivered to the same consumer after a rejoin. */
    public int joinCount() {
        return joins;
    }

    public String memberId() {
        synchronized (lock) {
            return memberId;
        }
    }

    // ------------------------------------------------------------------ test hooks (named so nobody uses them by accident)

    /** Unit tests only: pretend the join gave these partitions at these positions, and fetch through the given function. */
    void adoptForTesting(Map<PartitionRef, Long> startPositions, Fetcher testFetcher) {
        synchronized (lock) {
            memberId = "test-member";
            generation = 1;
            assignment = new TreeSet<>(startPositions.keySet());
            positions.clear();
            positions.putAll(startPositions);
            this.fetcher = testFetcher;
        }
        rebalanceNeeded.set(false);
    }

    /** Stops or resumes the heartbeats WITHOUT leaving: simulates a paused or partitioned consumer (a zombie in the making). */
    public void pauseHeartbeatsForTesting(boolean paused) {
        heartbeatsPaused.set(paused);
    }

    /** Dies like a killed process: no commit, no leave, threads and connections gone. The session just runs out. */
    public void crashForTesting() {
        stopped.set(true);
        closed = true;
        heartbeatThread.interrupt();
        closeQuietly(client);
        closeQuietly(heartbeatClient);
    }

    // ------------------------------------------------------------------ close

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        if (config.autoCommit()) {
            try {
                commitSync();                             // the last chance to save what the user has processed
            } catch (IOException | RuntimeException e) {
                LOG.log(System.Logger.Level.INFO, "final commit skipped: {0}", e.toString());
            }
        }
        closed = true;
        String id;
        synchronized (lock) {
            id = memberId;
        }
        if (!id.isEmpty()) {
            try {
                client.leaveGroup(config.groupId(), id);
            } catch (IOException | RuntimeException e) {
                LOG.log(System.Logger.Level.INFO, "leave skipped: {0}", e.toString());
            }
        }
        stopped.set(true);
        heartbeatThread.interrupt();
        try {
            heartbeatThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        closeQuietly(client);
        closeQuietly(heartbeatClient);
    }

    private static void closeQuietly(ClusterClient c) {
        try {
            c.close();
        } catch (IOException ignored) {
            // nothing more to do
        }
    }

    private static void parkMillis(long ms) {
        java.util.concurrent.locks.LockSupport.parkNanos(ms * 1_000_000L);
    }
}
