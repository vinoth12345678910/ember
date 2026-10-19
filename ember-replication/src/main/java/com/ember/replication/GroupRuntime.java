package com.ember.replication;

import com.ember.raft.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/**
 * Gives ONE RaftNode (not thread-safe by design) a mailbox, timers and a propose-and-wait helper.
 * Everything that touches the node is a task in the mailbox; at most one worker drains it at a time,
 * so the node only ever sees one thread at a time (asserted at runtime).
 */
public final class GroupRuntime implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(GroupRuntime.class.getName());

    /** The proposal was not accepted, or leadership was lost before the entry was applied. The outcome may be UNKNOWN. */
    public static final class NotLeaderException extends RuntimeException {
        private final int leaderHint;

        public NotLeaderException(int leaderHint) {
            super("not the leader (hint=" + leaderHint + ")", null, false, false);
            this.leaderHint = leaderHint;
        }

        public int leaderHint() {
            return leaderHint;
        }
    }

    public static final class OverloadedException extends RuntimeException {
        public OverloadedException() {
            super("too many proposals are waiting for a commit", null, false, false);
        }
    }

    /** Not applied in time. AMBIGUOUS: the entry may still be committed later. */
    public static final class ProposeTimeoutException extends RuntimeException {
        public ProposeTimeoutException() {
            super("the proposal was not committed in time", null, false, false);
        }
    }

    public static final class GroupFailedException extends RuntimeException {
        public GroupFailedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private record Pending(long term, CompletableFuture<Object> future) {
    }

    private record Applied(long term, Object result) {
    }

    private final String name;
    private final RaftConfig config;
    private final RaftStorage storage;
    private final RaftTransport transport;
    private final StateMachine stateMachine;
    private final Random random;
    private final LongSupplier clockMs;
    private final ScheduledExecutorService scheduler;
    private final Executor workers;
    private final int maxPending;

    private final ConcurrentLinkedQueue<Runnable> mailbox = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean scheduled = new AtomicBoolean(false);
    private volatile Thread owner;
    private volatile boolean closed;
    private volatile boolean failed;
    private volatile boolean leaderReady;        // leader AND has applied an entry of its own term (its no-op)

    // touched only by the mailbox thread
    private RaftNode node;
    private final Map<Long, Pending> pending = new HashMap<>();
    private Map<Long, Applied> appliedDuringPropose;
    private long armedAt = Long.MIN_VALUE;
    private ScheduledFuture<?> armedTimer;

    // read by anybody
    private volatile Role role = Role.FOLLOWER;
    private volatile int leaderId = -1;
    private volatile long term;
    private volatile long commitIndex;
    private volatile long lastApplied;
    private volatile long lastLogIndex;
    private final List<RaftListener> listeners = new CopyOnWriteArrayList<>();

    public GroupRuntime(String name, RaftConfig config, RaftStorage storage, RaftTransport transport, StateMachine stateMachine,
                        Random random, LongSupplier clockMs, ScheduledExecutorService scheduler, Executor workers, int maxPending) {
        this.name = name;
        this.config = config;
        this.storage = storage;
        this.transport = transport;
        this.stateMachine = stateMachine;
        this.random = random;
        this.clockMs = clockMs;
        this.scheduler = scheduler;
        this.workers = workers;
        this.maxPending = maxPending;
    }

    public static long monotonicMillis() {
        return System.nanoTime() / 1_000_000;
    }

    // ------------------------------------------------------------------ the mailbox

    private void execute(Runnable task) {
        if (closed) {
            return;
        }
        mailbox.add(task);
        kick();
    }

    private void kick() {
        if (scheduled.compareAndSet(false, true)) {
            try {
                workers.execute(this::drain);
            } catch (RuntimeException rejected) {              // the pool is shutting down
                scheduled.set(false);
            }
        }
    }

    private void drain() {
        owner = Thread.currentThread();
        try {
            int budget = 512;                                  // be fair to other groups sharing this worker
            Runnable task;
            while (budget-- > 0 && (task = mailbox.poll()) != null) {
                runSafely(task);
            }
            afterBatch();
        } finally {
            owner = null;
            scheduled.set(false);
            if (!mailbox.isEmpty()) {
                kick();
            }
        }
    }

    private void runSafely(Runnable task) {
        if (failed) {
            return;                                            // a failed group does nothing more: applying more could diverge replicas
        }
        try {
            task.run();
        } catch (Throwable t) {
            LOG.log(System.Logger.Level.ERROR, "group " + name + " failed; it stops processing", t);
            failed = true;
            failAllPending(new GroupFailedException("group " + name + " failed", t));
        }
    }

    private void assertOnGroupThread() {
        if (Thread.currentThread() != owner) {
            throw new IllegalStateException("RaftNode of group " + name + " touched outside its serial context");
        }
    }

    /** Refresh the read-only snapshot and arm the next timer. Runs at the end of every batch. */
    private void afterBatch() {
        if (node == null || closed) {
            return;
        }
        role = node.role();
        leaderId = node.leaderId();
        term = node.currentTerm();
        commitIndex = node.commitIndex();
        lastApplied = node.lastApplied();
        lastLogIndex = node.lastLogIndex();
        long next = node.nextTimerMs();
        if (next != armedAt) {
            if (armedTimer != null) {
                armedTimer.cancel(false);
            }
            armedAt = next;
            long delay = Math.max(0, next - clockMs.getAsLong());
            try {
                armedTimer = scheduler.schedule(() -> execute(this::tick), delay, TimeUnit.MILLISECONDS);
            } catch (RuntimeException rejected) {
                // scheduler shut down
            }
        }
    }

    private void tick() {
        assertOnGroupThread();
        armedAt = Long.MIN_VALUE;                              // always re-arm: the scheduler may wake us a millisecond early
        node.tick(clockMs.getAsLong());
    }

    // ------------------------------------------------------------------ public API

    /** Creates the node (inside the mailbox) and starts its timers. */
    public void start() {
        execute(() -> {
            assertOnGroupThread();
            node = new RaftNode(config, storage, transport, new Listener(), random, clockMs.getAsLong());
        });
    }

    /** Hands a Raft message from a peer to the node. Never blocks. */
    public void deliver(int fromNodeId, RaftMessage message) {
        execute(() -> {
            if (node != null) {
                assertOnGroupThread();
                node.receive(fromNodeId, message, clockMs.getAsLong());
            }
        });
    }

    /**
     * Proposes a command and completes when it is APPLIED here with the same term (result = what the state
     * machine returned). Fails with NotLeaderException / OverloadedException / ProposeTimeoutException.
     */
    public CompletableFuture<Object> propose(byte[] command, long timeoutMs) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        if (closed || failed) {
            future.completeExceptionally(new NotLeaderException(-1));
            return future;
        }
        execute(() -> {
            assertOnGroupThread();
            if (node == null || node.role() != Role.LEADER) {
                future.completeExceptionally(new NotLeaderException(node == null ? -1 : node.leaderId()));
                return;
            }
            if (pending.size() >= maxPending) {
                future.completeExceptionally(new OverloadedException());
                return;
            }
            appliedDuringPropose = new HashMap<>();
            ProposeResult result;
            Map<Long, Applied> appliedNow;
            try {
                result = node.propose(command, clockMs.getAsLong());
            } finally {
                appliedNow = appliedDuringPropose;             // a single-node cluster commits and applies INSIDE propose()
                appliedDuringPropose = null;
            }
            if (!result.accepted()) {
                future.completeExceptionally(new NotLeaderException(result.leaderHint()));
                return;
            }
            Applied already = appliedNow.get(result.index());
            if (already != null) {
                complete(future, result.term(), already);
                return;
            }
            pending.put(result.index(), new Pending(result.term(), future));
            if (timeoutMs > 0) {
                long index = result.index();
                try {
                    scheduler.schedule(() -> execute(() -> {
                        Pending p = pending.get(index);
                        if (p != null && p.future() == future) {
                            pending.remove(index);
                            future.completeExceptionally(new ProposeTimeoutException());
                        }
                    }), timeoutMs, TimeUnit.MILLISECONDS);
                } catch (RuntimeException rejected) {
                    // shutting down
                }
            }
        });
        return future;
    }

    public void addListener(RaftListener listener) {
        listeners.add(listener);
    }

    public String name() {
        return name;
    }

    public Role role() {
        return role;
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }

    /**
     * Leader that has applied an entry of its own term, i.e. everything committed before it. Only such a leader may
     * serve reads: its data log is up to date with the committed log.
     */
    public boolean isLeaderReady() {
        return leaderReady && role == Role.LEADER;
    }

    public int leaderId() {
        return leaderId;
    }

    public long term() {
        return term;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long lastApplied() {
        return lastApplied;
    }

    public long lastLogIndex() {
        return lastLogIndex;
    }

    public boolean hasFailed() {
        return failed;
    }

    /** Runs a task in the group's serial context and waits for it (tests and diagnostics). */
    public void runAndWait(Runnable task) throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);
        execute(() -> {
            try {
                task.run();
            } finally {
                done.countDown();
            }
        });
        done.await(10, TimeUnit.SECONDS);
    }

    // ------------------------------------------------------------------ apply and completion

    private static void complete(CompletableFuture<Object> future, long expectedTerm, Applied applied) {
        if (applied.term() == expectedTerm) {
            future.complete(applied.result());
        } else {
            future.completeExceptionally(new NotLeaderException(-1));   // our entry was overwritten by a newer leader
        }
    }

    private void failAllPending(RuntimeException reason) {
        for (Pending p : pending.values()) {
            p.future().completeExceptionally(reason);
        }
        pending.clear();
    }

    private final class Listener implements RaftListener {
        @Override
        public void onRoleChange(int nodeId, Role newRole, long newTerm, int newLeader) {
            assertOnGroupThread();
            leaderReady = false;                                    // becomes true again when we apply an entry of our own term
            if (newRole != Role.LEADER) {
                failAllPending(new NotLeaderException(newLeader));  // may or may not end up committed by the new leader
            }
            for (RaftListener l : listeners) {
                l.onRoleChange(nodeId, newRole, newTerm, newLeader);
            }
        }

        @Override
        public void onCommitted(int nodeId, long index, LogEntry entry) {
            assertOnGroupThread();
            Object result = stateMachine.apply(index, entry);      // an exception here fails the whole group (see runSafely)
            Applied applied = new Applied(entry.term(), result);
            if (node.role() == Role.LEADER && entry.term() == node.currentTerm()) {
                leaderReady = true;                                 // everything the previous leaders committed is applied here
            }
            if (appliedDuringPropose != null) {
                appliedDuringPropose.put(index, applied);
            }
            Pending p = pending.remove(index);
            if (p != null) {
                complete(p.future(), p.term(), applied);
            }
            for (RaftListener l : listeners) {
                l.onCommitted(nodeId, index, entry);
            }
        }
    }

    // ------------------------------------------------------------------ shutdown

    /** Graceful: finishes queued work, fails pending futures, stops timers, closes the storage. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        CountDownLatch done = new CountDownLatch(1);
        mailbox.add(() -> {
            failAllPending(new NotLeaderException(-1));
            done.countDown();
        });
        kick();
        try {
            done.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        shutDown();
    }

    /** Abrupt (test hook for a crash): drops queued work, no goodbye. */
    public void abort() {
        shutDown();
    }

    private void shutDown() {
        closed = true;
        ScheduledFuture<?> timer = armedTimer;
        if (timer != null) {
            timer.cancel(false);
        }
        mailbox.clear();
        for (int i = 0; i < 1000 && scheduled.get(); i++) {       // let a running batch finish before the files are closed
            java.util.concurrent.locks.LockSupport.parkNanos(1_000_000);
        }
        for (Pending p : new ArrayList<>(pending.values())) {
            p.future().completeExceptionally(new NotLeaderException(-1));
        }
        pending.clear();
        if (storage instanceof AutoCloseable c) {
            try {
                c.close();
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING, "closing storage of " + name, e);
            }
        }
    }
}
