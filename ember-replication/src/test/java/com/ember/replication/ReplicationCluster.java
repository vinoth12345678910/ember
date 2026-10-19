package com.ember.replication;

import com.ember.protocol.ProducerRecord;
import com.ember.raft.RaftConfig;
import com.ember.storage.FlushPolicy;
import com.ember.storage.Log;
import com.ember.storage.LogRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** N replicas of ONE Raft group ("t", 0) in one JVM: real threads, real files, short Raft timings. */
final class ReplicationCluster implements AutoCloseable {

    static final String TOPIC = "t";

    static final class Replica {
        final int id;
        Log dataLog;
        PartitionStateMachine stateMachine;
        GroupRuntime runtime;
        boolean up;

        Replica(int id) {
            this.id = id;
        }
    }

    final Path base;
    final int n;
    final List<Integer> ids = new ArrayList<>();
    final boolean fsync;
    final boolean checkQuorum;
    final int maxPending;
    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, r -> daemon(r, "test-scheduler"));
    final ExecutorService workers = Executors.newFixedThreadPool(4, r -> daemon(r, "test-worker"));
    final LocalTransport transport;
    final Replica[] replicas;

    ReplicationCluster(Path base, int n, boolean fsync, boolean checkQuorum, int maxPending, long seed) {
        this.base = base;
        this.n = n;
        this.fsync = fsync;
        this.checkQuorum = checkQuorum;
        this.maxPending = maxPending;
        this.transport = new LocalTransport(scheduler, seed);
        this.replicas = new Replica[n + 1];
        for (int i = 1; i <= n; i++) {
            ids.add(i);
            replicas[i] = new Replica(i);
        }
    }

    static ReplicationCluster started(Path base, int n) {
        ReplicationCluster c = new ReplicationCluster(base, n, false, true, 1000, 1);
        for (int i = 1; i <= n; i++) c.start(i);
        return c;
    }

    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    Path dataDir(int id) {
        return base.resolve("node-" + id + "/" + TOPIC + "-0");
    }

    Path raftDir(int id) {
        return base.resolve("node-" + id + "/__raft/" + TOPIC + "-0");
    }

    RaftConfig config(int id) {
        return RaftConfig.of(id, ids).withElectionTimeout(100, 200).withHeartbeat(20).withCheckQuorum(checkQuorum);
    }

    void start(int id) {
        Replica r = replicas[id];
        if (r.up) {
            return;
        }
        try {
            r.dataLog = new Log(dataDir(id), 1024 * 1024, 4096, FlushPolicy.never());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        r.stateMachine = new PartitionStateMachine(TOPIC + "-0@" + id, r.dataLog);
        FileRaftStorage storage = new FileRaftStorage(raftDir(id), fsync);
        r.runtime = new GroupRuntime(TOPIC + "-0@" + id, config(id), storage, transport.forGroup(id, TOPIC, 0), r.stateMachine,
                new Random(id * 7919L), GroupRuntime::monotonicMillis, scheduler, workers, maxPending);
        transport.register(id, TOPIC, 0, r.runtime);
        r.runtime.start();
        r.up = true;
    }

    /** Graceful shutdown. */
    void stop(int id) {
        Replica r = replicas[id];
        if (!r.up) return;
        transport.unregister(id, TOPIC, 0);
        r.runtime.close();
        closeLog(r);
        r.up = false;
    }

    /** Abrupt: no goodbye, queued work dropped. */
    void crash(int id) {
        Replica r = replicas[id];
        if (!r.up) return;
        transport.unregister(id, TOPIC, 0);
        r.runtime.abort();
        closeLog(r);
        r.up = false;
    }

    private void closeLog(Replica r) {
        try {
            r.dataLog.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The up replica that believes it is leader, with the highest term (or -1). */
    int leader() {
        int best = -1;
        for (int i = 1; i <= n; i++) {
            Replica r = replicas[i];
            if (r.up && r.runtime.isLeader() && (best < 0 || r.runtime.term() > replicas[best].runtime.term())) {
                best = i;
            }
        }
        return best;
    }

    int awaitLeader() {
        Await.until(15_000, "a leader to be elected", () -> leader() > 0);
        return leader();
    }

    /** Proposes on a specific replica and waits for the result (the base offset). */
    long produce(int id, long timestamp, String... values) throws Exception {
        List<ProducerRecord> records = new ArrayList<>();
        for (String v : values) records.add(new ProducerRecord(null, v.getBytes()));
        return (Long) replicas[id].runtime.propose(Commands.produceBatch(timestamp, records), 10_000).get(15, TimeUnit.SECONDS);
    }

    void awaitDataLogEnd(long expectedEnd) {
        for (int i = 1; i <= n; i++) {
            Replica r = replicas[i];
            if (r.up) {
                Await.until(15_000, "replica " + i + " to hold " + expectedEnd + " records (has " + r.dataLog.nextOffset() + ")",
                        () -> r.dataLog.nextOffset() == expectedEnd);
            }
        }
    }

    /** Every record (offset, timestamp, key, value) of a replica's data log. */
    List<LogRecord> records(int id) throws IOException {
        List<LogRecord> out = new ArrayList<>();
        Log log = replicas[id].dataLog;
        for (long o = log.startOffset(); o < log.nextOffset(); o++) {
            out.add(log.read(o));
        }
        return out;
    }

    /** offset:timestamp:key:value of every record: LogRecord holds byte[] so equals() would compare array identities. */
    List<String> summary(int id) throws IOException {
        List<String> out = new ArrayList<>();
        for (LogRecord r : records(id)) {
            out.add(r.offset() + ":" + r.timestamp() + ":" + (r.key() == null ? "null" : new String(r.key())) + ":" + new String(r.value()));
        }
        return out;
    }

    /** The data log segment files of a replica, concatenated: byte-for-byte comparable between replicas. */
    byte[] dataLogBytes(int id) throws IOException {
        replicas[id].dataLog.flush();
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        try (var files = Files.list(dataDir(id))) {
            for (Path p : files.filter(f -> f.toString().endsWith(".log")).sorted().toList()) {
                out.write(Files.readAllBytes(p));
            }
        }
        return out.toByteArray();
    }

    @Override
    public void close() {
        for (int i = 1; i <= n; i++) {
            if (replicas[i].up) {
                try {
                    stop(i);
                } catch (RuntimeException ignored) {
                    // best effort in tests
                }
            }
        }
        scheduler.shutdownNow();
        workers.shutdownNow();
    }
}
