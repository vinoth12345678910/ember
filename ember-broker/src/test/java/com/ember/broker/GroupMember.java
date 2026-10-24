package com.ember.broker;

import com.ember.client.*;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** A group consumer running in its own thread, recording everything it received. */
final class GroupMember implements AutoCloseable {

    final String name;
    final EmberGroupConsumer consumer;
    final List<GroupRecord> received = new CopyOnWriteArrayList<>();
    /** What was received, with the join count of the consumer at that moment (a rejoin legitimately repeats records). */
    final List<Delivery> deliveries = new CopyOnWriteArrayList<>();

    record Delivery(String topic, int partition, long offset, String value, int joinCount) {
    }

    final AtomicInteger errors = new AtomicInteger();
    volatile Throwable lastError;
    private final Thread thread;
    private volatile boolean stop;

    GroupMember(TestCluster c, String group, String name, int sessionMs, int heartbeatMs, boolean autoCommit, int commitMs, String... topics) {
        this(c, group, name, sessionMs, heartbeatMs, autoCommit, commitMs, false, topics);
    }

    GroupMember(TestCluster c, String group, String name, int sessionMs, int heartbeatMs, boolean autoCommit, int commitMs, boolean incremental, String... topics) {
        this.name = name;
        GroupConsumerConfig config = EmberGroupConsumerTest.groupConfig(c, group)
                .withClientId(name).withSession(sessionMs, heartbeatMs).withAutoCommit(autoCommit, commitMs).withIncrementalRejoin(incremental);
        this.consumer = new EmberGroupConsumer(config);
        consumer.subscribe(topics);
        this.thread = new Thread(() -> {
            while (!stop) {
                try {
                    List<GroupRecord> batch = consumer.poll(Duration.ofMillis(100));
                    int joins = consumer.joinCount();             // only this thread joins, so it is the join that produced this batch
                    for (GroupRecord r : batch) deliveries.add(new Delivery(r.topic(), r.partition(), r.offset(), new String(r.value()), joins));
                    received.addAll(batch);
                } catch (CommitFailedException e) {
                    // fenced: the next poll rejoins
                } catch (Exception e) {
                    errors.incrementAndGet();                   // cluster down / leader moving: try again
                    lastError = e;
                    java.util.concurrent.locks.LockSupport.parkNanos(50_000_000L);
                }
            }
        }, "group-member-" + name);
        thread.setDaemon(true);
        thread.start();
    }

    Set<String> values() {
        Set<String> out = new TreeSet<>();
        for (GroupRecord r : received) out.add(new String(r.value()));
        return out;
    }

    private void stopThread() throws InterruptedException {
        stop = true;
        thread.join(15_000);
    }

    /** Graceful: final commit, leave the group. */
    @Override
    public void close() throws Exception {
        stopThread();
        consumer.close();
    }

    /** Dies like kill -9: no commit, no leave. The coordinator only notices when the session runs out. */
    void crash() throws InterruptedException {
        stopThread();
        consumer.crashForTesting();
    }
}
