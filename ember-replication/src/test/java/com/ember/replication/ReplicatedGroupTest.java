package com.ember.replication;

import com.ember.storage.LogRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Three replicas of one partition: real threads, real files, short timings. */
class ReplicatedGroupTest {

    @Test
    void threeReplicasElectALeader(@TempDir Path dir) {
        try (ReplicationCluster c = ReplicationCluster.started(dir, 3)) {
            int leader = c.awaitLeader();
            Await.until(5_000, "everyone to know the leader", () -> {
                for (int i = 1; i <= 3; i++) if (c.replicas[i].runtime.leaderId() != leader) return false;
                return true;
            });
            long term = c.replicas[leader].runtime.term();
            assertTrue(term >= 1);
            for (int i = 1; i <= 3; i++) assertEquals(term, c.replicas[i].runtime.term());
        }
    }

    @Test
    void aProposalIsCommittedAndAppliedOnEveryReplicaWithTheSameOffsets(@TempDir Path dir) throws Exception {
        try (ReplicationCluster c = ReplicationCluster.started(dir, 3)) {
            int leader = c.awaitLeader();
            assertEquals(0, c.produce(leader, 1000, "a", "b", "c"));
            assertEquals(3, c.produce(leader, 2000, "d"));
            assertEquals(4, c.produce(leader, 3000, "e", "f"));
            c.awaitDataLogEnd(6);
            List<LogRecord> expected = c.records(leader);
            assertEquals(6, expected.size());
            for (int i = 1; i <= 3; i++) {
                assertEquals(c.summary(leader), c.summary(i), "replica " + i);
            }
            assertEquals(1000, expected.get(0).timestamp());
            assertEquals(2000, expected.get(3).timestamp());
            assertEquals("f", new String(expected.get(5).value()));
        }
    }

    @Test
    void proposingOnAFollowerFailsWithNotLeaderAndAHint(@TempDir Path dir) throws Exception {
        try (ReplicationCluster c = ReplicationCluster.started(dir, 3)) {
            int leader = c.awaitLeader();
            int follower = leader == 1 ? 2 : 1;
            Await.until(5_000, "the follower to learn the leader", () -> c.replicas[follower].runtime.leaderId() == leader);
            ExecutionException e = assertThrows(ExecutionException.class, () -> c.produce(follower, 1, "x"));
            assertInstanceOf(GroupRuntime.NotLeaderException.class, e.getCause());
            assertEquals(leader, ((GroupRuntime.NotLeaderException) e.getCause()).leaderHint());
        }
    }

    @Test
    void allReplicasDataLogsAreByteIdenticalAfterCatchUp(@TempDir Path dir) throws Exception {
        try (ReplicationCluster c = ReplicationCluster.started(dir, 3)) {
            int leader = c.awaitLeader();
            for (int i = 0; i < 50; i++) {
                c.produce(leader, 1000 + i, "v" + i, "w" + i);
            }
            c.awaitDataLogEnd(100);
            byte[] reference = c.dataLogBytes(1);
            assertTrue(reference.length > 1000);
            assertArrayEquals(reference, c.dataLogBytes(2));
            assertArrayEquals(reference, c.dataLogBytes(3));
        }
    }

    @Test
    void aRestartedFollowerReplaysItsRaftLogAndCatchesUp(@TempDir Path dir) throws Exception {
        try (ReplicationCluster c = ReplicationCluster.started(dir, 3)) {
            int leader = c.awaitLeader();
            int follower = leader == 1 ? 2 : 1;
            c.produce(leader, 1, "a", "b");
            c.awaitDataLogEnd(2);
            c.stop(follower);
            c.produce(leader, 2, "c", "d", "e");                          // the follower misses these
            c.start(follower);
            c.awaitDataLogEnd(5);
            assertEquals(c.summary(leader), c.summary(follower));
            assertEquals(2, c.replicas[follower].stateMachine.runningOffset() - 3, "it applied all 5 records exactly once");
        }
    }

    @Test
    void aWholeClusterRestartKeepsEverythingAndKeepsWorking(@TempDir Path dir) throws Exception {
        try (ReplicationCluster c = ReplicationCluster.started(dir, 3)) {
            int leader = c.awaitLeader();
            c.produce(leader, 1, "a", "b", "c");
            c.awaitDataLogEnd(3);
            for (int i = 1; i <= 3; i++) c.stop(i);
            for (int i = 1; i <= 3; i++) c.start(i);
            int again = c.awaitLeader();
            assertEquals(3, c.produce(again, 2, "d"));                    // offsets continue
            c.awaitDataLogEnd(4);
            assertEquals(c.summary(1), c.summary(2));
            assertEquals(c.summary(2), c.summary(3));
        }
    }

    // ---------------------------------------------------------------- failures

    @Test
    void anIsolatedLeaderStepsDownAndItsPendingProposalsFailWithNotLeader(@TempDir Path dir) throws Exception {
        try (ReplicationCluster c = ReplicationCluster.started(dir, 3)) {
            int leader = c.awaitLeader();
            c.produce(leader, 1, "before");
            c.awaitDataLogEnd(1);
            c.transport.isolate(leader, c.ids);
            CompletableFuture<Object> f = c.replicas[leader].runtime.propose(
                    Commands.produceBatch(2, List.of(new com.ember.protocol.ProducerRecord(null, "lost".getBytes()))), 30_000);
            ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(15, java.util.concurrent.TimeUnit.SECONDS));
            assertInstanceOf(GroupRuntime.NotLeaderException.class, e.getCause(), "CheckQuorum makes the stranded leader give up");
        }
    }

    @Test
    void afterTheLeaderIsKilledANewOneServesAndNoAcknowledgedProposalIsLost(@TempDir Path dir) throws Exception {
        try (ReplicationCluster c = ReplicationCluster.started(dir, 3)) {
            int leader = c.awaitLeader();
            List<String> acked = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                c.produce(leader, i, "a" + i);
                acked.add("a" + i);
            }
            c.crash(leader);
            Await.until(15_000, "a new leader", () -> c.leader() > 0 && c.leader() != leader);
            int next = c.leader();
            for (int i = 0; i < 20; i++) {
                c.produce(next, 100 + i, "b" + i);
                acked.add("b" + i);
            }
            c.awaitDataLogEnd(40);
            List<String> values = c.records(next).stream().map(r -> new String(r.value())).toList();
            assertEquals(acked, values, "every acknowledged record, in the order of acknowledgement");
            c.start(leader);                                              // the old leader comes back and catches up
            c.awaitDataLogEnd(40);
            assertEquals(c.summary(next), c.summary(leader));
        }
    }

    @Test
    void theOldLeadersUnacknowledgedProposalIsGoneAfterHealing(@TempDir Path dir) throws Exception {
        try (ReplicationCluster c = new ReplicationCluster(dir, 3, false, false, 1000, 3)) {          // no CheckQuorum here: the stranded leader keeps believing
            for (int i = 1; i <= 3; i++) c.start(i);
            int old = c.awaitLeader();
            c.produce(old, 1, "acked");
            c.awaitDataLogEnd(1);
            c.transport.isolate(old, c.ids);
            CompletableFuture<Object> stranded = c.replicas[old].runtime.propose(
                    Commands.produceBatch(2, List.of(new com.ember.protocol.ProducerRecord(null, "unacked".getBytes()))), 0);
            Await.until(15_000, "a new leader among the other two", () -> c.leader() > 0 && c.leader() != old);
            int next = c.leader();
            c.produce(next, 3, "after");
            c.transport.heal();
            Await.until(15_000, "the old leader to be overwritten", () -> stranded.isDone());
            assertTrue(stranded.isCompletedExceptionally());
            c.awaitDataLogEnd(2);
            List<String> values = c.records(old).stream().map(r -> new String(r.value())).toList();
            assertEquals(List.of("acked", "after"), values);
        }
    }

    @Test
    void thePendingLimitGivesOverloadedImmediately(@TempDir Path dir) throws Exception {
        try (ReplicationCluster c = new ReplicationCluster(dir, 3, false, false, 2, 4)) {
            for (int i = 1; i <= 3; i++) c.start(i);
            int leader = c.awaitLeader();
            c.transport.isolate(leader, c.ids);                           // nothing can commit now
            List<CompletableFuture<Object>> futures = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                futures.add(c.replicas[leader].runtime.propose(
                        Commands.produceBatch(i, List.of(new com.ember.protocol.ProducerRecord(null, ("x" + i).getBytes()))), 0));
            }
            Await.until(5_000, "the 3 extra proposals to be rejected", () -> futures.stream().filter(CompletableFuture::isDone).count() == 3);
            for (int i = 2; i < 5; i++) {
                ExecutionException e = assertThrows(ExecutionException.class, () -> futures.get(4).get());
                assertInstanceOf(GroupRuntime.OverloadedException.class, e.getCause());
            }
            assertFalse(futures.get(0).isDone());
            assertFalse(futures.get(1).isDone());
        }
    }

    @Test
    void withoutAMajorityAProposalTimesOutAndOnlyThatProposalFails(@TempDir Path dir) throws Exception {
        try (ReplicationCluster c = new ReplicationCluster(dir, 3, false, false, 1000, 5)) {
            for (int i = 1; i <= 3; i++) c.start(i);
            int leader = c.awaitLeader();
            c.transport.isolate(leader, c.ids);
            CompletableFuture<Object> f = c.replicas[leader].runtime.propose(
                    Commands.produceBatch(1, List.of(new com.ember.protocol.ProducerRecord(null, "x".getBytes()))), 300);
            ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(10, java.util.concurrent.TimeUnit.SECONDS));
            assertInstanceOf(GroupRuntime.ProposeTimeoutException.class, e.getCause());
            assertTrue(c.replicas[leader].runtime.isLeader(), "a timeout does not depose the leader");
        }
    }

    @Test
    void theMailboxNeverRunsTwoTasksOfAGroupAtTheSameTime(@TempDir Path dir) throws Exception {
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger maxInside = new AtomicInteger();
        try (ReplicationCluster c = ReplicationCluster.started(dir, 3)) {
            int leader = c.awaitLeader();
            c.replicas[leader].runtime.addListener(new com.ember.raft.RaftListener() {
                public void onRoleChange(int nodeId, com.ember.raft.Role role, long term, int leaderId) { }
                public void onCommitted(int nodeId, long index, com.ember.raft.LogEntry entry) {
                    int now = inside.incrementAndGet();
                    maxInside.accumulateAndGet(now, Math::max);
                    Thread.yield();
                    inside.decrementAndGet();
                }
            });
            Thread[] threads = new Thread[6];
            for (int t = 0; t < threads.length; t++) {
                final int id = t;
                threads[t] = new Thread(() -> {
                    try {
                        for (int i = 0; i < 40; i++) c.produce(leader, i, "t" + id + "-" + i);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                threads[t].start();
            }
            for (Thread t : threads) t.join(60_000);
            c.awaitDataLogEnd(240);
            assertEquals(1, maxInside.get(), "applies of one group must be serialized");
        }
    }

    @Test
    void messagesToAGroupThatWasStoppedAreDroppedQuietly(@TempDir Path dir) throws Exception {
        try (ReplicationCluster c = ReplicationCluster.started(dir, 3)) {
            int leader = c.awaitLeader();
            int follower = leader == 1 ? 2 : 1;
            c.crash(follower);
            c.produce(leader, 1, "still works with 2 of 3");
            c.awaitDataLogEnd(1);
        }
    }

    @Test
    void aDroppyDelayedNetworkStillConvergesToIdenticalLogs(@TempDir Path dir) throws Exception {
        try (ReplicationCluster c = ReplicationCluster.started(dir, 3)) {
            int leader = c.awaitLeader();
            c.transport.setDropProbability(0.2);
            c.transport.setDelay(0, 15);
            int acked = 0;
            for (int i = 0; i < 30; i++) {
                try {
                    int l = c.awaitLeader();
                    c.produce(l, i, "m" + i);
                    acked++;
                } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
                    // a failed attempt is allowed under 20% loss
                }
            }
            c.transport.setDropProbability(0);
            c.transport.setDelay(0, 0);
            final int ackedFinal = acked;
            assertTrue(ackedFinal > 5);
            Await.until(20_000, "all replicas equal", () -> {
                try {
                    return c.summary(1).equals(c.summary(2)) && c.summary(2).equals(c.summary(3)) && c.summary(1).size() >= ackedFinal;
                } catch (Exception e) {
                    return false;
                }
            });
            assertEquals(leader > 0, true);
        }
    }
}
