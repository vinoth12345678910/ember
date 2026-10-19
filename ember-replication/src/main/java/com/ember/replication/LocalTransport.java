package com.ember.replication;

import com.ember.raft.RaftMessage;
import com.ember.raft.RaftTransport;

import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An in-process "network" between GroupRuntimes, for tests of this module (and anyone who wants a cluster in one JVM):
 * links can be blocked, messages dropped or delayed. Delivery never blocks the sender.
 */
public final class LocalTransport {

    private final Map<String, GroupRuntime> groups = new ConcurrentHashMap<>();
    private final Set<Long> blockedLinks = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService scheduler;
    private final Random random;
    private volatile double dropProbability;
    private volatile long minDelayMs;
    private volatile long maxDelayMs;

    public LocalTransport(ScheduledExecutorService scheduler, long seed) {
        this.scheduler = scheduler;
        this.random = new Random(seed);
    }

    private static String key(int node, String topic, int partition) {
        return node + "|" + topic + "|" + partition;
    }

    private static long link(int from, int to) {
        return ((long) from << 32) | (to & 0xffffffffL);
    }

    public void register(int nodeId, String topic, int partition, GroupRuntime runtime) {
        groups.put(key(nodeId, topic, partition), runtime);
    }

    public void unregister(int nodeId, String topic, int partition) {
        groups.remove(key(nodeId, topic, partition));
    }

    /** The RaftTransport one group of one node uses to talk to its peers. */
    public RaftTransport forGroup(int fromNode, String topic, int partition) {
        return (to, message) -> send(fromNode, to, topic, partition, message);
    }

    private void send(int from, int to, String topic, int partition, RaftMessage message) {
        if (blockedLinks.contains(link(from, to))) {
            return;
        }
        if (dropProbability > 0) {
            boolean drop;
            synchronized (random) {
                drop = random.nextDouble() < dropProbability;
            }
            if (drop) {
                return;
            }
        }
        long delay = 0;
        if (maxDelayMs > 0) {
            synchronized (random) {
                delay = minDelayMs + (long) (random.nextDouble() * (maxDelayMs - minDelayMs + 1));
            }
        }
        Runnable deliver = () -> {
            GroupRuntime target = groups.get(key(to, topic, partition));
            if (target != null && !blockedLinks.contains(link(from, to))) {
                target.deliver(from, message);
            }
        };
        if (delay > 0) {
            try {
                scheduler.schedule(deliver, delay, TimeUnit.MILLISECONDS);
            } catch (RuntimeException shuttingDown) {
                // dropped: the network went away
            }
        } else {
            deliver.run();
        }
    }

    /** Blocks both directions between the two nodes. */
    public void block(int a, int b) {
        blockedLinks.add(link(a, b));
        blockedLinks.add(link(b, a));
    }

    public void unblock(int a, int b) {
        blockedLinks.remove(link(a, b));
        blockedLinks.remove(link(b, a));
    }

    /** Cuts one node off from all the others. */
    public void isolate(int node, Iterable<Integer> allNodes) {
        for (int other : allNodes) {
            if (other != node) {
                block(node, other);
            }
        }
    }

    public void heal() {
        blockedLinks.clear();
    }

    public void setDropProbability(double p) {
        this.dropProbability = p;
    }

    public void setDelay(long minMs, long maxMs) {
        this.minDelayMs = minMs;
        this.maxDelayMs = maxMs;
    }
}
