package com.ember.chaos;

/** Applies faults to an InProcessCluster through the test hooks of the brokers. Supports every fault type. */
public final class InProcessDriver implements FaultDriver {

    private final InProcessCluster cluster;

    public InProcessDriver(InProcessCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public int brokers() {
        return cluster.n;
    }

    @Override
    public boolean supports(Fault fault) {
        return fault != Fault.CLIENT_PAUSE;
    }

    @Override
    public void crash(int node) {
        cluster.crash(node);
    }

    @Override
    public void restart(int node) throws Exception {
        cluster.restart(node);
    }

    @Override
    public void pause(int node) {
        cluster.faults(node).pause();
    }

    @Override
    public void resume(int node) {
        cluster.faults(node).resume();
    }

    @Override
    public void isolate(int node) {
        if (cluster.isUp(node)) cluster.server(node).isolate();
    }

    @Override
    public void heal() {
        for (int i = 1; i <= cluster.n; i++) {
            if (cluster.isUp(i)) cluster.server(i).heal();
        }
    }

    @Override
    public void blockLink(int a, int b) {
        if (cluster.isUp(a)) cluster.server(a).blockPeer(b);
        if (cluster.isUp(b)) cluster.server(b).blockPeer(a);
    }

    @Override
    public void unblockLink(int a, int b) {
        if (cluster.isUp(a)) cluster.server(a).unblockPeer(b);
        if (cluster.isUp(b)) cluster.server(b).unblockPeer(a);
    }

    @Override
    public void slowFsync(int node, long millis) {
        cluster.faults(node).slowFsync(millis);
    }

    @Override
    public void dropMessages(int node, double probability, long seed) {
        cluster.faults(node).dropMessages(probability, seed);
    }

    @Override
    public void delayMessages(int node, long millis) {
        cluster.faults(node).delayMessages(millis);
    }

    @Override
    public void clearSoftFaults(int node) {
        cluster.faults(node).clear();
    }

    @Override
    public void healAll() throws Exception {
        for (int i = 1; i <= cluster.n; i++) {
            cluster.faults(i).clear();
            if (!cluster.isUp(i)) cluster.restart(i);
        }
        heal();
        for (int i = 1; i <= cluster.n; i++) {
            for (int j = i + 1; j <= cluster.n; j++) unblockLink(i, j);
        }
    }
}
