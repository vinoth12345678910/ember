package com.ember.chaos;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Faults for the docker compose cluster: docker kill / start / pause / unpause, and docker network disconnect / connect to
 * isolate a broker. Container names are prefix + node (for example ember1). Link, disk and message faults are NOT supported.
 */
public final class DockerFaultDriver implements FaultDriver {

    private final int brokers;
    private final String prefix;
    private final String network;
    private final boolean[] isolated;

    public DockerFaultDriver(int brokers, String containerPrefix, String network) {
        this.brokers = brokers;
        this.prefix = containerPrefix;
        this.network = network;
        this.isolated = new boolean[brokers + 1];
    }

    @Override
    public int brokers() {
        return brokers;
    }

    @Override
    public boolean supports(Fault fault) {
        return fault == Fault.CRASH_RESTART || fault == Fault.PAUSE || fault == Fault.ISOLATE;
    }

    private static void docker(String... args) throws Exception {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.addAll(List.of(args));
        Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        if (!p.waitFor(60, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new IllegalStateException("timed out: " + String.join(" ", command));
        }
        if (p.exitValue() != 0) {
            throw new IllegalStateException(String.join(" ", command) + " failed: " + out.trim());
        }
    }

    private String name(int node) {
        return prefix + node;
    }

    @Override
    public void crash(int node) throws Exception {
        docker("kill", name(node));
    }

    @Override
    public void restart(int node) throws Exception {
        docker("start", name(node));
    }

    @Override
    public void pause(int node) throws Exception {
        docker("pause", name(node));
    }

    @Override
    public void resume(int node) throws Exception {
        docker("unpause", name(node));
    }

    @Override
    public void isolate(int node) throws Exception {
        docker("network", "disconnect", network, name(node));
        isolated[node] = true;
    }

    @Override
    public void heal() throws Exception {
        for (int i = 1; i <= brokers; i++) {
            if (isolated[i]) {
                try {
                    docker("network", "connect", "--alias", name(i), network, name(i));
                } finally {
                    isolated[i] = false;
                }
            }
        }
    }

    @Override
    public void blockLink(int a, int b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unblockLink(int a, int b) {
    }

    @Override
    public void slowFsync(int node, long millis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropMessages(int node, double probability, long seed) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delayMessages(int node, long millis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearSoftFaults(int node) {
    }

    @Override
    public void healAll() throws Exception {
        heal();
        for (int i = 1; i <= brokers; i++) {
            try {
                docker("unpause", name(i));
            } catch (Exception ignored) {
                // was not paused
            }
            try {
                docker("start", name(i));
            } catch (Exception ignored) {
                // already running
            }
        }
    }
}
