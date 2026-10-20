package com.ember.chaos;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Faults for brokers started by scripts/run-cluster.sh: PID files (root/broker-N.pid), kill -9 / -STOP / -CONT, and the script
 * again to restart. Network and disk faults need a root-level tool and are NOT supported (supports() says so).
 */
public final class ProcessFaultDriver implements FaultDriver {

    private final int brokers;
    private final Path root;
    private final Path runScript;
    private final int basePort;
    private final List<String> brokerFlags;

    public ProcessFaultDriver(int brokers, Path root, Path runScript, int basePort, List<String> brokerFlags) {
        this.brokers = brokers;
        this.root = root;
        this.runScript = runScript;
        this.basePort = basePort;
        this.brokerFlags = List.copyOf(brokerFlags);
    }

    @Override
    public int brokers() {
        return brokers;
    }

    @Override
    public boolean supports(Fault fault) {
        return fault == Fault.CRASH_RESTART || fault == Fault.PAUSE;
    }

    private String pid(int node) throws Exception {
        return Files.readString(root.resolve("broker-" + node + ".pid")).trim();
    }

    private static void run(String... command) throws Exception {
        Process p = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        if (!p.waitFor(60, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new IllegalStateException("timed out: " + String.join(" ", command));
        }
    }

    @Override
    public void crash(int node) throws Exception {
        run("kill", "-9", pid(node));
    }

    @Override
    public void restart(int node) throws Exception {
        List<String> command = new ArrayList<>(List.of(runScript.toString(), Integer.toString(brokers), Integer.toString(basePort), root.toString()));
        command.addAll(brokerFlags);
        run(command.toArray(new String[0]));                              // starts only the brokers that are not running
    }

    @Override
    public void pause(int node) throws Exception {
        run("kill", "-STOP", pid(node));
    }

    @Override
    public void resume(int node) throws Exception {
        run("kill", "-CONT", pid(node));
    }

    @Override
    public void isolate(int node) {
        throw new UnsupportedOperationException("isolate needs the docker driver");
    }

    @Override
    public void heal() {
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
        for (int i = 1; i <= brokers; i++) {
            File pidFile = root.resolve("broker-" + i + ".pid").toFile();
            if (pidFile.exists()) {
                try {
                    run("kill", "-CONT", pid(i));
                } catch (Exception ignored) {
                    // not running
                }
            }
        }
        restart(1);
    }
}
