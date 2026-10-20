package com.ember.chaos;

import com.ember.broker.BrokerConfig;
import com.ember.broker.BrokerServer;
import com.ember.replication.ConfigurableFaults;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;

/**
 * N real brokers in one JVM on ephemeral ports (bind all, then start all). A crashed or stopped broker comes back on the
 * same data directory and the same port. Every broker has a ConfigurableFaults injector (nothing injected until asked).
 */
public final class InProcessCluster implements AutoCloseable {

    public final Path base;
    public final int n;
    final BrokerServer[] servers;
    final ConfigurableFaults[] faults;
    final int[] ports;
    final int[] adminPorts;
    private final String[] extraFlags;
    private final Map<Integer, InetSocketAddress> addresses = new TreeMap<>();

    public InProcessCluster(Path base, int n, boolean fsync, String... extra) throws Exception {
        this.base = base;
        this.n = n;
        List<String> flags = new ArrayList<>(List.of(extra));
        if (!fsync) flags.add("--unsafe-no-raft-fsync");
        this.extraFlags = flags.toArray(new String[0]);
        this.servers = new BrokerServer[n + 1];
        this.faults = new ConfigurableFaults[n + 1];
        this.ports = new int[n + 1];
        this.adminPorts = new int[n + 1];
        for (int i = 1; i <= n; i++) {
            try (java.net.ServerSocket probe = new java.net.ServerSocket(0)) {      // a free port, kept across restarts so admin addresses stay valid
                adminPorts[i] = probe.getLocalPort();
            }
        }
        for (int i = 1; i <= n; i++) {
            faults[i] = new ConfigurableFaults();
            servers[i] = new BrokerServer(config(i), Clock.systemUTC(), faults[i]);
            ports[i] = servers[i].bind();
            addresses.put(i, new InetSocketAddress("localhost", ports[i]));
        }
        for (int i = 1; i <= n; i++) servers[i].start(addresses);
    }

    BrokerConfig config(int i) {
        List<String> flags = new ArrayList<>(List.of(
                "--node-id=" + i, "--port=" + ports[i], "--data-dir=" + base.resolve("broker-" + i),
                "--segment-bytes=65536", "--index-interval-bytes=1024", "--maintenance-interval-ms=3600000",
                "--raft-election-min-ms=150", "--raft-election-max-ms=300", "--raft-heartbeat-ms=30",
                "--handler-threads=16", "--produce-timeout-ms=4000", "--data-flush=never", "--group-min-session-ms=100",
                "--admin-port=" + adminPorts[i]));
        flags.addAll(List.of(extraFlags));
        return BrokerConfig.parse(flags.toArray(new String[0]));
    }

    public boolean isUp(int i) {
        return servers[i] != null;
    }

    public synchronized void crash(int i) {
        if (servers[i] != null) {
            servers[i].crash();
            servers[i] = null;
        }
    }

    public synchronized void stop(int i) {
        if (servers[i] != null) {
            servers[i].close();
            servers[i] = null;
        }
    }

    public synchronized void restart(int i) throws Exception {
        if (servers[i] != null) return;
        faults[i].clear();
        BrokerServer s = new BrokerServer(config(i), Clock.systemUTC(), faults[i]);
        int port = s.bind();
        if (port != ports[i]) {
            throw new IllegalStateException("broker " + i + " came back on port " + port + " instead of " + ports[i]);
        }
        s.start(addresses);
        servers[i] = s;
    }

    public synchronized void stopAll() {
        for (int i = 1; i <= n; i++) stop(i);
    }

    public BrokerServer server(int i) {
        return servers[i];
    }

    public ConfigurableFaults faults(int i) {
        return faults[i];
    }

    public List<String> bootstrap() {
        List<String> out = new ArrayList<>();
        for (int i = 1; i <= n; i++) out.add("localhost:" + ports[i]);
        return out;
    }

    public String adminAddress(int i) {
        return "localhost:" + adminPorts[i];
    }

    public Path dataDir(int i) {
        return base.resolve("broker-" + i);
    }

    public static boolean await(long timeoutMs, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return true;
            java.util.concurrent.locks.LockSupport.parkNanos(20_000_000L);
        }
        return condition.getAsBoolean();
    }

    /** Waits until every UP broker answers /health/ready with 200. */
    public boolean awaitReady(long timeoutMs) {
        return await(timeoutMs, () -> {
            for (int i = 1; i <= n; i++) {
                if (servers[i] != null && !AdminClient.ready(adminAddress(i))) return false;
            }
            return true;
        });
    }

    @Override
    public void close() {
        for (int i = 1; i <= n; i++) {
            if (servers[i] != null) servers[i].close();
            servers[i] = null;
        }
    }
}
