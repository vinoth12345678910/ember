package com.ember.broker;

import java.time.Clock;

public final class EmberBroker {

    private EmberBroker() {
    }

    public static void main(String[] args) throws Exception {
        BrokerConfig config;
        try {
            config = BrokerConfig.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println("usage: java -jar ember-broker-all.jar [--port=9099] [--data-dir=./ember-data] "
                    + "[--flush-ms=1000|never] [--flush-messages=N|never] [--segment-bytes=N] ... (see README)");
            System.exit(2);
            return;
        }
        BrokerServer server = new BrokerServer(config, Clock.systemUTC());
        int port;
        try {
            port = server.start();
        } catch (IllegalStateException | IllegalArgumentException e) {     // e.g. the data directory is locked, or a legacy directory
            System.err.println("error: " + e.getMessage());
            System.exit(1);
            return;
        }
        // Runs on Ctrl+C and SIGTERM: stop accepting, finish in-flight work, fsync every log.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            System.out.println("Ember broker shut down cleanly");
        }, "ember-shutdown"));
        System.out.println("Ember broker started: node=" + config.nodeId() + " port=" + port
                + " dataDir=" + config.dataDir().toAbsolutePath()
                + (config.singleBroker() ? " (single broker)" : " cluster=" + config.cluster().stream().map(m -> m.nodeId() + "@" + m.host() + ":" + m.port()).toList()));
    }
}
