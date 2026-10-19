package com.ember.broker;

import java.io.IOException;
import java.nio.file.Path;

/** Starts a real broker on port 0 with small segments, so tests exercise rolling. */
final class ServerFixture {

    private ServerFixture() {
    }

    static BrokerConfig config(Path dir, String... extra) {
        String[] flags = new String[extra.length + 13];
        flags[0] = "--port=0";
        flags[1] = "--data-dir=" + dir;
        flags[2] = "--segment-bytes=4096";
        flags[3] = "--index-interval-bytes=256";
        flags[4] = "--maintenance-interval-ms=3600000";      // tests run maintenance by hand
        flags[5] = "--flush-ms=never";
        flags[6] = "--default-partitions=1";                 // Phase 2 tests assume one partition per topic; extra flags override
        // Phase 5: every broker is a Raft node. Fast timings and no Raft fsync keep the old integration tests quick.
        flags[7] = "--raft-election-min-ms=50";
        flags[8] = "--raft-election-max-ms=100";
        flags[9] = "--raft-heartbeat-ms=20";
        flags[10] = "--unsafe-no-raft-fsync";
        flags[11] = "--produce-timeout-ms=5000";
        flags[12] = "--group-min-session-ms=100";            // Phase 6: tests use short consumer-group sessions
        System.arraycopy(extra, 0, flags, 13, extra.length);
        return BrokerConfig.parse(flags);
    }

    static BrokerServer start(BrokerConfig config, java.time.Clock clock) throws IOException, InterruptedException {
        BrokerServer server = new BrokerServer(config, clock);
        server.start();
        return server;
    }
}
