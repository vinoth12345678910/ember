package com.ember.broker;

import com.ember.storage.FlushPolicy;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BrokerClusterConfigTest {

    @Test
    void defaultsAreASingleBrokerWithTheDocumentedRaftSettings() {
        BrokerConfig c = BrokerConfig.defaults();
        assertTrue(c.singleBroker());
        assertEquals(300, c.raftElectionMinMs());
        assertEquals(600, c.raftElectionMaxMs());
        assertEquals(100, c.raftHeartbeatMs());
        assertEquals(5000, c.produceTimeoutMs());
        assertEquals(1000, c.maxPendingProduces());
        assertEquals(10_000, c.peerQueueSize());
        assertFalse(c.unsafeNoRaftFsync(), "the unsafe switch is OFF unless asked for");
        assertEquals(FlushPolicy.never(), c.dataFlush(), "the Raft log is the durable truth; the data log needs no fsync");
    }

    @Test
    void theClusterFlagListsEveryBrokerAndTheListenPortComesFromTheSelfEntry() {
        BrokerConfig c = BrokerConfig.parse("--node-id=2", "--cluster=3@h3:9003,1@h1:9001,2@h2:9002");
        assertFalse(c.singleBroker());
        assertEquals(List.of(new BrokerConfig.ClusterMember(1, "h1", 9001), new BrokerConfig.ClusterMember(2, "h2", 9002),
                new BrokerConfig.ClusterMember(3, "h3", 9003)), c.cluster());
        assertEquals(9002, c.port());
        assertEquals("h2", c.host());
        assertEquals(2, c.nodeId());
    }

    @Test
    void thePortFlagMustAgreeWithTheClusterEntry() {
        assertDoesNotThrow(() -> BrokerConfig.parse("--node-id=1", "--port=9001", "--cluster=1@h:9001,2@h:9002"));
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse("--node-id=1", "--port=9999", "--cluster=1@h:9001,2@h:9002"));
    }

    @Test
    void theClusterMustContainThisBrokerAndBeWellFormed() {
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse("--node-id=7", "--cluster=1@h:9001,2@h:9002"));
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse("--node-id=1", "--cluster=1@h9001"));
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse("--node-id=1", "--cluster=1@h:9001,1@g:9002"));
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse("--node-id=1", "--cluster=x@h:9001"));
    }

    @Test
    void raftTimingsAreValidated() {
        BrokerConfig c = BrokerConfig.parse("--raft-election-min-ms=50", "--raft-election-max-ms=80", "--raft-heartbeat-ms=10");
        assertEquals(50, c.raftElectionMinMs());
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse("--raft-heartbeat-ms=400"));
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse("--raft-election-min-ms=500", "--raft-election-max-ms=400"));
    }

    @Test
    void theBareUnsafeSwitchAndTheOtherNewFlags() {
        BrokerConfig c = BrokerConfig.parse("--unsafe-no-raft-fsync", "--produce-timeout-ms=1234", "--max-pending-produces=7", "--peer-queue-size=99");
        assertTrue(c.unsafeNoRaftFsync());
        assertEquals(1234, c.produceTimeoutMs());
        assertEquals(7, c.maxPendingProduces());
        assertEquals(99, c.peerQueueSize());
    }

    @Test
    void theDataFlushFlagAndTheOldFlagsSteerTheDataLogs() {
        assertEquals(FlushPolicy.everyMessage(), BrokerConfig.parse("--data-flush=message").dataFlush());
        assertEquals(FlushPolicy.afterMillis(250), BrokerConfig.parse("--data-flush=250ms").dataFlush());
        assertEquals(FlushPolicy.never(), BrokerConfig.parse("--data-flush=never").dataFlush());
        assertEquals(new FlushPolicy(1, 1000), BrokerConfig.parse("--flush-messages=1").dataFlush(), "Phase 3 flags still work");
        assertEquals(FlushPolicy.never(), BrokerConfig.parse("--flush-ms=never", "--flush-messages=never").dataFlush());
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse("--data-flush=sometimes"));
    }

    @Test
    void aSingleEntryClusterIsStillSingleBrokerMode() {
        assertTrue(BrokerConfig.parse("--node-id=5", "--cluster=5@h:9005").singleBroker());
        assertEquals(Path.of("./ember-data"), BrokerConfig.parse().dataDir());
    }
}
