package com.ember.broker;

import com.ember.storage.FlushPolicy;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class BrokerConfigTest {

    @Test
    void defaults() {
        BrokerConfig c = BrokerConfig.defaults();
        assertEquals(0, c.nodeId());
        assertEquals("localhost", c.host());
        assertEquals(9099, c.port());
        assertEquals(Path.of("./ember-data"), c.dataDir());
        assertEquals(64L * 1024 * 1024, c.segmentBytes());
        assertEquals(1000, c.flushMs());
        assertEquals(Long.MAX_VALUE, c.flushMessages());
        assertEquals(8 * 1024 * 1024, c.maxFrameBytes());
        assertEquals(1024 * 1024, c.maxFetchBytes());
        assertTrue(c.autoCreateTopics());
        assertEquals(16, c.handlerThreads());              // Phase 5: a produce waits for its commit on a handler thread
        assertEquals(new FlushPolicy(Long.MAX_VALUE, 1000), c.flushPolicy());
    }

    @Test
    void flagsOverrideDefaults() {
        BrokerConfig c = BrokerConfig.parse("--port=0", "--data-dir=/tmp/x", "--flush-messages=1",
                "--flush-ms=never", "--auto-create-topics=false", "--node-id=3");
        assertEquals(0, c.port());
        assertEquals(Path.of("/tmp/x"), c.dataDir());
        assertEquals(FlushPolicy.everyMessage(), c.flushPolicy());
        assertFalse(c.autoCreateTopics());
        assertEquals(3, c.nodeId());
    }

    @Test
    void badFlagsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse("--nope=1"));
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse("port=1"));
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse("--port=abc"));
        assertThrows(IllegalArgumentException.class, () -> BrokerConfig.parse("--port"));
    }
}
