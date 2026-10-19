package com.ember.broker;

import com.ember.client.EmberClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;

import static org.junit.jupiter.api.Assertions.*;

class DataDirLockTest {

    @Test
    void aSecondBrokerOnTheSameDirectoryFailsFastWithAClearMessage(@TempDir Path dir) throws Exception {
        try (BrokerServer first = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC())) {
            BrokerServer second = new BrokerServer(ServerFixture.config(dir), Clock.systemUTC());
            IllegalStateException e = assertThrows(IllegalStateException.class, second::start);
            assertTrue(e.getMessage().contains("another Ember broker is using this data directory"), e.getMessage());
            second.close();                                                 // closing a broker that never started is harmless

            // the failed attempt must not have released or damaged the first broker's lock
            try (EmberClient client = new EmberClient("localhost", first.port())) {
                assertNotNull(client.metadata());
            }
            BrokerServer third = new BrokerServer(ServerFixture.config(dir), Clock.systemUTC());
            assertThrows(IllegalStateException.class, third::start);
        }
    }

    @Test
    void afterTheFirstBrokerClosesANewOneStarts(@TempDir Path dir) throws Exception {
        BrokerServer first = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
        first.close();
        try (BrokerServer next = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
             EmberClient client = new EmberClient("localhost", next.port())) {
            assertNotNull(client.metadata());
        }
    }

    @Test
    void differentDirectoriesDoNotInterfere(@TempDir Path a, @TempDir Path b) throws Exception {
        try (BrokerServer one = ServerFixture.start(ServerFixture.config(a), Clock.systemUTC());
             BrokerServer two = ServerFixture.start(ServerFixture.config(b), Clock.systemUTC())) {
            assertNotEquals(one.port(), two.port());
        }
    }

    @Test
    void aStartThatFailsAfterTakingTheLockReleasesIt(@TempDir Path dir, @TempDir Path other) throws Exception {
        try (BrokerServer holder = ServerFixture.start(ServerFixture.config(other), Clock.systemUTC())) {
            // the port is taken, so this broker locks `dir`, then fails to bind
            BrokerServer failing = new BrokerServer(ServerFixture.config(dir, "--port=" + holder.port()), Clock.systemUTC());
            assertThrows(Exception.class, failing::start);
        }
        try (BrokerServer ok = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC())) {
            assertTrue(ok.port() > 0);                                      // the lock on `dir` was released
        }
    }
}
