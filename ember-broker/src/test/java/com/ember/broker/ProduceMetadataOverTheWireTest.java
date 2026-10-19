package com.ember.broker;

import com.ember.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProduceMetadataOverTheWireTest {

    private static ProducerRecord rec(String v) {
        return new ProducerRecord(null, v.getBytes());
    }

    @Test
    void produceThenMetadata(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir, "--host=h.example", "--node-id=2"), Clock.systemUTC());
             RawClient c = new RawClient(server.port())) {
            c.send(11, new ProduceRequest("orders", 0, List.of(rec("a"), rec("b"), rec("c"))));
            ResponseCodec.Decoded produced = c.read(ApiKey.PRODUCE);
            assertEquals(11, produced.correlationId());
            assertEquals(new ProduceResponse("orders", 0, 0, 3), produced.body());

            c.send(12, new ProduceRequest("orders", 0, List.of(rec("d"))));
            assertEquals(3, ((ProduceResponse) c.read(ApiKey.PRODUCE).body()).baseOffset());

            c.send(13, new MetadataRequest(List.of()));
            MetadataResponse meta = (MetadataResponse) c.read(ApiKey.METADATA).body();
            assertEquals(new MetadataResponse.BrokerInfo(2, "h.example", server.port()), meta.broker());
            assertEquals(new MetadataResponse.PartitionMetadata(0, 0, 4), meta.topics().get(0).partitions().get(0));
        }
    }

    @Test
    void errorsComeBackAsErrorCodesOnTheSameConnection(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
             RawClient c = new RawClient(server.port())) {
            c.send(1, new ProduceRequest("t", 5, List.of(rec("x"))));
            assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, c.read(ApiKey.PRODUCE).errorCode());

            c.sendRawBody(9, 0, 2, new byte[0]);
            ResponseCodec.Decoded unsupportedApi = c.read(ApiKey.PRODUCE);
            assertEquals(2, unsupportedApi.correlationId());
            assertEquals(ErrorCode.UNSUPPORTED_API, unsupportedApi.errorCode());

            c.sendRawBody(0, 1, 3, new byte[0]);
            assertEquals(ErrorCode.UNSUPPORTED_VERSION, c.read(ApiKey.PRODUCE).errorCode());

            c.sendRawBody(0, 0, 4, new byte[]{0, 5});          // body is cut off
            ResponseCodec.Decoded invalid = c.read(ApiKey.PRODUCE);
            assertEquals(4, invalid.correlationId());
            assertEquals(ErrorCode.INVALID_REQUEST, invalid.errorCode());

            c.send(5, new MetadataRequest(List.of()));          // the connection still works
            assertEquals(ErrorCode.NONE, c.read(ApiKey.METADATA).errorCode());
        }
    }

    @Test
    void gracefulShutdownWithAConnectedClientIsQuick(@TempDir Path dir) throws Exception {
        BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
        try (RawClient c = new RawClient(server.port())) {
            c.send(1, new MetadataRequest(List.of()));
            c.read(ApiKey.METADATA);
            long start = System.nanoTime();
            server.close();
            server.close();                                     // twice is harmless
            long ms = (System.nanoTime() - start) / 1_000_000;
            assertTrue(ms < 5000, "shutdown took " + ms + " ms");
            assertTrue(c.awaitClosed());                        // the broker hung up on the client
        }
    }

    @Test
    void closeBeforeStartIsHarmless(@TempDir Path dir) {
        new BrokerServer(ServerFixture.config(dir), Clock.systemUTC()).close();
    }

    @Test
    void startingOnAPortThatIsTakenFailsCleanly(@TempDir Path dir, @TempDir Path dir2) throws Exception {
        try (BrokerServer first = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC())) {
            BrokerServer second = new BrokerServer(ServerFixture.config(dir2, "--port=" + first.port()), Clock.systemUTC());
            assertThrows(Exception.class, second::start);
        }
    }
}
