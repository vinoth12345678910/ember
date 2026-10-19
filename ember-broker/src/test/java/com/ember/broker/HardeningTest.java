package com.ember.broker;

import com.ember.client.EmberClient;
import com.ember.protocol.*;
import com.ember.storage.LogRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HardeningTest {

    private static void assertBrokerStillServes(BrokerServer server) throws IOException {
        try (EmberClient client = new EmberClient("localhost", server.port())) {
            assertNotNull(client.metadata());
        }
    }

    @Test
    void garbageBytesCloseTheConnectionButNotTheBroker(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC())) {
            try (RawClient c = new RawClient(server.port())) {
                // as a length this is ~1.1 GB: far above maxFrameBytes
                c.sendRaw("GET / HTTP/1.1\r\nHost: x\r\n\r\n".getBytes());
                assertTrue(c.awaitClosed());
            }
            assertBrokerStillServes(server);
        }
    }

    @Test
    void aFrameLargerThanMaxFrameBytesClosesTheConnection(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir, "--max-frame-bytes=1024"), Clock.systemUTC())) {
            try (RawClient c = new RawClient(server.port())) {
                c.sendRaw(ByteBuffer.allocate(4 + 100).putInt(5000).array());   // announces 5000, sends little
                assertTrue(c.awaitClosed());
            }
            assertBrokerStillServes(server);
        }
    }

    @Test
    void aFrameOfExactlyMaxFrameBytesIsAccepted(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir, "--max-frame-bytes=1024"), Clock.systemUTC());
             EmberClient client = new EmberClient("localhost", server.port())) {
            // header 8 + topic 2+1 + partition 4 + count 4 + keyLen 4 + valueLen 4 + value = 1024
            int valueLength = 1024 - (8 + 3 + 4 + 4 + 4 + 4);
            assertEquals(0, client.produce("t", List.of(new ProducerRecord(null, new byte[valueLength]))));
            assertThrows(IOException.class, () -> client.produce("t", List.of(new ProducerRecord(null, new byte[valueLength + 1]))));
            assertBrokerStillServes(server);
        }
    }

    @Test
    void negativeLengthAndTooShortFramesCloseTheConnection(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC())) {
            try (RawClient c = new RawClient(server.port())) {
                c.sendRaw(ByteBuffer.allocate(4).putInt(-1).array());
                assertTrue(c.awaitClosed());
            }
            try (RawClient c = new RawClient(server.port())) {
                c.sendRaw(ByteBuffer.allocate(4 + 3).putInt(3).put(new byte[3]).array());   // header needs 8 bytes
                assertTrue(c.awaitClosed());
            }
            try (RawClient c = new RawClient(server.port())) {
                c.sendRaw(new byte[4]);                                                      // zero-length frame
                assertTrue(c.awaitClosed());
            }
            assertBrokerStillServes(server);
        }
    }

    @Test
    void pipelinedRequestsAreAnsweredInOrder(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir, "--segment-bytes=1000000"), Clock.systemUTC());
             RawClient c = new RawClient(server.port())) {
            int rounds = 40;
            int corr = 1;
            for (int i = 0; i < rounds; i++) {            // write everything first, read nothing yet
                c.send(corr++, new ProduceRequest("p", 0, List.of(new ProducerRecord(null, ("r" + 1).getBytes()))));
                c.send(corr++, new FetchRequest("p", 0, 0, 100_000));
                c.send(corr++, new MetadataRequest(List.of()));
            }
            int id = 1;
            for (int i = 0; i < rounds; i++) {
                ResponseCodec.Decoded produce = c.read(ApiKey.PRODUCE);
                assertEquals(id++, produce.correlationId());
                assertEquals(i, ((ProduceResponse) produce.body()).baseOffset());

                ResponseCodec.Decoded fetch = c.read(ApiKey.FETCH);
                assertEquals(id++, fetch.correlationId());
                List<LogRecord> records = RecordDecoder.decodeAll(((FetchResponse) fetch.body()).records());
                assertEquals(i + 1, records.size());      // the fetch sees every produce sent before it

                ResponseCodec.Decoded meta = c.read(ApiKey.METADATA);
                assertEquals(id++, meta.correlationId());
                assertEquals(i + 1, ((MetadataResponse) meta.body()).topics().get(0).partitions().get(0).logEndOffset());
            }
        }
    }

    @Test
    void aRequestArrivingInTinyPiecesIsReassembled(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
             RawClient c = new RawClient(server.port())) {
            ByteBuffer frame = RequestCodec.encodeFrame(new RequestHeader(ApiKey.PRODUCE, (short) 0, 9),
                    new ProduceRequest("t", 0, List.of(new ProducerRecord("k".getBytes(), "value".getBytes()))));
            byte[] bytes = new byte[frame.remaining()];
            frame.get(bytes);
            for (byte b : bytes) {
                c.sendRaw(new byte[]{b});                 // TCP_NODELAY on the client side makes these separate packets
            }
            assertEquals(0, ((ProduceResponse) c.read(ApiKey.PRODUCE).body()).baseOffset());
        }
    }

    @Test
    void twoRequestsInOneWriteBothGetAnswers(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
             RawClient c = new RawClient(server.port())) {
            ByteBuffer a = RequestCodec.encodeFrame(new RequestHeader(ApiKey.METADATA, (short) 0, 1), new MetadataRequest(List.of()));
            ByteBuffer b = RequestCodec.encodeFrame(new RequestHeader(ApiKey.METADATA, (short) 0, 2), new MetadataRequest(List.of()));
            ByteBuffer both = ByteBuffer.allocate(a.remaining() + b.remaining()).put(a).put(b);
            c.sendRaw(both.array());
            assertEquals(1, c.read(ApiKey.METADATA).correlationId());
            assertEquals(2, c.read(ApiKey.METADATA).correlationId());
        }
    }

    @Test
    void idleConnectionsAreClosed(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir, "--idle-timeout-ms=200"), Clock.systemUTC());
             RawClient c = new RawClient(server.port())) {
            assertTrue(c.awaitClosed());                  // blocks until the broker hangs up (or the 10 s socket timeout fails the test)
            assertBrokerStillServes(server);
        }
    }

    @Test
    void aClientThatVanishesDuringALargeFetchDoesNotHurtTheBroker(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir, "--segment-bytes=16777216", "--max-fetch-bytes=4194304"), Clock.systemUTC());
             EmberClient producer = new EmberClient("localhost", server.port())) {
            byte[] value = new byte[100_000];
            for (int i = 0; i < 40; i++) {
                producer.produce("t", List.of(new ProducerRecord(null, value)));
            }
            for (int i = 0; i < 5; i++) {
                try (RawClient c = new RawClient(server.port())) {
                    c.send(1, new FetchRequest("t", 0, 0, 4_000_000));
                }                                         // close without reading a single response byte
            }
            assertEquals(40, producer.fetch("t", 0, 0, 100).highWatermark());
            assertBrokerStillServes(server);
        }
    }

    @Test
    void manyConnectionsAtOnce(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC())) {
            List<EmberClient> clients = new ArrayList<>();
            try {
                for (int i = 0; i < 200; i++) {
                    clients.add(new EmberClient("localhost", server.port()));
                }
                long expectedTotal = 0;
                for (EmberClient c : clients) {
                    c.produce("many", List.of(new ProducerRecord(null, new byte[]{1})));
                    expectedTotal++;
                }
                assertEquals(expectedTotal, clients.get(0).fetch("many", 0, 0, 100_000).highWatermark());
            } finally {
                for (EmberClient c : clients) {
                    c.close();
                }
            }
        }
    }

    @Test
    void anOpenSocketThatNeverSendsAnythingDoesNotBlockOthers(@TempDir Path dir) throws Exception {
        try (BrokerServer server = ServerFixture.start(ServerFixture.config(dir), Clock.systemUTC());
             Socket silent = new Socket("localhost", server.port())) {
            assertBrokerStillServes(server);
        }
    }
}
