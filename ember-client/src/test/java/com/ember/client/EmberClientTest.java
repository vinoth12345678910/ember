package com.ember.client;

import com.ember.protocol.*;
import com.ember.storage.LogRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EmberClientTest {

    private static final ClientConfig FAST = new ClientConfig(2_000, 300, 1024 * 1024);

    private static ByteBuffer ok(int corr, Response body) {
        return ResponseCodec.encode(corr, ErrorCode.NONE, body);
    }

    @Test
    void produceReturnsTheBaseOffset() throws Exception {
        try (FakeServer s = new FakeServer(c -> {
            var req = c.readRequest();
            c.writeFrame(ok(req.header().correlationId(), new ProduceResponse("t", 0, 42, 2)));
        }); EmberClient client = new EmberClient("localhost", s.port(), FAST)) {
            assertEquals(42, client.produce("t", List.of(new ProducerRecord(null, new byte[]{1}), new ProducerRecord(null, new byte[]{2}))));
        }
    }

    @Test
    void correlationIdsIncreaseAndAreChecked() throws Exception {
        try (FakeServer s = new FakeServer(c -> {
            var first = c.readRequest();
            c.writeFrame(ok(first.header().correlationId(), new MetadataResponse(new MetadataResponse.BrokerInfo(0, "h", 1), List.of())));
            var second = c.readRequest();
            assertEquals(first.header().correlationId() + 1, second.header().correlationId());
            c.writeFrame(ok(second.header().correlationId() + 1, new MetadataResponse(new MetadataResponse.BrokerInfo(0, "h", 1), List.of())));
        }); EmberClient client = new EmberClient("localhost", s.port(), FAST)) {
            client.metadata();
            IOException e = assertThrows(IOException.class, client::metadata);
            assertTrue(e.getMessage().contains("correlation id mismatch"), e.getMessage());
            assertThrows(IOException.class, client::metadata);          // the client closed itself
        }
    }

    @Test
    void errorCodesBecomeEmberExceptionsAndTheClientStaysUsable() throws Exception {
        try (FakeServer s = new FakeServer(c -> {
            c.writeFrame(ResponseCodec.encodeError(c.readRequest().header().correlationId(), ErrorCode.OFFSET_OUT_OF_RANGE));
            var req = c.readRequest();
            c.writeFrame(ok(req.header().correlationId(), new ProduceResponse("t", 0, 0, 1)));
        }); EmberClient client = new EmberClient("localhost", s.port(), FAST)) {
            EmberException e = assertThrows(EmberException.class, () -> client.fetch("t", 0, 5, 100));
            assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, e.errorCode());
            assertEquals(0, client.produce("t", List.of(new ProducerRecord(null, new byte[0]))));
        }
    }

    @Test
    void readTimeoutThrowsAndClosesTheClient() throws Exception {
        try (FakeServer s = new FakeServer(c -> c.readRequest());       // reads the request, never answers
             EmberClient client = new EmberClient("localhost", s.port(), FAST)) {
            assertThrows(java.net.SocketTimeoutException.class, client::metadata);
            assertThrows(IOException.class, client::metadata);          // a late answer must never be mistaken for the next one
        }
    }

    @Test
    void oversizeResponseLengthIsRejectedBeforeAllocating() throws Exception {
        try (FakeServer s = new FakeServer(c -> {
            c.readRequest();
            c.out.writeInt(Integer.MAX_VALUE);
            c.out.flush();
        }); EmberClient client = new EmberClient("localhost", s.port(), FAST)) {
            IOException e = assertThrows(IOException.class, client::metadata);
            assertTrue(e.getMessage().contains("frame length"), e.getMessage());
        }
    }

    @Test
    void negativeResponseLengthIsRejected() throws Exception {
        try (FakeServer s = new FakeServer(c -> {
            c.readRequest();
            c.out.writeInt(-5);
            c.out.flush();
        }); EmberClient client = new EmberClient("localhost", s.port(), FAST)) {
            assertThrows(IOException.class, client::metadata);
        }
    }

    @Test
    void corruptRecordBytesAreDetectedByTheCrc() throws Exception {
        ByteBuffer rec = new LogRecord(0, 1, null, "hello".getBytes()).encode();
        byte[] bytes = new byte[rec.remaining()];
        rec.get(bytes);
        bytes[bytes.length - 1] ^= 0x55;
        try (FakeServer s = new FakeServer(c -> {
            var req = c.readRequest();
            c.writeFrame(ok(req.header().correlationId(), new FetchResponse("t", 0, 0, 1, ByteBuffer.wrap(bytes))));
        }); EmberClient client = new EmberClient("localhost", s.port(), FAST)) {
            IOException e = assertThrows(IOException.class, () -> client.fetch("t", 0, 0, 100));
            assertTrue(e.getMessage().contains("corrupt"), e.getMessage());
        }
    }

    @Test
    void fetchDecodesRecordsAndComputesTheNextOffset() throws Exception {
        ByteBuffer two = ByteBuffer.allocate(200);
        two.put(new LogRecord(7, 1, "k".getBytes(), "a".getBytes()).encode());
        two.put(new LogRecord(8, 1, null, "b".getBytes()).encode());
        two.flip();
        try (FakeServer s = new FakeServer(c -> {
            var req = c.readRequest();
            c.writeFrame(ok(req.header().correlationId(), new FetchResponse("t", 0, 3, 9, two)));
        }); EmberClient client = new EmberClient("localhost", s.port(), FAST)) {
            FetchResult r = client.fetch("t", 0, 7, 1000);
            assertEquals(2, r.records().size());
            assertEquals(3, r.logStartOffset());
            assertEquals(9, r.highWatermark());
            assertEquals(9, r.nextFetchOffset());
        }
    }

    @Test
    void metadataIsRequestedInVersionOneAndReturnsTheLeaders() throws Exception {
        MetadataResponse reply = new MetadataResponse(new MetadataResponse.BrokerInfo(2, "h", 7), List.of(
                new MetadataResponse.TopicMetadata("t", List.of(
                        new MetadataResponse.PartitionMetadata(0, 2, 0, 5),
                        new MetadataResponse.PartitionMetadata(1, 2, 0, 3)))));
        try (FakeServer s = new FakeServer(c -> {
            var req = c.readRequest();
            assertEquals(1, req.header().apiVersion());
            c.writeFrame(ResponseCodec.encode(req.header().correlationId(), ErrorCode.NONE, reply, (short) 1));
        }); EmberClient client = new EmberClient("localhost", s.port(), FAST)) {
            MetadataResponse m = client.metadata("t");
            assertEquals(reply, m);
            assertEquals(2, m.topics().get(0).partitions().get(1).leaderNodeId());
        }
    }

    @Test
    void createTopicReturnsTheCountAndMapsAlreadyExists() throws Exception {
        try (FakeServer s = new FakeServer(c -> {
            var first = c.readRequest();
            assertEquals(new CreateTopicRequest("t", 4), first.request());
            c.writeFrame(ok(first.header().correlationId(), new CreateTopicResponse("t", 4)));
            var second = c.readRequest();
            c.writeFrame(ResponseCodec.encodeError(second.header().correlationId(), ErrorCode.TOPIC_ALREADY_EXISTS));
        }); EmberClient client = new EmberClient("localhost", s.port(), FAST)) {
            assertEquals(4, client.createTopic("t", 4));
            assertEquals(ErrorCode.TOPIC_ALREADY_EXISTS,
                    assertThrows(EmberException.class, () -> client.createTopic("t", 4)).errorCode());
        }
    }

    @Test
    void producePassesThePartitionThrough() throws Exception {
        try (FakeServer s = new FakeServer(c -> {
            var req = c.readRequest();
            assertEquals(3, ((ProduceRequest) req.request()).partition());
            c.writeFrame(ok(req.header().correlationId(), new ProduceResponse("t", 3, 10, 1)));
        }); EmberClient client = new EmberClient("localhost", s.port(), FAST)) {
            assertEquals(10, client.produce("t", 3, List.of(new ProducerRecord(null, new byte[0]))));
        }
    }

    @Test
    void emptyFetchKeepsTheSameNextOffset() {
        assertEquals(5, new FetchResult(5, 0, 5, List.of()).nextFetchOffset());
    }

    @Test
    void aFirstRecordAtTheWrongOffsetIsAProtocolViolation() throws Exception {
        ByteBuffer wrong = new LogRecord(99, 1, null, "a".getBytes()).encode();
        try (FakeServer s = new FakeServer(c -> {
            var req = c.readRequest();
            c.writeFrame(ok(req.header().correlationId(), new FetchResponse("t", 0, 0, 100, wrong)));
        }); EmberClient client = new EmberClient("localhost", s.port(), FAST)) {
            assertThrows(IOException.class, () -> client.fetch("t", 0, 7, 1000));
        }
    }

    @Test
    void serverHangingUpMidResponseIsAnIOException() throws Exception {
        try (FakeServer s = new FakeServer(c -> {
            c.readRequest();
            c.out.writeInt(100);                    // promise 100 bytes, send none, then the thread keeps the socket... close it
            c.out.flush();
            c.out.close();
        }); EmberClient client = new EmberClient("localhost", s.port(), FAST)) {
            assertThrows(IOException.class, client::metadata);
        }
    }

    @Test
    void connectionRefusedIsAnIOException() throws Exception {
        int freePort;
        try (ServerSocket tmp = new ServerSocket(0)) {
            freePort = tmp.getLocalPort();
        }
        assertThrows(IOException.class, () -> new EmberClient("localhost", freePort, FAST));
    }
}
