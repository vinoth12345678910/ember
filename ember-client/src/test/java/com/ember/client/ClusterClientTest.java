package com.ember.client;

import com.ember.protocol.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ClusterClientTest {

    private static final ClientConfig FAST = new ClientConfig(1_000, 2_000, 1024 * 1024).withRetries(5, 1, 5);

    private static ClusterMetadataResponse layout(int port, int leader) {
        return new ClusterMetadataResponse(List.of(new ClusterMetadataResponse.Broker(1, "localhost", port)), 1,
                List.of(new ClusterMetadataResponse.Topic("t", List.of(new ClusterMetadataResponse.Partition(0, leader, List.of(1), 0, 7)))));
    }

    private static void answerMetadata(FakeServer.Connection c, int port, int leader) throws IOException {
        var req = c.readRequest();
        assertEquals(2, req.header().apiVersion(), "the cluster client asks METADATA v2");
        c.writeFrame(ResponseCodec.encode(req.header().correlationId(), ErrorCode.NONE, layout(port, leader), (short) 2));
    }

    private static int deadPort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Test
    void aRetriableErrorRefreshesMetadataAndTheRetrySucceeds() throws Exception {
        int[] port = {0};
        try (FakeServer s = new FakeServer(c -> {
            answerMetadata(c, port[0], 1);
            var first = c.readRequest();                                   // PRODUCE -> not the leader (stale metadata)
            c.writeFrame(ResponseCodec.encodeError(first.header().correlationId(), ErrorCode.NOT_LEADER_OR_FOLLOWER));
            answerMetadata(c, port[0], 1);
            var second = c.readRequest();
            c.writeFrame(ResponseCodec.encode(second.header().correlationId(), ErrorCode.NONE, new ProduceResponse("t", 0, 42, 1)));
        })) {
            port[0] = s.port();
            try (ClusterClient client = new ClusterClient("localhost", s.port(), FAST)) {
                assertEquals(42, client.produce("t", 0, List.of(new ProducerRecord(null, new byte[]{1}))));
                assertEquals(1, client.retriedRequests());
            }
        }
    }

    @Test
    void aFatalErrorIsNotRetried() throws Exception {
        int[] port = {0};
        try (FakeServer s = new FakeServer(c -> {
            answerMetadata(c, port[0], 1);
            var req = c.readRequest();
            c.writeFrame(ResponseCodec.encodeError(req.header().correlationId(), ErrorCode.OFFSET_OUT_OF_RANGE));
        })) {
            port[0] = s.port();
            try (ClusterClient client = new ClusterClient("localhost", s.port(), FAST)) {
                EmberException e = assertThrows(EmberException.class, () -> client.fetch("t", 0, 99, 100));
                assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, e.errorCode());
                assertEquals(0, client.retriedRequests());
            }
        }
    }

    @Test
    void aDeadBootstrapBrokerIsSkipped() throws Exception {
        int[] port = {0};
        try (FakeServer s = new FakeServer(c -> {
            answerMetadata(c, port[0], 1);
            var req = c.readRequest();
            c.writeFrame(ResponseCodec.encode(req.header().correlationId(), ErrorCode.NONE, new ProduceResponse("t", 0, 5, 1)));
        })) {
            port[0] = s.port();
            ClientConfig cfg = FAST.withBootstrap(List.of("localhost:" + deadPort(), "localhost:" + s.port()));
            try (ClusterClient client = new ClusterClient(cfg)) {
                assertEquals(5, client.produce("t", 0, List.of(new ProducerRecord(null, new byte[]{1}))));
            }
        }
    }

    @Test
    void unknownTopicsFailFastInsteadOfUsingTheWholeRetryBudget() throws Exception {
        int[] port = {0};
        try (FakeServer s = new FakeServer(c -> {
            for (int i = 0; i < 20; i++) {                                 // metadata never lists topic "nope"
                var req = c.readRequest();
                c.writeFrame(ResponseCodec.encode(req.header().correlationId(), ErrorCode.NONE, layout(port[0], 1), (short) 2));
            }
        })) {
            port[0] = s.port();
            ClientConfig cfg = FAST.withRetries(50, 1, 2);
            try (ClusterClient client = new ClusterClient("localhost", s.port(), cfg)) {
                EmberException e = assertThrows(EmberException.class, () -> client.fetch("nope", 0, 0, 100));
                assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, e.errorCode());
                assertTrue(client.retriedRequests() <= 5, "retried " + client.retriedRequests());
            }
        }
    }

    @Test
    void whenNoBrokerAnswersTheClientGivesUpWithAnIoException() throws Exception {
        ClientConfig cfg = FAST.withBootstrap(List.of("localhost:" + deadPort())).withRetries(2, 1, 2);
        try (ClusterClient client = new ClusterClient(cfg)) {
            assertThrows(IOException.class, () -> client.produce("t", 0, List.of(new ProducerRecord(null, new byte[0]))));
            assertEquals(2, client.retriedRequests());
        }
    }

    @Test
    void aBootstrapListIsRequired() {
        assertThrows(IllegalArgumentException.class, () -> new ClusterClient(ClientConfig.defaults()));
        assertNotNull(ByteBuffer.allocate(0));
    }
}
