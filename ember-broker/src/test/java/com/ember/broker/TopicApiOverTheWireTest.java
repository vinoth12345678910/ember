package com.ember.broker;

import com.ember.protocol.*;
import com.ember.storage.LogRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** CREATE_TOPIC and METADATA v0/v1 through the real Netty pipeline, using raw frames. */
class TopicApiOverTheWireTest {

    private static BrokerServer start(Path dir, String... extra) throws Exception {
        return ServerFixture.start(ServerFixture.config(dir, extra), Clock.systemUTC());
    }

    @Test
    void createTopicThenDuplicate(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir); RawClient c = new RawClient(server.port())) {
            c.send(1, new CreateTopicRequest("orders", 4));
            ResponseCodec.Decoded ok = c.read(ApiKey.CREATE_TOPIC);
            assertEquals(1, ok.correlationId());
            assertEquals(new CreateTopicResponse("orders", 4), ok.body());

            c.send(2, new CreateTopicRequest("orders", 4));
            ResponseCodec.Decoded dup = c.read(ApiKey.CREATE_TOPIC);
            assertEquals(ErrorCode.TOPIC_ALREADY_EXISTS, dup.errorCode());
            assertNull(dup.body());

            c.send(3, new CreateTopicRequest("orders", 9));      // different count: still "already exists"
            assertEquals(ErrorCode.TOPIC_ALREADY_EXISTS, c.read(ApiKey.CREATE_TOPIC).errorCode());
            assertEquals(4, server.logManager().topic("orders").partitionCount());
        }
    }

    @Test
    void invalidCreateRequestsAreRejectedWithoutKillingTheConnection(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir, "--max-partitions=8"); RawClient c = new RawClient(server.port())) {
            c.send(1, new CreateTopicRequest("t", 9));                      // over the broker's limit: broker decides
            assertEquals(ErrorCode.INVALID_REQUEST, c.read(ApiKey.CREATE_TOPIC).errorCode());

            // partitionCount 0 never reaches the broker logic: the codec rejects it, the connection stays open
            ByteBuffer body = ByteBuffer.allocate(2 + 1 + 4).putShort((short) 1).put((byte) 't').putInt(0);
            c.sendRawBody(3, 0, 2, body.array());
            ResponseCodec.Decoded zero = c.read(ApiKey.CREATE_TOPIC);
            assertEquals(2, zero.correlationId());
            assertEquals(ErrorCode.INVALID_REQUEST, zero.errorCode());

            ByteBuffer badName = ByteBuffer.allocate(2 + 3 + 4).putShort((short) 3).put("a/b".getBytes()).putInt(1);
            c.sendRawBody(3, 0, 3, badName.array());
            assertEquals(ErrorCode.INVALID_REQUEST, c.read(ApiKey.CREATE_TOPIC).errorCode());

            c.send(4, new CreateTopicRequest("fine", 8));
            assertEquals(ErrorCode.NONE, c.read(ApiKey.CREATE_TOPIC).errorCode());
            assertNull(server.logManager().topic("t"));
        }
    }

    @Test
    void metadataV0AndV1AnswerInTheirOwnLayouts(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir, "--node-id=3"); RawClient c = new RawClient(server.port())) {
            c.send(1, new CreateTopicRequest("a", 2));
            c.read(ApiKey.CREATE_TOPIC);
            Await.until(10_000, "both partitions of topic a to have a leader",
                    () -> server.logManager().topic("a").replica(0).group().isLeader() && server.logManager().topic("a").replica(1).group().isLeader());

            c.send(2, (short) 0, new MetadataRequest(List.of()));
            ByteBuffer v0Bytes = c.readFrame();
            c.send(3, (short) 1, new MetadataRequest(List.of()));
            ByteBuffer v1Bytes = c.readFrame();
            assertEquals(v0Bytes.remaining() + 2 * 4, v1Bytes.remaining());          // 2 partitions, 4 bytes more each

            MetadataResponse v0 = (MetadataResponse) ResponseCodec.decode(ApiKey.METADATA, (short) 0, v0Bytes).body();
            MetadataResponse v1 = (MetadataResponse) ResponseCodec.decode(ApiKey.METADATA, (short) 1, v1Bytes).body();
            assertEquals(MetadataResponse.PartitionMetadata.NO_LEADER, v0.topics().get(0).partitions().get(0).leaderNodeId());
            assertEquals(3, v1.topics().get(0).partitions().get(1).leaderNodeId());
            assertEquals(2, v1.topics().get(0).partitions().size());
            assertEquals(server.port(), v1.broker().port());
        }
    }

    @Test
    void unsupportedVersionsGetUnsupportedVersion(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir); RawClient c = new RawClient(server.port())) {
            c.sendRawBody(2, 3, 1, new byte[]{0, 0, 0, 0});                          // METADATA v3 (v2 exists since Phase 5)
            assertEquals(ErrorCode.UNSUPPORTED_VERSION, c.read(ApiKey.METADATA).errorCode());
            c.sendRawBody(3, 1, 2, new byte[0]);                                     // CREATE_TOPIC v1
            ResponseCodec.Decoded d = c.read(ApiKey.CREATE_TOPIC);
            assertEquals(2, d.correlationId());
            assertEquals(ErrorCode.UNSUPPORTED_VERSION, d.errorCode());
            c.sendRawBody(0, 1, 3, new byte[0]);                                     // PRODUCE v1
            assertEquals(ErrorCode.UNSUPPORTED_VERSION, c.read(ApiKey.PRODUCE).errorCode());
            c.sendRawBody(1, 1, 4, new byte[0]);                                     // FETCH v1
            assertEquals(ErrorCode.UNSUPPORTED_VERSION, c.read(ApiKey.FETCH).errorCode());
            c.send(5, new MetadataRequest(List.of()));
            assertEquals(ErrorCode.NONE, c.read(ApiKey.METADATA).errorCode());       // connection still fine
        }
    }

    @Test
    void phase2StyleClientsStillWorkOnPartitionZero(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir, "--default-partitions=3"); RawClient c = new RawClient(server.port())) {
            c.send(1, new ProduceRequest("old", 0, List.of(new ProducerRecord(null, "v".getBytes()))));
            assertEquals(new ProduceResponse("old", 0, 0, 1), c.read(ApiKey.PRODUCE).body());
            assertEquals(3, server.logManager().topic("old").partitionCount());     // auto-created with the default

            c.send(2, new FetchRequest("old", 0, 0, 1000));
            FetchResponse f = (FetchResponse) c.read(ApiKey.FETCH).body();
            assertEquals(0, RecordDecoder.decodeAll(f.records()).get(0).offset());

            c.send(3, new MetadataRequest(List.of("old")));                          // v0
            MetadataResponse m = (MetadataResponse) c.read(ApiKey.METADATA).body();
            assertEquals(3, m.topics().get(0).partitions().size());
        }
    }

    @Test
    void partitionsOutOfRangeAreUnknownOverTheWire(@TempDir Path dir) throws Exception {
        try (BrokerServer server = start(dir); RawClient c = new RawClient(server.port())) {
            c.send(1, new CreateTopicRequest("t", 2));
            c.read(ApiKey.CREATE_TOPIC);
            c.send(2, new ProduceRequest("t", 2, List.of(new ProducerRecord(null, "x".getBytes()))));
            assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, c.read(ApiKey.PRODUCE).errorCode());
            c.send(3, new ProduceRequest("t", 1, List.of(new ProducerRecord(null, "x".getBytes()))));
            assertEquals(new ProduceResponse("t", 1, 0, 1), c.read(ApiKey.PRODUCE).body());
            c.send(4, new FetchRequest("t", 1, 0, 1000));
            List<LogRecord> got = RecordDecoder.decodeAll(((FetchResponse) c.read(ApiKey.FETCH).body()).records());
            assertEquals(1, got.size());
            c.send(5, new FetchRequest("t", 2, 0, 1000));
            assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, c.read(ApiKey.FETCH).errorCode());
        }
    }
}
