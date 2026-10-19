package com.ember.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class Phase3CodecTest {

    private static final int CORR = 77;

    private static ByteBuffer body(int apiKey, int version, java.util.function.Consumer<ByteBuffer> writer) {
        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putShort((short) apiKey).putShort((short) version).putInt(CORR);
        writer.accept(buf);
        buf.flip();
        return buf;
    }

    private static void str(ByteBuffer buf, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) b.length).put(b);
    }

    private static ProtocolException failure(ByteBuffer b) {
        return assertThrows(ProtocolException.class, () -> RequestCodec.decode(b));
    }

    private static MetadataResponse sampleMetadata() {
        return new MetadataResponse(new MetadataResponse.BrokerInfo(4, "h", 9),
                List.of(new MetadataResponse.TopicMetadata("a", List.of(
                                new MetadataResponse.PartitionMetadata(0, 4, 0, 10),
                                new MetadataResponse.PartitionMetadata(1, 4, 5, 6))),
                        new MetadataResponse.TopicMetadata("b", List.of(new MetadataResponse.PartitionMetadata(0, 4, 0, 0)))));
    }

    // ---- CREATE_TOPIC

    @Test
    void createTopicRequestRoundTrips() {
        RequestCodec.Decoded d = RequestCodec.decode(RequestCodec.encode(
                new RequestHeader(ApiKey.CREATE_TOPIC, (short) 0, 5), new CreateTopicRequest("my-topic.v2", 12)));
        assertEquals(ApiKey.CREATE_TOPIC, d.header().apiKey());
        assertEquals(5, d.header().correlationId());
        assertEquals(new CreateTopicRequest("my-topic.v2", 12), d.request());
    }

    @Test
    void createTopicResponseRoundTrips() {
        ResponseCodec.Decoded d = ResponseCodec.decode(ApiKey.CREATE_TOPIC,
                ResponseCodec.encode(5, ErrorCode.NONE, new CreateTopicResponse("t", 4)));
        assertEquals(new CreateTopicResponse("t", 4), d.body());
    }

    @Test
    void topicAlreadyExistsIsErrorCodeEightAndHasNoBody() {
        assertEquals(8, ErrorCode.TOPIC_ALREADY_EXISTS.code());
        assertEquals(ErrorCode.TOPIC_ALREADY_EXISTS, ErrorCode.fromCode((short) 8));
        ByteBuffer bytes = ResponseCodec.encodeError(9, ErrorCode.TOPIC_ALREADY_EXISTS);
        assertEquals(6, bytes.remaining());
        ResponseCodec.Decoded d = ResponseCodec.decode(ApiKey.CREATE_TOPIC, bytes);
        assertEquals(ErrorCode.TOPIC_ALREADY_EXISTS, d.errorCode());
        assertNull(d.body());
    }

    @Test
    void createTopicRejectsPartitionCountBelowOne() {
        for (int bad : new int[]{0, -1, Integer.MIN_VALUE}) {
            ProtocolException e = failure(body(3, 0, buf -> { str(buf, "t"); buf.putInt(bad); }));
            assertEquals(ErrorCode.INVALID_REQUEST, e.errorCode());
            assertEquals(CORR, e.correlationId());
        }
    }

    @Test
    void createTopicMalformedBodies() {
        assertEquals(ErrorCode.INVALID_REQUEST, failure(body(3, 0, buf -> { str(buf, "bad name"); buf.putInt(1); })).errorCode());
        assertEquals(ErrorCode.INVALID_REQUEST, failure(body(3, 0, buf -> { str(buf, "x".repeat(129)); buf.putInt(1); })).errorCode());
        assertEquals(ErrorCode.INVALID_REQUEST, failure(body(3, 0, buf -> { str(buf, ""); buf.putInt(1); })).errorCode());
        assertEquals(ErrorCode.INVALID_REQUEST, failure(body(3, 0, buf -> buf.putShort((short) 50).put((byte) 'a'))).errorCode());   // length past the end
        assertEquals(ErrorCode.INVALID_REQUEST, failure(body(3, 0, buf -> { str(buf, "t"); buf.putShort((short) 1); })).errorCode());   // partitionCount truncated
        assertEquals(ErrorCode.INVALID_REQUEST, failure(body(3, 0, buf -> { str(buf, "t"); buf.putInt(2).put((byte) 1); })).errorCode());  // trailing byte
        assertEquals(ErrorCode.INVALID_REQUEST, failure(body(3, 0, buf -> buf.putShort((short) 2).put((byte) 0xC3).put((byte) 0x28).putInt(1))).errorCode());  // bad UTF-8
    }

    @Test
    void anAbsurdlyLargePartitionCountDecodesBecauseTheLimitIsBrokerConfig() {
        RequestCodec.Decoded d = RequestCodec.decode(body(3, 0, buf -> { str(buf, "t"); buf.putInt(Integer.MAX_VALUE); }));
        assertEquals(Integer.MAX_VALUE, ((CreateTopicRequest) d.request()).partitionCount());
    }

    // ---- versions

    @Test
    void onlyMetadataSupportsVersionOne() {
        for (int api : new int[]{0, 1, 3}) {
            ProtocolException e = failure(body(api, 1, buf -> { }));
            assertEquals(ErrorCode.UNSUPPORTED_VERSION, e.errorCode(), "api " + api);
            assertEquals(CORR, e.correlationId());
        }
        assertEquals(ErrorCode.UNSUPPORTED_VERSION, failure(body(2, 3, buf -> buf.putInt(0))).errorCode());   // Phase 5: METADATA v2 now exists
        assertEquals(ErrorCode.UNSUPPORTED_VERSION, failure(body(2, -1, buf -> buf.putInt(0))).errorCode());
        assertEquals(ErrorCode.UNSUPPORTED_API, failure(body(4, 0, buf -> { })).errorCode());
    }

    @Test
    void metadataRequestVersionOneRoundTripsWithTheSameBodyAsVersionZero() {
        MetadataRequest req = new MetadataRequest(List.of("a", "b"));
        ByteBuffer v0 = RequestCodec.encode(new RequestHeader(ApiKey.METADATA, (short) 0, 1), req);
        ByteBuffer v1 = RequestCodec.encode(new RequestHeader(ApiKey.METADATA, (short) 1, 1), req);
        assertEquals(v0.remaining(), v1.remaining());
        RequestCodec.Decoded d = RequestCodec.decode(v1);
        assertEquals(1, d.header().apiVersion());
        assertEquals(req, d.request());
        assertEquals(0, RequestCodec.decode(v0).header().apiVersion());
    }

    // ---- METADATA v0 / v1 responses

    @Test
    void metadataV1CarriesTheLeaderAndV0DoesNot() {
        MetadataResponse in = sampleMetadata();
        MetadataResponse v1 = (MetadataResponse) ResponseCodec.decode(ApiKey.METADATA, (short) 1,
                ResponseCodec.encode(3, ErrorCode.NONE, in, (short) 1)).body();
        assertEquals(in, v1);
        assertEquals(4, v1.topics().get(0).partitions().get(1).leaderNodeId());

        MetadataResponse v0 = (MetadataResponse) ResponseCodec.decode(ApiKey.METADATA, (short) 0,
                ResponseCodec.encode(3, ErrorCode.NONE, in, (short) 0)).body();
        assertEquals(MetadataResponse.PartitionMetadata.NO_LEADER, v0.topics().get(0).partitions().get(0).leaderNodeId());
        assertEquals(10, v0.topics().get(0).partitions().get(0).logEndOffset());
        assertEquals(in.broker(), v0.broker());
    }

    @Test
    void v1IsExactlyFourBytesPerPartitionLongerThanV0() {
        MetadataResponse in = sampleMetadata();                  // 3 partitions in total
        int v0 = ResponseCodec.encode(1, ErrorCode.NONE, in, (short) 0).remaining();
        int v1 = ResponseCodec.encode(1, ErrorCode.NONE, in, (short) 1).remaining();
        assertEquals(v0 + 3 * 4, v1);
    }

    @Test
    void oldDefaultEncodingIsStillVersionZero() {
        MetadataResponse in = sampleMetadata();
        assertEquals(ResponseCodec.encode(1, ErrorCode.NONE, in, (short) 0), ResponseCodec.encode(1, ErrorCode.NONE, in));
    }

    @Test
    void decodingV0BytesAsV1Fails() {
        ByteBuffer v0 = ResponseCodec.encode(1, ErrorCode.NONE, sampleMetadata(), (short) 0);
        assertThrows(ProtocolException.class, () -> ResponseCodec.decode(ApiKey.METADATA, (short) 1, v0));
    }

    @Test
    void metadataV1ResponseWithAHugePartitionCountIsRejectedBeforeAllocating() {
        ByteBuffer buf = ByteBuffer.allocate(100);
        buf.putInt(1).putShort(ErrorCode.NONE.code());
        buf.putInt(0);                       // nodeId
        str(buf, "h");
        buf.putInt(1);                       // port
        buf.putInt(1);                       // topicCount
        str(buf, "t");
        buf.putInt(Integer.MAX_VALUE);       // partitionCount: a lie
        buf.flip();
        assertThrows(ProtocolException.class, () -> ResponseCodec.decode(ApiKey.METADATA, (short) 1, buf));
    }

    @Test
    void metadataV1ResponseWithTrailingBytesIsRejected() {
        ByteBuffer ok = ResponseCodec.encode(1, ErrorCode.NONE, sampleMetadata(), (short) 1);
        ByteBuffer longer = ByteBuffer.allocate(ok.remaining() + 1).put(ok).put((byte) 9).flip();
        assertThrows(ProtocolException.class, () -> ResponseCodec.decode(ApiKey.METADATA, (short) 1, longer));
    }

    @Test
    void createTopicResponseTruncatedOrWithTrailingBytesIsRejected() {
        ByteBuffer ok = ResponseCodec.encode(1, ErrorCode.NONE, new CreateTopicResponse("t", 2));
        ByteBuffer cut = ok.duplicate();
        cut.limit(cut.limit() - 1);
        assertThrows(ProtocolException.class, () -> ResponseCodec.decode(ApiKey.CREATE_TOPIC, cut));
        ByteBuffer longer = ByteBuffer.allocate(ok.remaining() + 2).put(ok).putShort((short) 0).flip();
        assertThrows(ProtocolException.class, () -> ResponseCodec.decode(ApiKey.CREATE_TOPIC, longer));
    }
}
