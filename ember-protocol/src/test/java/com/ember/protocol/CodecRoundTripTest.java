package com.ember.protocol;

import com.ember.storage.CorruptRecordException;
import com.ember.storage.LogRecord;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CodecRoundTripTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RequestCodec.Decoded roundTrip(int corr, Request request) {
        return RequestCodec.decode(RequestCodec.encode(new RequestHeader(request.apiKey(), (short) 0, corr), request));
    }

    @Test
    void produceRequestWithNullKeyAndEmptyValue() {
        ProduceRequest in = new ProduceRequest("orders", 0, List.of(
                new ProducerRecord(b("k"), b("v")),
                new ProducerRecord(null, b("no key")),
                new ProducerRecord(b(""), new byte[0])));
        RequestCodec.Decoded out = roundTrip(42, in);
        assertEquals(42, out.header().correlationId());
        assertEquals(ApiKey.PRODUCE, out.header().apiKey());
        ProduceRequest p = (ProduceRequest) out.request();
        assertEquals("orders", p.topic());
        assertEquals(3, p.records().size());
        assertArrayEquals(b("k"), p.records().get(0).key());
        assertNull(p.records().get(1).key());
        assertArrayEquals(b(""), p.records().get(2).key());
        assertEquals(0, p.records().get(2).value().length);
    }

    @Test
    void fetchRequest() {
        RequestCodec.Decoded out = roundTrip(-7, new FetchRequest("t.1-a_b", 0, 123456789012L, 65536));
        assertEquals(-7, out.header().correlationId());
        assertEquals(new FetchRequest("t.1-a_b", 0, 123456789012L, 65536), out.request());
    }

    @Test
    void metadataRequest() {
        assertEquals(new MetadataRequest(List.of()), roundTrip(1, new MetadataRequest(List.of())).request());
        assertEquals(new MetadataRequest(List.of("a", "b")), roundTrip(1, new MetadataRequest(List.of("a", "b"))).request());
    }

    @Test
    void encodeFramePutsTheBodyLengthFirst() {
        ByteBuffer frame = RequestCodec.encodeFrame(new RequestHeader(ApiKey.METADATA, (short) 0, 9), new MetadataRequest(List.of()));
        assertEquals(frame.remaining() - 4, frame.getInt(0));
    }

    @Test
    void produceResponse() {
        ResponseCodec.Decoded out = ResponseCodec.decode(ApiKey.PRODUCE,
                ResponseCodec.encode(5, ErrorCode.NONE, new ProduceResponse("orders", 0, 100L, 3)));
        assertEquals(5, out.correlationId());
        assertEquals(ErrorCode.NONE, out.errorCode());
        assertEquals(new ProduceResponse("orders", 0, 100L, 3), out.body());
    }

    @Test
    void fetchResponseCarriesRecordBytes() {
        ByteBuffer records = ByteBuffer.allocate(0);
        LogRecord r1 = new LogRecord(10, 1000, b("k"), b("v1"));
        LogRecord r2 = new LogRecord(11, 1001, null, b("v2"));
        ByteBuffer all = ByteBuffer.allocate(r1.sizeInBytes() + r2.sizeInBytes());
        all.put(r1.encode()).put(r2.encode()).flip();

        ResponseCodec.Decoded out = ResponseCodec.decode(ApiKey.FETCH,
                ResponseCodec.encode(6, ErrorCode.NONE, new FetchResponse("t", 0, 5, 12, all)));
        FetchResponse f = (FetchResponse) out.body();
        assertEquals(5, f.logStartOffset());
        assertEquals(12, f.highWatermark());
        List<LogRecord> decoded = RecordDecoder.decodeAll(f.records());
        assertEquals(List.of(10L, 11L), decoded.stream().map(LogRecord::offset).toList());
        assertNull(decoded.get(1).key());
        assertEquals(0, records.remaining());
    }

    @Test
    void emptyFetchResponse() {
        FetchResponse f = (FetchResponse) ResponseCodec.decode(ApiKey.FETCH,
                ResponseCodec.encode(1, ErrorCode.NONE, new FetchResponse("t", 0, 0, 0, ByteBuffer.allocate(0)))).body();
        assertEquals(0, f.records().remaining());
        assertTrue(RecordDecoder.decodeAll(f.records()).isEmpty());
    }

    @Test
    void metadataResponse() {
        MetadataResponse in = new MetadataResponse(new MetadataResponse.BrokerInfo(3, "localhost", 9099), List.of(
                new MetadataResponse.TopicMetadata("a", List.of(new MetadataResponse.PartitionMetadata(0, 0, 10))),
                new MetadataResponse.TopicMetadata("b", List.of(new MetadataResponse.PartitionMetadata(0, 5, 6)))));
        assertEquals(in, ResponseCodec.decode(ApiKey.METADATA, ResponseCodec.encode(2, ErrorCode.NONE, in)).body());
        MetadataResponse empty = new MetadataResponse(new MetadataResponse.BrokerInfo(0, "h", 1), List.of());
        assertEquals(empty, ResponseCodec.decode(ApiKey.METADATA, ResponseCodec.encode(2, ErrorCode.NONE, empty)).body());
    }

    @Test
    void errorResponseEndsAfterTheErrorCode() {
        for (ErrorCode code : ErrorCode.values()) {
            if (code == ErrorCode.NONE) continue;
            ByteBuffer bytes = ResponseCodec.encodeError(77, code);
            assertEquals(6, bytes.remaining());
            ResponseCodec.Decoded out = ResponseCodec.decode(ApiKey.FETCH, bytes);
            assertEquals(77, out.correlationId());
            assertEquals(code, out.errorCode());
            assertNull(out.body());
        }
    }

    @Test
    void errorResponseWithTrailingBytesIsRejected() {
        ByteBuffer bytes = ByteBuffer.allocate(7);
        bytes.putInt(1).putShort(ErrorCode.INTERNAL_ERROR.code()).put((byte) 0).flip();
        assertThrows(ProtocolException.class, () -> ResponseCodec.decode(ApiKey.PRODUCE, bytes));
    }

    @Test
    void unknownErrorCodeIsRejected() {
        ByteBuffer bytes = ByteBuffer.allocate(6);
        bytes.putInt(1).putShort((short) 99).flip();
        assertThrows(ProtocolException.class, () -> ResponseCodec.decode(ApiKey.PRODUCE, bytes));
    }

    @Test
    void fetchHeaderPlusRecordsEqualsWholeFrameEncode() {
        LogRecord r = new LogRecord(0, 1, b("key"), b("value"));
        ByteBuffer records = r.encode();
        int recordsLength = records.remaining();

        ByteBuffer header = FetchResponse.encodeHeader(31, "topic-x", 0, 4, 99, recordsLength);
        ByteBuffer streamed = ByteBuffer.allocate(header.remaining() + recordsLength);
        streamed.put(header).put(records.duplicate()).flip();

        ByteBuffer whole = ResponseCodec.encodeFrame(31, ErrorCode.NONE,
                new FetchResponse("topic-x", 0, 4, 99, records));
        assertEquals(whole, streamed);
        assertEquals(streamed.remaining() - 4, FetchResponse.frameLength("topic-x", recordsLength));
    }

    @Test
    void recordDecoderRejectsAPartialTrailingRecord() {
        ByteBuffer full = new LogRecord(0, 1, null, b("abc")).encode();
        ByteBuffer cut = ByteBuffer.wrap(java.util.Arrays.copyOf(full.array(), full.remaining() - 1));
        assertThrows(CorruptRecordException.class, () -> RecordDecoder.decodeAll(cut));
    }

    @Test
    void recordDecoderDetectsFlippedBits() {
        ByteBuffer full = new LogRecord(0, 1, null, b("abc")).encode();
        byte[] bytes = full.array().clone();
        bytes[bytes.length - 1] ^= 1;
        assertThrows(CorruptRecordException.class, () -> RecordDecoder.decodeAll(ByteBuffer.wrap(bytes)));
    }
}
