package com.ember.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MalformedRequestTest {

    private static final int CORR = 1234;

    /** Builds a frame body by hand so we can break it in ways the encoder refuses to. */
    private static ByteBuffer body(int apiKey, int version, ByteBufferWriter w) {
        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putShort((short) apiKey).putShort((short) version).putInt(CORR);
        w.write(buf);
        buf.flip();
        return buf;
    }

    interface ByteBufferWriter {
        void write(ByteBuffer buf);
    }

    private static void str(ByteBuffer buf, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) b.length).put(b);
    }

    private static ProtocolException failure(ByteBuffer body) {
        return assertThrows(ProtocolException.class, () -> RequestCodec.decode(body));
    }

    private static void assertInvalid(ByteBuffer body) {
        ProtocolException e = failure(body);
        assertEquals(ErrorCode.INVALID_REQUEST, e.errorCode());
        assertEquals(CORR, e.correlationId());
    }

    @Test
    void headerShorterThanEightBytesHasNoCorrelationId() {
        ProtocolException e = failure(ByteBuffer.allocate(7));
        assertNull(e.correlationId());
    }

    @Test
    void unsupportedApiKey() {
        ProtocolException e = failure(body(9, 0, buf -> { }));
        assertEquals(ErrorCode.UNSUPPORTED_API, e.errorCode());
        assertEquals(CORR, e.correlationId());
    }

    @Test
    void unsupportedVersion() {
        ProtocolException e = failure(body(0, 1, buf -> { }));
        assertEquals(ErrorCode.UNSUPPORTED_VERSION, e.errorCode());
        assertEquals(CORR, e.correlationId());
    }

    @Test
    void produceNegativeRecordCount() {
        assertInvalid(body(0, 0, buf -> { str(buf, "t"); buf.putInt(0).putInt(-1); }));
    }

    @Test
    void produceZeroRecords() {
        assertInvalid(body(0, 0, buf -> { str(buf, "t"); buf.putInt(0).putInt(0); }));
    }

    @Test
    void produceHugeRecordCountIsRejectedBeforeAllocating() {
        assertInvalid(body(0, 0, buf -> { str(buf, "t"); buf.putInt(0).putInt(Integer.MAX_VALUE); }));
    }

    @Test
    void produceKeyLengthPastTheEnd() {
        assertInvalid(body(0, 0, buf -> { str(buf, "t"); buf.putInt(0).putInt(1).putInt(1000).put((byte) 1).putInt(0); }));
    }

    @Test
    void produceKeyLengthBelowMinusOne() {
        assertInvalid(body(0, 0, buf -> { str(buf, "t"); buf.putInt(0).putInt(1).putInt(-2).putInt(0); }));
    }

    @Test
    void produceNegativeValueLength() {
        assertInvalid(body(0, 0, buf -> { str(buf, "t"); buf.putInt(0).putInt(1).putInt(-1).putInt(-5); }));
    }

    @Test
    void produceValueLengthPastTheEnd() {
        assertInvalid(body(0, 0, buf -> { str(buf, "t"); buf.putInt(0).putInt(1).putInt(-1).putInt(99).put((byte) 1); }));
    }

    @Test
    void produceTrailingGarbage() {
        assertInvalid(body(0, 0, buf -> { str(buf, "t"); buf.putInt(0).putInt(1).putInt(-1).putInt(0).put((byte) 7); }));
    }

    @Test
    void produceFewerRecordsThanAnnounced() {
        assertInvalid(body(0, 0, buf -> { str(buf, "t"); buf.putInt(0).putInt(3).putInt(-1).putInt(0).putInt(-1).putInt(0).putInt(-1); }));
    }

    @Test
    void stringLengthPastTheEnd() {
        assertInvalid(body(1, 0, buf -> buf.putShort((short) 200).put((byte) 'a')));
    }

    @Test
    void negativeStringLength() {
        assertInvalid(body(1, 0, buf -> buf.putShort((short) -1)));
    }

    @Test
    void topicWithBadUtf8() {
        assertInvalid(body(1, 0, buf -> {
            buf.putShort((short) 2).put((byte) 0xC3).put((byte) 0x28);        // invalid 2-byte sequence
            buf.putInt(0).putLong(0).putInt(10);
        }));
    }

    @Test
    void topicTooLong() {
        assertInvalid(body(1, 0, buf -> { str(buf, "a".repeat(129)); buf.putInt(0).putLong(0).putInt(10); }));
    }

    @Test
    void topicEmptyOrWithBadCharacters() {
        assertInvalid(body(1, 0, buf -> { str(buf, ""); buf.putInt(0).putLong(0).putInt(10); }));
        assertInvalid(body(1, 0, buf -> { str(buf, "a/b"); buf.putInt(0).putLong(0).putInt(10); }));
        assertInvalid(body(1, 0, buf -> { str(buf, "héllo"); buf.putInt(0).putLong(0).putInt(10); }));
    }

    @Test
    void topicOfExactly128CharsIsFine() {
        RequestCodec.Decoded d = RequestCodec.decode(body(1, 0, buf -> { str(buf, "a".repeat(128)); buf.putInt(0).putLong(0).putInt(10); }));
        assertEquals(128, ((FetchRequest) d.request()).topic().length());
    }

    @Test
    void fetchMaxBytesMustBePositive() {
        assertInvalid(body(1, 0, buf -> { str(buf, "t"); buf.putInt(0).putLong(0).putInt(0); }));
        assertInvalid(body(1, 0, buf -> { str(buf, "t"); buf.putInt(0).putLong(0).putInt(-1); }));
    }

    @Test
    void fetchTruncated() {
        assertInvalid(body(1, 0, buf -> { str(buf, "t"); buf.putInt(0).putLong(0); }));
    }

    @Test
    void fetchTrailingBytes() {
        assertInvalid(body(1, 0, buf -> { str(buf, "t"); buf.putInt(0).putLong(0).putInt(10).putInt(1); }));
    }

    @Test
    void metadataNegativeAndHugeTopicCount() {
        assertInvalid(body(2, 0, buf -> buf.putInt(-1)));
        assertInvalid(body(2, 0, buf -> buf.putInt(Integer.MAX_VALUE)));
    }

    @Test
    void metadataTrailingBytes() {
        assertInvalid(body(2, 0, buf -> buf.putInt(0).put((byte) 1)));
    }

    @Test
    void decodeDoesNotMoveTheCallersBufferPosition() {
        ByteBuffer b = RequestCodec.encode(new RequestHeader(ApiKey.METADATA, (short) 0, 1), new MetadataRequest(List.of()));
        int remaining = b.remaining();
        RequestCodec.decode(b);
        assertEquals(remaining, b.remaining());
    }

    @Test
    void encoderRefusesInvalidTopicsAndMismatchedHeaders() {
        assertThrows(ProtocolException.class, () -> RequestCodec.encode(
                new RequestHeader(ApiKey.FETCH, (short) 0, 1), new FetchRequest("bad topic", 0, 0, 1)));
        assertThrows(IllegalArgumentException.class, () -> RequestCodec.encode(
                new RequestHeader(ApiKey.PRODUCE, (short) 0, 1), new FetchRequest("t", 0, 0, 1)));
    }
}
