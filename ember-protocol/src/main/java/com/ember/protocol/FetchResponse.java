package com.ember.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * highWatermark: in Phase 2 this is simply the log end offset (the next offset to be assigned).
 * Phase 5 (replication) will change it to the replicated commit point.
 * records: zero or more complete LogRecord encodings, byte-identical to the log file.
 */
public record FetchResponse(String topic, int partition, long logStartOffset, long highWatermark,
                            ByteBuffer records) implements Response {

    /**
     * Number of bytes after the INT32 frameLength field for a FETCH response with
     * recordsLength bytes of records: corrId(4) + error(2) + topic(2+n) + partition(4)
     * + logStart(8) + highWatermark(8) + recordsLength(4) + records.
     */
    public static int frameLength(String topic, int recordsLength) {
        return 4 + 2 + 2 + topic.getBytes(StandardCharsets.UTF_8).length + 4 + 8 + 8 + 4 + recordsLength;
    }

    /**
     * Everything up to and including recordsLength, WITH the leading INT32 frameLength (which
     * already counts the records that will follow). The broker sends this buffer and then streams
     * the records straight from the file.
     */
    public static ByteBuffer encodeHeader(int correlationId, String topic, int partition,
                                          long logStartOffset, long highWatermark, int recordsLength) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int frameLength = frameLength(topic, recordsLength);
        ByteBuffer buf = ByteBuffer.allocate(4 + frameLength - recordsLength);
        buf.putInt(frameLength);
        buf.putInt(correlationId);
        buf.putShort(ErrorCode.NONE.code());
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(partition);
        buf.putLong(logStartOffset);
        buf.putLong(highWatermark);
        buf.putInt(recordsLength);
        buf.flip();
        return buf;
    }
}
