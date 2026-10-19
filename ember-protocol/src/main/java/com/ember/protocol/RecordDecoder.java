package com.ember.protocol;

import com.ember.storage.LogRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class RecordDecoder {

    private RecordDecoder() {
    }

    /**
     * Decodes back-to-back LogRecords until the buffer is empty. Each LogRecord.decode checks
     * the CRC, so this is where corruption is detected on the client. A partial record at the
     * end throws CorruptRecordException.
     */
    public static List<LogRecord> decodeAll(ByteBuffer records) {
        ByteBuffer buf = records.slice();
        List<LogRecord> out = new ArrayList<>();
        while (buf.hasRemaining()) {
            out.add(LogRecord.decode(buf));
        }
        return out;
    }
}
