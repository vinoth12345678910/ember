package com.ember.chaos;

import com.ember.storage.Log;
import com.ember.storage.LogRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/** The same CRC32 of a data log that the broker's debug endpoint computes: over (offset, timestamp, key, value) of every record. */
public final class LogDigest {

    public record Digest(long start, long end, long crc) {
    }

    private LogDigest() {
    }

    public static Digest of(Log log) throws IOException {
        long start = log.startOffset();
        long end = log.nextOffset();
        CRC32 crc = new CRC32();
        ByteBuffer scratch = ByteBuffer.allocate(16);
        for (long offset = start; offset < end; offset++) {
            LogRecord r = log.read(offset);
            scratch.clear();
            scratch.putLong(r.offset()).putLong(r.timestamp());
            crc.update(scratch.array(), 0, 16);
            crc.update(r.key() == null ? new byte[]{(byte) 0xFF} : r.key());
            crc.update(r.value());
        }
        return new Digest(start, end, crc.getValue());
    }
}
