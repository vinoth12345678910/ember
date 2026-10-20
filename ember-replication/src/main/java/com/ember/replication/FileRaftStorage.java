package com.ember.replication;

import com.ember.raft.EntryType;
import com.ember.raft.LogEntry;
import com.ember.raft.RaftStorage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import static java.nio.file.StandardOpenOption.*;

/**
 * The durable Raft state of ONE group: {@code state} (currentTerm + votedFor, replaced atomically) and
 * {@code log} (append-only, checksummed entries). Not thread-safe: the GroupRuntime serializes all calls.
 *
 * Entry frame: [INT32 length][INT32 crc][INT64 index][INT64 term][INT8 type][payload]
 * where length counts everything after itself and crc covers index..payload.
 */
public final class FileRaftStorage implements RaftStorage, AutoCloseable {

    private static final System.Logger LOG = System.getLogger(FileRaftStorage.class.getName());
    private static final int FIXED = 4 + 8 + 8 + 1;                // crc + index + term + type
    private static final int MAX_ENTRY = 128 * 1024 * 1024;         // sanity bound used while scanning, never allocate more
    private static final int CACHE_ENTRIES = 256;

    private final Path dir;
    private final boolean fsync;
    private final FileChannel log;

    private long term;
    private int votedFor = -1;

    // index tables, 1-based index i lives at slot i-1
    private long[] positions = new long[1024];
    private long[] terms = new long[1024];
    private int count = 0;
    private long fileEnd = 0;
    private boolean dirty = false;

    private final Map<Long, LogEntry> cache = new LinkedHashMap<>(64, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Long, LogEntry> eldest) {
            return size() > CACHE_ENTRIES;
        }
    };

    public FileRaftStorage(Path dir, boolean fsync) {
        this.dir = dir;
        this.fsync = fsync;
        try {
            Files.createDirectories(dir);
            Files.deleteIfExists(dir.resolve("state.tmp"));           // a write that never reached its rename
            loadState();
            this.log = FileChannel.open(dir.resolve("log"), CREATE, READ, WRITE);
            try {
                scanLog();
            } catch (IOException | RuntimeException e) {
                log.close();
                throw e;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ------------------------------------------------------------------ state file

    private void loadState() throws IOException {
        Path state = dir.resolve("state");
        if (!Files.exists(state)) {
            return;                                                  // brand new group: term 0, no vote
        }
        byte[] bytes = Files.readAllBytes(state);
        if (bytes.length != 16) {
            throw corruptState(state, "unexpected length " + bytes.length);
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int storedCrc = buf.getInt();
        CRC32 crc = new CRC32();
        crc.update(bytes, 4, 12);
        if ((int) crc.getValue() != storedCrc) {
            throw corruptState(state, "checksum mismatch");
        }
        term = buf.getLong();
        votedFor = buf.getInt();
    }

    private static IllegalStateException corruptState(Path state, String why) {
        // refuse to guess: a wrong vote or term can make this node vote twice in one term
        return new IllegalStateException("Raft state file " + state + " is corrupt (" + why + "); refusing to start this group");
    }

    @Override
    public long currentTerm() {
        return term;
    }

    @Override
    public int votedFor() {
        return votedFor;
    }

    @Override
    public void setTermAndVote(long newTerm, int newVotedFor) {
        try {
            ByteBuffer buf = ByteBuffer.allocate(16);
            buf.position(4).putLong(newTerm).putInt(newVotedFor);
            CRC32 crc = new CRC32();
            crc.update(buf.array(), 4, 12);
            buf.putInt(0, (int) crc.getValue());
            buf.position(0);

            Path tmp = dir.resolve("state.tmp");
            try (FileChannel ch = FileChannel.open(tmp, CREATE, TRUNCATE_EXISTING, WRITE)) {
                while (buf.hasRemaining()) {
                    ch.write(buf);
                }
                if (fsync) {
                    ch.force(true);                                   // the bytes are on disk BEFORE the rename makes them visible
                }
            }
            Files.move(tmp, dir.resolve("state"), StandardCopyOption.ATOMIC_MOVE);
            if (fsync) {
                syncDirectory();                                      // and the rename itself is durable
            }
            term = newTerm;
            votedFor = newVotedFor;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void syncDirectory() {
        try (FileChannel ch = FileChannel.open(dir, READ)) {
            ch.force(true);
        } catch (IOException | RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG, "could not fsync directory {0}: {1}", dir, e.toString());
        }
    }

    // ------------------------------------------------------------------ log: open and scan

    private void scanLog() throws IOException {
        long size = log.size();
        long pos = 0;
        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        while (pos + 4 <= size) {
            lenBuf.clear();
            readFully(lenBuf, pos);
            int length = lenBuf.getInt(0);
            if (length < FIXED || length > MAX_ENTRY || pos + 4 + length > size) {
                break;                                               // torn or corrupt length
            }
            ByteBuffer frame = ByteBuffer.allocate(length);
            readFully(frame, pos + 4);
            if (!frameIsValid(frame.array(), count + 1)) {
                break;                                               // bad CRC, or an index that does not continue the log
            }
            addToIndex(pos, frame.getLong(4 + 8));
            pos += 4 + length;
        }
        if (pos < size) {
            LOG.log(System.Logger.Level.WARNING, "Raft log {0}: dropping {1} bytes after the last valid entry (index {2})",
                    dir, size - pos, count);
            log.truncate(pos);
            if (fsync) {
                log.force(false);
            }
        }
        fileEnd = pos;
    }

    /** frame = bytes AFTER the length field. */
    private static boolean frameIsValid(byte[] frame, long expectedIndex) {
        ByteBuffer buf = ByteBuffer.wrap(frame);
        int storedCrc = buf.getInt(0);
        CRC32 crc = new CRC32();
        crc.update(frame, 4, frame.length - 4);
        if ((int) crc.getValue() != storedCrc) {
            return false;
        }
        byte type = frame[4 + 8 + 8];
        return buf.getLong(4) == expectedIndex && (type == 0 || type == 1);
    }

    private void addToIndex(long position, long entryTerm) {
        if (count == positions.length) {
            positions = Arrays.copyOf(positions, count * 2);
            terms = Arrays.copyOf(terms, count * 2);
        }
        positions[count] = position;
        terms[count] = entryTerm;
        count++;
    }

    private void readFully(ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int n = log.read(buf, pos);
            if (n < 0) {
                throw new IOException("unexpected end of Raft log file at " + pos);
            }
            pos += n;
        }
    }

    // ------------------------------------------------------------------ log: RaftStorage

    @Override
    public long lastIndex() {
        return count;
    }

    @Override
    public long lastTerm() {
        return count == 0 ? 0 : terms[count - 1];
    }

    @Override
    public long termAt(long index) {
        if (index == 0) {
            return 0;
        }
        checkIndex(index);
        return terms[(int) index - 1];
    }

    private void checkIndex(long index) {
        if (index < 1 || index > count) {
            throw new IllegalArgumentException("no entry at index " + index + " (last index is " + count + ")");
        }
    }

    @Override
    public LogEntry entryAt(long index) {
        checkIndex(index);
        LogEntry cached = cache.get(index);
        if (cached != null) {
            return cached;
        }
        try {
            long pos = positions[(int) index - 1];
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            readFully(lenBuf, pos);
            int length = lenBuf.getInt(0);
            ByteBuffer frame = ByteBuffer.allocate(length);
            readFully(frame, pos + 4);
            if (!frameIsValid(frame.array(), index)) {
                throw new IllegalStateException("Raft log " + dir + " is corrupt at index " + index + " (changed after it was opened)");
            }
            LogEntry entry = decode(frame.array());
            cache.put(index, entry);
            return entry;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static LogEntry decode(byte[] frame) {
        long entryTerm = ByteBuffer.wrap(frame).getLong(4 + 8);
        EntryType type = frame[4 + 8 + 8] == 0 ? EntryType.NOOP : EntryType.COMMAND;
        return new LogEntry(entryTerm, type, Arrays.copyOfRange(frame, FIXED, frame.length));
    }

    @Override
    public List<LogEntry> entries(long fromIndex, int maxCount) {
        if (fromIndex < 1) {
            throw new IllegalArgumentException("fromIndex must be >= 1");
        }
        List<LogEntry> out = new ArrayList<>();
        for (long i = fromIndex; i <= count && out.size() < maxCount; i++) {
            out.add(entryAt(i));
        }
        return out;
    }

    @Override
    public void append(List<LogEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        try {
            int total = 0;
            List<byte[]> payloads = new ArrayList<>(entries.size());
            for (LogEntry e : entries) {
                byte[] p = e.command();
                payloads.add(p);
                total += 4 + FIXED + p.length;
            }
            ByteBuffer buf = ByteBuffer.allocate(total);
            long position = fileEnd;
            int firstSlot = count;
            for (int i = 0; i < entries.size(); i++) {
                LogEntry e = entries.get(i);
                byte[] payload = payloads.get(i);
                long index = (long) count + 1;
                int start = buf.position();
                buf.putInt(FIXED + payload.length).putInt(0).putLong(index).putLong(e.term())
                        .put((byte) (e.type() == EntryType.NOOP ? 0 : 1)).put(payload);
                CRC32 crc = new CRC32();
                crc.update(buf.array(), start + 8, FIXED - 4 + payload.length);
                buf.putInt(start + 4, (int) crc.getValue());
                addToIndex(position + start, e.term());
                cache.put(index, e);
            }
            buf.flip();
            long writeAt = fileEnd;
            while (buf.hasRemaining()) {
                writeAt += log.write(buf, writeAt);
            }
            fileEnd = writeAt;
            dirty = true;
            assert count == firstSlot + entries.size();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void truncateFrom(long index) {
        if (index < 1 || index > count + 1) {
            throw new IllegalArgumentException("cannot truncate from " + index + " (last index is " + count + ")");
        }
        if (index > count) {
            return;
        }
        try {
            long newEnd = positions[(int) index - 1];
            log.truncate(newEnd);
            fileEnd = newEnd;
            count = (int) index - 1;
            cache.keySet().removeIf(k -> k >= index);
            dirty = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Optional metrics: how many real fsyncs happened and how long each took (microseconds). */
    public void instrument(com.ember.metrics.Counter fsyncs, com.ember.metrics.Histogram fsyncMicros) {
        this.fsyncCounter = fsyncs;
        this.fsyncLatency = fsyncMicros;
    }

    private volatile FaultInjector faults = FaultInjector.NONE;
    private volatile com.ember.metrics.Counter fsyncCounter;
    private volatile com.ember.metrics.Histogram fsyncLatency;

    /** TEST HOOK: see FaultInjector. */
    public void setFaultInjector(FaultInjector faults) {
        this.faults = faults == null ? FaultInjector.NONE : faults;
    }

    @Override
    public void sync() {
        if (!dirty) {
            return;
        }
        try {
            if (fsync) {
                long start = System.nanoTime();
                FaultInjector injector = faults;
                if (injector.active() && injector.fsyncDelayNanos() > 0) {      // TEST HOOK (off by default): a slow disk
                    java.util.concurrent.locks.LockSupport.parkNanos(injector.fsyncDelayNanos());
                }
                log.force(false);
                com.ember.metrics.Counter counter = fsyncCounter;
                if (counter != null) {
                    counter.inc();
                    fsyncLatency.record((System.nanoTime() - start) / 1_000);
                }
            }
            dirty = false;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Closes the file without any extra flushing (also what a crash looks like to the files). */
    @Override
    public void close() {
        try {
            log.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Path directory() {
        return dir;
    }
}
