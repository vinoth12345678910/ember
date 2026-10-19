package com.ember.replication;

import com.ember.protocol.ProducerRecord;
import com.ember.raft.EntryType;
import com.ember.raft.LogEntry;
import com.ember.storage.KeyValue;
import com.ember.storage.Log;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes committed PRODUCE_BATCH entries into the partition's DATA log, idempotently.
 *
 * After a restart the node replays committed entries from index 1, so runningOffset (records contained in the
 * entries applied so far) starts at 0 while the data log may already hold all, part, or none of them.
 * Counting records by position tells us exactly what is missing.
 */
public final class PartitionStateMachine implements StateMachine {

    private final String name;
    private final Log dataLog;
    private long runningOffset = 0;

    public PartitionStateMachine(String name, Log dataLog) {
        this.name = name;
        this.dataLog = dataLog;
    }

    @Override
    public Object apply(long index, LogEntry entry) {
        if (entry.type() == EntryType.NOOP) {
            return null;                                           // a leader's no-op carries no records
        }
        Commands.ProduceBatch batch = Commands.decodeProduceBatch(entry.command());
        List<ProducerRecord> records = batch.records();
        int n = records.size();
        try {
            long next = dataLog.nextOffset();
            if (runningOffset + n <= next) {
                // the whole batch is already in the data log (we crashed after writing it): do not write it twice
            } else if (runningOffset == next) {
                appendAll(batch.timestamp(), records);
            } else if (runningOffset < next) {
                // only part of the batch survived the crash: append exactly the missing tail
                appendAll(batch.timestamp(), records.subList((int) (next - runningOffset), n));
            } else {
                throw new IllegalStateException("partition " + name + ": the data log ends at " + next
                        + " but the applied entries already contain " + runningOffset + " records; the data log lost more than a torn tail");
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        long baseOffset = runningOffset;
        runningOffset += n;
        return baseOffset;
    }

    private void appendAll(long timestamp, List<ProducerRecord> records) throws IOException {
        List<KeyValue> kvs = new ArrayList<>(records.size());
        for (ProducerRecord r : records) {
            kvs.add(new KeyValue(r.key(), r.value()));
        }
        long expectedBase = dataLog.nextOffset();
        long base = dataLog.appendAll(timestamp, kvs);
        if (base != expectedBase) {
            throw new IllegalStateException("partition " + name + ": appended at " + base + " but expected " + expectedBase);
        }
    }

    public long runningOffset() {
        return runningOffset;
    }
}
