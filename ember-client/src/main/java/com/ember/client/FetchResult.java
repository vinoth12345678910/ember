package com.ember.client;

import com.ember.storage.LogRecord;

import java.util.List;

/** fetchOffset is the offset that was asked for; it is what to ask again when nothing came back. */
public record FetchResult(long fetchOffset, long logStartOffset, long highWatermark, List<LogRecord> records) {

    /** Where to continue: just after the last record, or the same offset if the response was empty. */
    public long nextFetchOffset() {
        return records.isEmpty() ? fetchOffset : records.get(records.size() - 1).offset() + 1;
    }
}
