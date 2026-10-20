package com.ember.chaos;

import java.util.ArrayList;
import java.util.List;

import static com.ember.chaos.HistoryRecorder.data;

/** A tiny DSL over HistoryRecorder for writing histories by hand in tests. */
final class H {

    final HistoryRecorder rec = new HistoryRecorder();

    long produce(String process, String topic, int partition, String key, String value, Outcome outcome, long offset) {
        long id = rec.invoke(process, "produce", data("topic", topic, "partition", partition, "key", key, "value", value));
        rec.complete(id, process, "produce", outcome, data("offset", offset, "retried", false));
        return id;
    }

    long produceRetried(String process, String topic, int partition, String key, String value, long offset) {
        long id = rec.invoke(process, "produce", data("topic", topic, "partition", partition, "key", key, "value", value));
        rec.complete(id, process, "produce", Outcome.OK, data("offset", offset, "retried", true));
        return id;
    }

    /** rows: {offset, key, value} */
    void read(String process, String topic, int partition, boolean isFinal, long start, long end, Object[]... rows) {
        long id = rec.invoke(process, "read", data("topic", topic, "partition", partition, "from", start, "final", isFinal));
        List<List<Object>> records = new ArrayList<>();
        for (Object[] r : rows) records.add(List.of(r[0], r[1] == null ? "" : r[1], r[2]));
        rec.complete(id, process, "read", Outcome.OK, data("records", records, "start", start, "end", end));
    }

    void poll(String consumer, long generation, Object[]... rows) {       // rows: {topic, partition, offset, value}
        long id = rec.invoke(consumer, "poll", data("consumer", consumer, "group", "g"));
        List<List<Object>> records = new ArrayList<>();
        for (Object[] r : rows) records.add(List.of(r));
        rec.complete(id, consumer, "poll", Outcome.OK, data("records", records, "generation", generation, "joinCount", 1));
    }

    void commit(String member, long generation, Outcome outcome, Object[]... offsets) {   // offsets: {topic, partition, offset}
        List<List<Object>> rows = new ArrayList<>();
        for (Object[] r : offsets) rows.add(List.of(r));
        long id = rec.invoke(member, "commit", data("group", "g", "member", member, "generation", generation, "offsets", rows));
        rec.complete(id, member, "commit", outcome, data());
    }

    void replicaData(String group, int replica, long start, long end, long crc) {
        rec.replica(data("scope", "data", "group", group, "replica", Integer.toString(replica), "start", start, "end", end, "crc", crc));
    }

    void replicaRaft(String group, int replica, long index, long digest, String terms) {
        rec.replica(data("scope", "raftlog", "group", group, "replica", Integer.toString(replica), "index", index, "digest", digest, "terms", terms));
    }

    void replicaState(String scope, int replica, long digest) {
        rec.replica(data("scope", scope, "group", scope, "replica", Integer.toString(replica), "digest", digest));
    }

    History history() {
        return rec.snapshot();
    }

    static Object[] row(long offset, String key, String value) {
        return new Object[]{offset, key, value};
    }
}
