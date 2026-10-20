package com.ember.chaos;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Records every client operation as two events (invoke, complete), plus nemesis and replica-state events. Thread-safe. If a
 * file is given, every event is also appended to it at once, so a run that dies still leaves a history to check offline.
 *
 * Vocabulary of data fields (all values JSON-friendly):
 *   produce  invoke: topic, partition, key, value          complete: offset, retried, error
 *   read     invoke: topic, partition, from, final         complete: records [[offset,key,value]...], start, end, error
 *   poll     invoke: consumer, group                       complete: records [[topic,partition,offset,value]...], generation, joinCount
 *   commit   invoke: group, member, generation, offsets [[topic,partition,offset]...]    complete: error
 *   nemesis  (single event): fault, target, phase=start|stop
 *   replica  (single event): scope=data|raftlog|groupstate|metastate, group, replica, ...digest fields
 */
public final class HistoryRecorder implements AutoCloseable {

    private final AtomicLong ids = new AtomicLong();
    private final long originNanos = System.nanoTime();
    private final List<HistoryEvent> events = new ArrayList<>();
    private final BufferedWriter file;

    public HistoryRecorder() {
        this.file = null;
    }

    public HistoryRecorder(Path fileToAppend) throws IOException {
        this.file = Files.newBufferedWriter(fileToAppend, StandardCharsets.UTF_8);
    }

    private long now() {
        return System.nanoTime() - originNanos;
    }

    private synchronized void add(HistoryEvent e) {
        events.add(e);
        if (file != null) {
            try {
                file.write(e.toJson());
                file.newLine();
                file.flush();
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
    }

    public long invoke(String process, String type, Map<String, Object> data) {
        long id = ids.incrementAndGet();
        add(new HistoryEvent(now(), HistoryEvent.INVOKE, id, process, type, null, data));
        return id;
    }

    public void complete(long opId, String process, String type, Outcome outcome, Map<String, Object> data) {
        add(new HistoryEvent(now(), HistoryEvent.COMPLETE, opId, process, type, outcome, data));
    }

    public void nemesis(String fault, String target, String phase) {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("fault", fault);
        d.put("target", target);
        d.put("phase", phase);
        add(new HistoryEvent(now(), HistoryEvent.NEMESIS, ids.incrementAndGet(), "nemesis", "nemesis", null, d));
    }

    public void replica(Map<String, Object> data) {
        add(new HistoryEvent(now(), HistoryEvent.REPLICA, ids.incrementAndGet(), "collector", "replica", null, data));
    }

    public synchronized History snapshot() {
        return new History(events);
    }

    @Override
    public synchronized void close() throws IOException {
        if (file != null) {
            file.close();
        }
    }

    /** Convenience for building a data map: data("topic", "t", "partition", 0). */
    public static Map<String, Object> data(Object... keyValues) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            m.put((String) keyValues[i], keyValues[i + 1]);
        }
        return m;
    }
}
