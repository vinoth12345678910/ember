package com.ember.chaos;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** An immutable recorded history, readable from and writable to a file (one JSON object per line). */
public final class History {

    /** An operation: its invoke and (if it ever completed) its complete event. */
    public record Op(HistoryEvent invoke, HistoryEvent complete) {
        public Outcome outcome() {
            return complete == null ? Outcome.INFO : complete.outcome();
        }
    }

    private final List<HistoryEvent> events;

    public History(List<HistoryEvent> events) {
        this.events = List.copyOf(events);
    }

    public List<HistoryEvent> events() {
        return events;
    }

    /** Operations of one type, in the order they were invoked. An operation that never completed is INFO. */
    public List<Op> ops(String type) {
        Map<Long, HistoryEvent> completes = new HashMap<>();
        for (HistoryEvent e : events) {
            if (e.kind().equals(HistoryEvent.COMPLETE) && e.type().equals(type)) completes.put(e.opId(), e);
        }
        List<Op> out = new ArrayList<>();
        for (HistoryEvent e : events) {
            if (e.kind().equals(HistoryEvent.INVOKE) && e.type().equals(type)) out.add(new Op(e, completes.get(e.opId())));
        }
        return out;
    }

    public List<HistoryEvent> ofKind(String kind) {
        return events.stream().filter(e -> e.kind().equals(kind)).toList();
    }

    public void write(Path file) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            for (HistoryEvent e : events) {
                w.write(e.toJson());
                w.newLine();
            }
        }
    }

    public static History read(Path file) throws IOException {
        List<HistoryEvent> events = new ArrayList<>();
        try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = r.readLine()) != null) {
                if (!line.isBlank()) events.add(HistoryEvent.fromJson(line));
            }
        }
        return new History(events);
    }
}
