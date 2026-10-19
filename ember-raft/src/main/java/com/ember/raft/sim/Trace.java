package com.ember.raft.sim;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * A human-readable log of a run. Keeps a bounded ring buffer of the last lines (for violation
 * reports) and a rolling hash of ALL lines (so two runs can be compared without storing everything).
 * Optionally keeps every line (used when re-running a failing seed to write its full trace file).
 */
public final class Trace {

    private final int ringCapacity;
    private final ArrayDeque<String> ring = new ArrayDeque<>();
    private final List<String> full;
    private long hash = 0xcbf29ce484222325L;
    private long lines = 0;

    public Trace(int ringCapacity, boolean keepEverything) {
        this.ringCapacity = ringCapacity;
        this.full = keepEverything ? new ArrayList<>() : null;
    }

    public void add(long timeMs, String text) {
        StringBuilder sb = new StringBuilder(text.length() + 8);
        sb.append("t=");
        String t = Long.toString(timeMs);
        for (int i = t.length(); i < 5; i++) {
            sb.append('0');
        }
        sb.append(t).append(' ').append(text);
        String line = sb.toString();

        hash = (hash ^ line.hashCode()) * 0x100000001b3L;       // String.hashCode is specified, so this is stable across JVMs
        lines++;
        if (ring.size() == ringCapacity) {
            ring.removeFirst();
        }
        ring.addLast(line);
        if (full != null) {
            full.add(line);
        }
    }

    public List<String> tail(int n) {
        List<String> all = new ArrayList<>(ring);
        return all.subList(Math.max(0, all.size() - n), all.size());
    }

    /** Every line, or null if full recording was not switched on. */
    public List<String> full() {
        return full;
    }

    public long hash() {
        return hash;
    }

    public long lineCount() {
        return lines;
    }
}
