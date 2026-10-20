package com.ember.broker;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A tiny JSON writer for /status (no library). Usage: w.beginObject().name("a").value(1).name("b").beginArray()...
 * It only writes; commas and nesting are tracked here so callers cannot produce broken JSON by forgetting one.
 */
final class JsonWriter {

    private final StringBuilder out = new StringBuilder();
    private final Deque<boolean[]> stack = new ArrayDeque<>();   // per open container: {hasElements, expectingValueAfterName}

    JsonWriter beginObject() {
        beforeValue();
        out.append('{');
        stack.push(new boolean[]{false, false});
        return this;
    }

    JsonWriter endObject() {
        stack.pop();
        out.append('}');
        return this;
    }

    JsonWriter beginArray() {
        beforeValue();
        out.append('[');
        stack.push(new boolean[]{false, false});
        return this;
    }

    JsonWriter endArray() {
        stack.pop();
        out.append(']');
        return this;
    }

    JsonWriter name(String name) {
        boolean[] top = stack.peek();
        if (top == null) {
            throw new IllegalStateException("a name needs an open object");
        }
        if (top[0]) {
            out.append(',');
        }
        top[0] = true;
        top[1] = true;
        string(name);
        out.append(':');
        return this;
    }

    JsonWriter value(String v) {
        beforeValue();
        if (v == null) {
            out.append("null");
        } else {
            string(v);
        }
        return this;
    }

    JsonWriter value(long v) {
        beforeValue();
        out.append(v);
        return this;
    }

    JsonWriter value(double v) {
        beforeValue();
        out.append(Double.isNaN(v) || Double.isInfinite(v) ? "null" : Double.toString(v));
        return this;
    }

    JsonWriter value(boolean v) {
        beforeValue();
        out.append(v);
        return this;
    }

    private void beforeValue() {
        boolean[] top = stack.peek();
        if (top == null) {
            return;
        }
        if (top[1]) {
            top[1] = false;                      // the value that follows a name: the comma was written by name()
            return;
        }
        if (top[0]) {
            out.append(',');
        }
        top[0] = true;
    }

    private void string(String s) {
        out.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }

    @Override
    public String toString() {
        if (!stack.isEmpty()) {
            throw new IllegalStateException("unclosed JSON container");
        }
        return out.toString();
    }
}
