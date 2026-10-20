package com.ember.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** An immutable, sorted set of label pairs. Equality is by content, so it is a safe map key. */
public final class Labels implements Comparable<Labels> {

    public static final Labels NONE = new Labels(new String[0]);

    private final String[] pairs;          // k0, v0, k1, v1 ... sorted by key
    private final int hash;

    private Labels(String[] pairs) {
        this.pairs = pairs;
        this.hash = java.util.Arrays.hashCode(pairs);
    }

    /** Labels.of("topic", "orders", "partition", "3") */
    public static Labels of(String... keyValues) {
        if (keyValues.length == 0) {
            return NONE;
        }
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("labels come in key,value pairs");
        }
        Map<String, String> sorted = new TreeMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            String key = keyValues[i];
            if (!MetricsRegistry.isValidLabelName(key)) {
                throw new IllegalArgumentException("invalid label name: " + key);
            }
            if (sorted.put(key, String.valueOf(keyValues[i + 1])) != null) {
                throw new IllegalArgumentException("duplicate label: " + key);
            }
        }
        String[] flat = new String[sorted.size() * 2];
        int i = 0;
        for (Map.Entry<String, String> e : sorted.entrySet()) {
            flat[i++] = e.getKey();
            flat[i++] = e.getValue();
        }
        return new Labels(flat);
    }

    public int size() {
        return pairs.length / 2;
    }

    public String key(int i) {
        return pairs[2 * i];
    }

    public String value(int i) {
        return pairs[2 * i + 1];
    }

    public List<String[]> asList() {
        List<String[]> out = new ArrayList<>();
        for (int i = 0; i < size(); i++) {
            out.add(new String[]{key(i), value(i)});
        }
        return Collections.unmodifiableList(out);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Labels l && java.util.Arrays.equals(pairs, l.pairs);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public int compareTo(Labels other) {
        return java.util.Arrays.compare(pairs, other.pairs);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < size(); i++) {
            sb.append(i > 0 ? "," : "").append(key(i)).append('=').append(value(i));
        }
        return sb.append('}').toString();
    }
}
