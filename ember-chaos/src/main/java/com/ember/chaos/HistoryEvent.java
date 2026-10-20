package com.ember.chaos;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One line of the history. kind is invoke | complete | nemesis | replica. An operation is an invoke and a complete with the
 * same opId. data carries the operation-specific fields (see HistoryRecorder for the vocabulary).
 */
public record HistoryEvent(long timeNanos, String kind, long opId, String process, String type, Outcome outcome, Map<String, Object> data) {

    public static final String INVOKE = "invoke";
    public static final String COMPLETE = "complete";
    public static final String NEMESIS = "nemesis";
    public static final String REPLICA = "replica";

    public HistoryEvent {
        data = data == null ? Map.of() : Map.copyOf(data);
    }

    public String toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("t", timeNanos);
        m.put("k", kind);
        m.put("op", opId);
        m.put("p", process);
        m.put("ty", type);
        m.put("o", outcome == null ? null : outcome.name());
        m.put("d", new LinkedHashMap<>(data));
        return Json.write(m);
    }

    @SuppressWarnings("unchecked")
    public static HistoryEvent fromJson(String line) {
        Map<String, Object> m = Json.parseObject(line);
        Object o = m.get("o");
        return new HistoryEvent(((Number) m.get("t")).longValue(), (String) m.get("k"), ((Number) m.get("op")).longValue(),
                (String) m.get("p"), (String) m.get("ty"), o == null ? null : Outcome.valueOf((String) o), (Map<String, Object>) m.get("d"));
    }

    // ---- typed accessors used by the checker

    public String str(String key) {
        Object v = data.get(key);
        return v == null ? null : v.toString();
    }

    public long num(String key) {
        Object v = data.get(key);
        return v == null ? -1 : ((Number) v).longValue();
    }

    public boolean flag(String key) {
        return Boolean.TRUE.equals(data.get(key));
    }

    @SuppressWarnings("unchecked")
    public java.util.List<java.util.List<Object>> rows(String key) {
        Object v = data.get(key);
        return v == null ? java.util.List.of() : (java.util.List<java.util.List<Object>>) v;
    }
}
