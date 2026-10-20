package com.ember.chaos;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A small JSON writer and parser (no library) for the history file: one JSON object per line. Values are Map, List, String,
 * Long, Double, Boolean or null. The parser is strict: any trailing garbage is an error.
 */
public final class Json {

    private final String text;
    private int pos;

    private Json(String text) {
        this.text = text;
    }

    // ------------------------------------------------------------------ writing

    public static String write(Object value) {
        StringBuilder sb = new StringBuilder();
        write(sb, value);
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static void write(StringBuilder sb, Object v) {
        if (v == null) {
            sb.append("null");
        } else if (v instanceof String s) {
            string(sb, s);
        } else if (v instanceof Boolean || v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
            sb.append(v);
        } else if (v instanceof Double || v instanceof Float) {
            double d = ((Number) v).doubleValue();
            sb.append(Double.isNaN(d) || Double.isInfinite(d) ? "null" : Double.toString(d));
        } else if (v instanceof Map<?, ?> m) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : ((Map<Object, Object>) m).entrySet()) {
                if (!first) sb.append(',');
                first = false;
                string(sb, String.valueOf(e.getKey()));
                sb.append(':');
                write(sb, e.getValue());
            }
            sb.append('}');
        } else if (v instanceof Iterable<?> list) {
            sb.append('[');
            boolean first = true;
            for (Object o : list) {
                if (!first) sb.append(',');
                first = false;
                write(sb, o);
            }
            sb.append(']');
        } else {
            throw new IllegalArgumentException("cannot write " + v.getClass());
        }
    }

    private static void string(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        sb.append('"');
    }

    // ------------------------------------------------------------------ parsing

    public static Object parse(String text) {
        Json p = new Json(text);
        Object v = p.value();
        p.skipSpace();
        if (p.pos != text.length()) {
            throw new IllegalArgumentException("trailing characters at " + p.pos + " in: " + text);
        }
        return v;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseObject(String text) {
        return (Map<String, Object>) parse(text);
    }

    private void skipSpace() {
        while (pos < text.length() && Character.isWhitespace(text.charAt(pos))) pos++;
    }

    private char next() {
        if (pos >= text.length()) throw new IllegalArgumentException("unexpected end of JSON: " + text);
        return text.charAt(pos++);
    }

    private void expect(char c) {
        skipSpace();
        if (next() != c) throw new IllegalArgumentException("expected '" + c + "' at " + (pos - 1) + " in: " + text);
    }

    private Object value() {
        skipSpace();
        if (pos >= text.length()) throw new IllegalArgumentException("unexpected end of JSON: " + text);
        char c = text.charAt(pos);
        if (c == '{') {
            pos++;
            Map<String, Object> map = new LinkedHashMap<>();
            skipSpace();
            if (text.charAt(pos) == '}') {
                pos++;
                return map;
            }
            while (true) {
                skipSpace();
                String key = string();
                expect(':');
                map.put(key, value());
                skipSpace();
                char d = next();
                if (d == '}') return map;
                if (d != ',') throw new IllegalArgumentException("expected ',' or '}' at " + (pos - 1));
            }
        }
        if (c == '[') {
            pos++;
            List<Object> list = new ArrayList<>();
            skipSpace();
            if (text.charAt(pos) == ']') {
                pos++;
                return list;
            }
            while (true) {
                list.add(value());
                skipSpace();
                char d = next();
                if (d == ']') return list;
                if (d != ',') throw new IllegalArgumentException("expected ',' or ']' at " + (pos - 1));
            }
        }
        if (c == '"') return string();
        if (text.startsWith("true", pos)) {
            pos += 4;
            return Boolean.TRUE;
        }
        if (text.startsWith("false", pos)) {
            pos += 5;
            return Boolean.FALSE;
        }
        if (text.startsWith("null", pos)) {
            pos += 4;
            return null;
        }
        int start = pos;
        while (pos < text.length() && "+-0123456789.eE".indexOf(text.charAt(pos)) >= 0) pos++;
        if (start == pos) throw new IllegalArgumentException("bad JSON value at " + pos + " in: " + text);
        String number = text.substring(start, pos);
        if (number.indexOf('.') < 0 && number.indexOf('e') < 0 && number.indexOf('E') < 0) {
            return Long.parseLong(number);
        }
        return Double.parseDouble(number);
    }

    private String string() {
        if (next() != '"') throw new IllegalArgumentException("expected a string at " + (pos - 1));
        StringBuilder sb = new StringBuilder();
        while (true) {
            char c = next();
            if (c == '"') return sb.toString();
            if (c == '\\') {
                char e = next();
                switch (e) {
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case '"', '\\', '/' -> sb.append(e);
                    case 'u' -> {
                        sb.append((char) Integer.parseInt(text.substring(pos, pos + 4), 16));
                        pos += 4;
                    }
                    default -> throw new IllegalArgumentException("bad escape \\" + e);
                }
            } else {
                sb.append(c);
            }
        }
    }
}
