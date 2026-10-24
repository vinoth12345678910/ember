package com.ember.web;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Minimal JSON: parse into Map/List/String/Double/Boolean/null, and write the same types (plus Number) back out. */
final class Json {

    private final String s;
    private int pos;

    private Json(String s) {
        this.s = s;
    }

    static Object parse(String text) {
        Json p = new Json(text);
        Object v = p.value();
        p.ws();
        if (p.pos != text.length()) throw new IllegalArgumentException("trailing characters in JSON");
        return v;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> parseObject(String text) {
        Object v = parse(text);
        if (!(v instanceof Map)) throw new IllegalArgumentException("expected a JSON object");
        return (Map<String, Object>) v;
    }

    private void ws() {
        while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) pos++;
    }

    private char peek() {
        if (pos >= s.length()) throw new IllegalArgumentException("unexpected end of JSON");
        return s.charAt(pos);
    }

    private Object value() {
        ws();
        char c = peek();
        if (c == '{') {
            pos++;
            Map<String, Object> m = new LinkedHashMap<>();
            ws();
            if (peek() == '}') { pos++; return m; }
            while (true) {
                ws();
                String k = string();
                ws();
                expect(':');
                m.put(k, value());
                ws();
                char d = s.charAt(pos++);
                if (d == '}') return m;
                if (d != ',') throw new IllegalArgumentException("expected , or } at " + (pos - 1));
            }
        }
        if (c == '[') {
            pos++;
            List<Object> l = new ArrayList<>();
            ws();
            if (peek() == ']') { pos++; return l; }
            while (true) {
                l.add(value());
                ws();
                char d = s.charAt(pos++);
                if (d == ']') return l;
                if (d != ',') throw new IllegalArgumentException("expected , or ] at " + (pos - 1));
            }
        }
        if (c == '"') return string();
        if (s.startsWith("true", pos)) { pos += 4; return Boolean.TRUE; }
        if (s.startsWith("false", pos)) { pos += 5; return Boolean.FALSE; }
        if (s.startsWith("null", pos)) { pos += 4; return null; }
        int start = pos;
        while (pos < s.length() && "+-0123456789.eE".indexOf(s.charAt(pos)) >= 0) pos++;
        if (start == pos) throw new IllegalArgumentException("unexpected character at " + pos);
        return Double.parseDouble(s.substring(start, pos));
    }

    private void expect(char c) {
        if (pos >= s.length() || s.charAt(pos++) != c) throw new IllegalArgumentException("expected '" + c + "' at " + (pos - 1));
    }

    private String string() {
        expect('"');
        StringBuilder b = new StringBuilder();
        while (true) {
            if (pos >= s.length()) throw new IllegalArgumentException("unterminated string");
            char c = s.charAt(pos++);
            if (c == '"') return b.toString();
            if (c != '\\') { b.append(c); continue; }
            char e = s.charAt(pos++);
            switch (e) {
                case 'n' -> b.append('\n');
                case 't' -> b.append('\t');
                case 'r' -> b.append('\r');
                case 'b' -> b.append('\b');
                case 'f' -> b.append('\f');
                case 'u' -> { b.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16)); pos += 4; }
                default -> b.append(e);
            }
        }
    }

    // ------------------------------------------------------------------ writing

    static String write(Object o) {
        StringBuilder b = new StringBuilder();
        write(b, o);
        return b.toString();
    }

    private static void write(StringBuilder b, Object o) {
        if (o == null) b.append("null");
        else if (o instanceof String str) quote(b, str);
        else if (o instanceof Boolean || o instanceof Integer || o instanceof Long) b.append(o);
        else if (o instanceof Number n) {
            double d = n.doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) b.append("null");
            else if (d == Math.rint(d) && Math.abs(d) < 1e15) b.append((long) d);
            else b.append(d);
        } else if (o instanceof Map<?, ?> m) {
            b.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!first) b.append(',');
                first = false;
                quote(b, String.valueOf(e.getKey()));
                b.append(':');
                write(b, e.getValue());
            }
            b.append('}');
        } else if (o instanceof Iterable<?> it) {
            b.append('[');
            boolean first = true;
            for (Object x : it) {
                if (!first) b.append(',');
                first = false;
                write(b, x);
            }
            b.append(']');
        } else quote(b, o.toString());
    }

    private static void quote(StringBuilder b, String str) {
        b.append('"');
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                default -> {
                    if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
                    else b.append(c);
                }
            }
        }
        b.append('"');
    }

    /** Builds an ordered map from alternating keys and values. */
    static Map<String, Object> obj(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }
}
