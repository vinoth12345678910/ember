package com.ember.broker;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A strict little JSON parser for tests: objects -> Map, arrays -> List, numbers -> Double, plus String / Boolean / null. */
final class TestJson {

    private final String text;
    private int pos;

    private TestJson(String text) {
        this.text = text;
    }

    static Object parse(String text) {
        TestJson p = new TestJson(text);
        Object value = p.value();
        p.skipSpace();
        if (p.pos != text.length()) throw new IllegalArgumentException("trailing characters at " + p.pos + " in: " + text);
        return value;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> object(String text) {
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
                if (d != ',') throw new IllegalArgumentException("expected ',' or '}' at " + (pos - 1) + " in: " + text);
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
                if (d != ',') throw new IllegalArgumentException("expected ',' or ']' at " + (pos - 1) + " in: " + text);
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
        return Double.parseDouble(text.substring(start, pos));
    }

    private String string() {
        if (next() != '"') throw new IllegalArgumentException("expected a string at " + (pos - 1) + " in: " + text);
        StringBuilder sb = new StringBuilder();
        while (true) {
            char c = next();
            if (c == '"') return sb.toString();
            if (c < 0x20) throw new IllegalArgumentException("raw control character in a JSON string");
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
