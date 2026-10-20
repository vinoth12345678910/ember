package com.ember.client;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A strict little JSON reader for the admin endpoints (objects, arrays, strings, numbers, booleans, null). */
final class MiniJson {

    private final String text;
    private int pos;

    private MiniJson(String text) {
        this.text = text;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> parseObject(String text) {
        MiniJson p = new MiniJson(text);
        Object v = p.value();
        p.skipSpace();
        if (p.pos != text.length() || !(v instanceof Map)) {
            throw new IllegalArgumentException("not a JSON object: " + text);
        }
        return (Map<String, Object>) v;
    }

    private void skipSpace() {
        while (pos < text.length() && Character.isWhitespace(text.charAt(pos))) pos++;
    }

    private char next() {
        if (pos >= text.length()) throw new IllegalArgumentException("unexpected end of JSON");
        return text.charAt(pos++);
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
                skipSpace();
                if (next() != ':') throw new IllegalArgumentException("expected ':' at " + (pos - 1));
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
        if (start == pos) throw new IllegalArgumentException("bad JSON value at " + pos);
        return Double.parseDouble(text.substring(start, pos));
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
                    case 'u' -> {
                        sb.append((char) Integer.parseInt(text.substring(pos, pos + 4), 16));
                        pos += 4;
                    }
                    default -> sb.append(e);
                }
            } else {
                sb.append(c);
            }
        }
    }
}
