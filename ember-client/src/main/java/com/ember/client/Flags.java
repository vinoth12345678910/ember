package com.ember.client;

import java.util.HashMap;
import java.util.Map;

/** --name=value flags, no library. */
final class Flags {

    private final Map<String, String> values = new HashMap<>();
    private final java.util.Set<String> bareSwitches = new java.util.HashSet<>();   // --follow, given without a value

    Flags(String[] args, int from) {
        for (int i = from; i < args.length; i++) {
            String a = args[i];
            int eq = a.indexOf('=');
            if (a.startsWith("--") && eq < 0 && a.length() > 2) {
                values.put(a.substring(2), "true");                 // bare switch, e.g. --follow
                bareSwitches.add(a.substring(2));
                continue;
            }
            if (!a.startsWith("--") || eq < 3) {
                throw new IllegalArgumentException("expected --name=value but got: " + a);
            }
            values.put(a.substring(2, eq), a.substring(eq + 1));
        }
    }

    /** A bare switch such as --follow (no value) or --follow=true. */
    boolean has(String name) {
        return values.containsKey(name) && !values.get(name).equals("false");
    }

    String get(String name, String fallback) {
        rejectBare(name);
        return values.getOrDefault(name, fallback);
    }

    String require(String name) {
        rejectBare(name);
        String v = values.get(name);
        if (v == null) {
            throw new IllegalArgumentException("missing required flag --" + name);
        }
        return v;
    }

    int getInt(String name, int fallback) {
        rejectBare(name);
        return values.containsKey(name) ? parseInt(name) : fallback;
    }

    long getLong(String name, long fallback) {
        rejectBare(name);
        try {
            return values.containsKey(name) ? Long.parseLong(values.get(name)) : fallback;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + name + " must be a number");
        }
    }

    /** "--topic" without "=value" must not silently become the value "true". */
    private void rejectBare(String name) {
        if (bareSwitches.contains(name)) {
            throw new IllegalArgumentException("--" + name + " needs a value, like --" + name + "=...");
        }
    }

    private int parseInt(String name) {
        try {
            return Integer.parseInt(values.get(name));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + name + " must be a number");
        }
    }
}
