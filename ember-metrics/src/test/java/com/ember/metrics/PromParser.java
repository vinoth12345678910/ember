package com.ember.metrics;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A tiny parser for the Prometheus text format, used only by tests to check that what we print can be read back. */
final class PromParser {

    record Sample(String name, Map<String, String> labels, double value) {
    }

    record Parsed(List<Sample> samples, Map<String, String> types, Map<String, Integer> helpCount, Map<String, Integer> typeCount) {
        List<Sample> named(String name) {
            return samples.stream().filter(s -> s.name().equals(name)).toList();
        }

        double value(String name, String... labelPairs) {
            Map<String, String> want = new LinkedHashMap<>();
            for (int i = 0; i < labelPairs.length; i += 2) want.put(labelPairs[i], labelPairs[i + 1]);
            for (Sample s : samples) {
                if (s.name().equals(name) && s.labels().entrySet().containsAll(want.entrySet())) return s.value();
            }
            throw new AssertionError("no sample " + name + want + " in:\n" + samples);
        }
    }

    static Parsed parse(String text) {
        List<Sample> samples = new ArrayList<>();
        Map<String, String> types = new LinkedHashMap<>();
        Map<String, Integer> helps = new LinkedHashMap<>();
        Map<String, Integer> typeCount = new LinkedHashMap<>();
        for (String line : text.split("\n", -1)) {
            if (line.isEmpty()) continue;
            if (line.startsWith("# HELP ")) {
                helps.merge(line.split(" ", 4)[2], 1, Integer::sum);
            } else if (line.startsWith("# TYPE ")) {
                String[] p = line.split(" ", 4);
                types.put(p[2], p[3]);
                typeCount.merge(p[2], 1, Integer::sum);
            } else if (line.startsWith("#")) {
                throw new AssertionError("unknown comment line: " + line);
            } else {
                samples.add(sample(line));
            }
        }
        return new Parsed(samples, types, helps, typeCount);
    }

    private static Sample sample(String line) {
        int brace = line.indexOf('{');
        int i;
        String name;
        Map<String, String> labels = new LinkedHashMap<>();
        if (brace < 0) {
            int space = line.indexOf(' ');
            name = line.substring(0, space);
            i = space;
        } else {
            name = line.substring(0, brace);
            i = brace + 1;
            while (line.charAt(i) != '}') {
                int eq = line.indexOf('=', i);
                String key = line.substring(i, eq);
                if (line.charAt(eq + 1) != '"') throw new AssertionError("label value must be quoted: " + line);
                StringBuilder value = new StringBuilder();
                int j = eq + 2;
                while (line.charAt(j) != '"') {
                    char c = line.charAt(j);
                    if (c == '\\') {
                        char n = line.charAt(++j);
                        value.append(n == 'n' ? '\n' : n);
                    } else {
                        value.append(c);
                    }
                    j++;
                }
                labels.put(key, value.toString());
                i = j + 1;
                if (line.charAt(i) == ',') i++;
            }
            i++;
        }
        String number = line.substring(i).trim();
        double v = number.equals("+Inf") ? Double.POSITIVE_INFINITY : number.equals("-Inf") ? Double.NEGATIVE_INFINITY : Double.parseDouble(number);
        return new Sample(name, labels, v);
    }
}
