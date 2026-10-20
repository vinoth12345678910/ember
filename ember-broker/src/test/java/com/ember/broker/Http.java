package com.ember.broker;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** HTTP and Prometheus-text helpers for the admin tests. */
final class Http {

    record Response(int status, String body) {
    }

    record Sample(String name, Map<String, String> labels, double value) {
    }

    private static final HttpClient CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private static final Pattern LINE = Pattern.compile("^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\\{(.*)\\})? (\\S+)$");
    private static final Pattern LABEL = Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)=\"((?:[^\"\\\\]|\\\\.)*)\"");

    private Http() {
    }

    static Response get(int port, String path) throws Exception {
        return send(port, path, "GET");
    }

    static Response send(int port, String path, String method) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(10)).method(method, HttpRequest.BodyPublishers.noBody()).build();
        HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        return new Response(response.statusCode(), response.body());
    }

    static List<Sample> scrape(int port) throws Exception {
        Response r = get(port, "/metrics");
        if (r.status() != 200) throw new AssertionError("/metrics answered " + r.status());
        List<Sample> out = new ArrayList<>();
        for (String line : r.body().split("\n")) {
            if (line.isEmpty() || line.startsWith("#")) continue;
            Matcher m = LINE.matcher(line);
            if (!m.matches()) throw new AssertionError("unparsable metrics line: " + line);
            Map<String, String> labels = new LinkedHashMap<>();
            if (m.group(2) != null) {
                Matcher l = LABEL.matcher(m.group(2));
                while (l.find()) labels.put(l.group(1), l.group(2).replace("\\n", "\n").replace("\\\"", "\"").replace("\\\\", "\\"));
            }
            String v = m.group(3);
            out.add(new Sample(m.group(1), labels, v.equals("+Inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(v)));
        }
        return out;
    }

    /** The value of the single sample with this name whose labels include all the given pairs; NaN if absent. */
    static double value(List<Sample> samples, String name, String... labelPairs) {
        for (Sample s : samples) {
            if (!s.name().equals(name)) continue;
            boolean all = true;
            for (int i = 0; i < labelPairs.length; i += 2) {
                if (!labelPairs[i + 1].equals(s.labels().get(labelPairs[i]))) all = false;
            }
            if (all) return s.value();
        }
        return Double.NaN;
    }

    static double value(int port, String name, String... labelPairs) throws Exception {
        return value(scrape(port), name, labelPairs);
    }
}
