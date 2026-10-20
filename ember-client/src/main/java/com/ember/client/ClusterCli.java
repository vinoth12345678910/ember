package com.ember.client;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/** `cluster status`, `cluster metrics`, `healthcheck`: read-only views over the brokers' admin HTTP endpoints. */
final class ClusterCli {

    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    private record Reply(int status, String body) {
    }

    private ClusterCli() {
    }

    private static Reply get(String address, String path) throws IOException {
        try {
            HttpResponse<String> r = HTTP.send(HttpRequest.newBuilder(URI.create("http://" + address + path)).timeout(Duration.ofSeconds(5)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            return new Reply(r.statusCode(), r.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", e);
        }
    }

    private static List<String> admins(Flags flags) {
        List<String> out = new ArrayList<>();
        for (String a : flags.require("admin").split(",")) {
            if (!a.matches(".+:\\d+")) throw new IllegalArgumentException("--admin entries look like host:port, got: " + a);
            out.add(a.trim());
        }
        return out;
    }

    static void run(String sub, Flags flags, PrintStream out) throws IOException {
        switch (sub) {
            case "status" -> status(admins(flags), out);
            case "metrics" -> metrics(admins(flags), flags.get("filter", ""), out);
            default -> throw new IllegalArgumentException("cluster subcommands: status, metrics");
        }
    }

    @SuppressWarnings("unchecked")
    private static void status(List<String> admins, PrintStream out) {
        Map<String, Map<String, String>> cells = new TreeMap<>();          // group -> admin address -> "role/term"
        Map<String, String> leaderOf = new TreeMap<>();
        out.printf(Locale.ROOT, "%-5s %-22s %-14s %-9s %-11s %-11s%n", "NODE", "ADMIN", "STATE", "UPTIME", "CONTROLLER", "COORDINATOR");
        int row = 0;
        for (String admin : admins) {
            row++;
            try {
                Reply r = get(admin, "/status");
                Map<String, Object> s = MiniJson.parseObject(r.body());
                boolean ready = Boolean.TRUE.equals(s.get("ready"));
                int node = ((Number) s.get("nodeId")).intValue();
                out.printf(Locale.ROOT, "%-5d %-22s %-14s %-9s %-11s %-11s%n", node, admin, ready ? "ready" : "NOT READY",
                        String.format(Locale.ROOT, "%.0fs", ((Number) s.get("uptimeSeconds")).doubleValue()), id(s.get("controllerId")), id(s.get("coordinatorId")));
                Object groups = s.get("raftGroups");
                if (groups instanceof List<?> list) {
                    for (Object g : list) {
                        Map<String, Object> group = (Map<String, Object>) g;
                        String name = group.get("group").toString();
                        String role = group.get("role").toString().toLowerCase(Locale.ROOT);
                        cells.computeIfAbsent(name, k -> new TreeMap<>()).put("node " + node, role + "/" + ((Number) group.get("term")).longValue());
                        if (role.equals("leader")) leaderOf.put(name, Integer.toString(node));
                    }
                }
            } catch (Exception e) {
                out.printf(Locale.ROOT, "%-5s %-22s %-14s%n", "?", admin, "UNREACHABLE");
            }
        }
        if (cells.isEmpty()) {
            return;
        }
        TreeSet<String> columns = new TreeSet<>();
        cells.values().forEach(m -> columns.addAll(m.keySet()));
        out.println();
        StringBuilder header = new StringBuilder(String.format(Locale.ROOT, "%-14s", "RAFT GROUP"));
        for (String c : columns) header.append(String.format(Locale.ROOT, " %-14s", c));
        out.println(header + " LEADER");
        for (Map.Entry<String, Map<String, String>> g : cells.entrySet()) {
            StringBuilder line = new StringBuilder(String.format(Locale.ROOT, "%-14s", g.getKey()));
            for (String c : columns) line.append(String.format(Locale.ROOT, " %-14s", g.getValue().getOrDefault(c, "-")));
            out.println(line + " " + leaderOf.getOrDefault(g.getKey(), "none"));
        }
    }

    private static String id(Object o) {
        int v = o == null ? -1 : ((Number) o).intValue();
        return v < 0 ? "none" : Integer.toString(v);
    }

    private static void metrics(List<String> admins, String filter, PrintStream out) throws IOException {
        for (String admin : admins) {
            Reply r = get(admin, "/metrics");
            if (r.status() != 200) {
                throw new IOException(admin + " answered " + r.status() + " for /metrics");
            }
            if (admins.size() > 1) out.println("# " + admin);
            for (String line : r.body().split("\n")) {
                if (!line.isEmpty() && !line.startsWith("#") && line.startsWith(filter)) out.println(line);
            }
        }
    }

    /** Exit code 0 only if /health/ready answers 200. For the Docker HEALTHCHECK (the runtime image has no curl). */
    static int healthcheck(Flags flags, PrintStream out, PrintStream err) {
        String admin = flags.get("admin", "localhost:8080");
        try {
            Reply r = get(admin, "/health/ready");
            if (r.status() == 200) {
                out.println("ready");
                return 0;
            }
            err.println("not ready (" + r.status() + "): " + r.body().trim());
            return 1;
        } catch (IOException e) {
            err.println("unreachable: " + e);
            return 1;
        }
    }
}
