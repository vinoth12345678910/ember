package com.ember.web;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Forwards the "Live Fleet" page's calls to the Fleet demo process (demo/fleet/bin/live.js), which listens on localhost only and has no
 * login of its own. The console checks the user is an administrator first; this class only moves bytes.
 */
final class FleetProxy {

    record Result(int status, String body) {
    }

    private final String base;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    FleetProxy(String baseUrl) {
        this.base = baseUrl == null ? null : baseUrl.replaceAll("/+$", "");
    }

    Result get(String path) {
        return call(base == null ? null : HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(5)).GET().build());
    }

    Result post(String path, String body) {
        return call(base == null ? null : HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body == null ? "{}" : body, StandardCharsets.UTF_8)).build());
    }

    private Result call(HttpRequest request) {
        if (request == null) {
            return new Result(503, "{\"error\":\"the Fleet demo is not configured (start the console with --fleet-url=http://127.0.0.1:8099)\"}");
        }
        try {
            HttpResponse<String> r = http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            return new Result(r.statusCode(), r.body());
        } catch (IOException e) {
            return new Result(503, "{\"error\":\"the Fleet demo is not running\",\"hint\":\"start it with: node demo/fleet/bin/live.js\"}");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Result(503, "{\"error\":\"interrupted\"}");
        }
    }
}
