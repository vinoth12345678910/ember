package com.ember.chaos;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** Reads the admin HTTP endpoints of a broker (metrics, health, status, checksum). */
public final class AdminClient {

    public record Response(int status, String body) {
    }

    private static final HttpClient CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    private AdminClient() {
    }

    /** address is host:port */
    public static Response get(String address, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + address + path)).timeout(Duration.ofSeconds(10)).GET().build();
        HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        return new Response(response.statusCode(), response.body());
    }

    public static boolean ready(String address) {
        try {
            return get(address, "/health/ready").status() == 200;
        } catch (Exception e) {
            return false;
        }
    }
}
