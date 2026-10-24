package com.ember.web;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** The Live Fleet page's proxy: administrators only, bytes passed through, and a clear answer when the demo is not running. No broker is needed. */
class FleetProxyTest {

    HttpServer fleet;
    WebServer web;
    BrokerGateway gateway;
    final AtomicReference<String> lastPost = new AtomicReference<>();
    final HttpClient http = HttpClient.newHttpClient();
    String adminToken, consumerToken, base;

    @BeforeEach
    void start() throws Exception {
        fleet = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fleet.createContext("/state", ex -> reply(ex, 200, "{\"uptimeSec\":7,\"drivers\":[[12.9,77.5,0]]}"));
        fleet.createContext("/control", ex -> {
            lastPost.set(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            reply(ex, lastPost.get().contains("boom") ? 400 : 200, lastPost.get().contains("boom") ? "{\"error\":\"unknown action\"}" : "{\"ok\":true}");
        });
        fleet.createContext("/compare", ex -> reply(ex, 200, "{\"available\":false,\"running\":false,\"result\":null}"));
        fleet.start();
        UserStore users = new UserStore(null);
        users.create("boss", "Boss", "password1", UserStore.Role.ADMIN, 1);
        users.create("bob", "Bob", "password2", UserStore.Role.CONSUMER, 1);
        Tokens tokens = new Tokens("secret-secret-secret".getBytes(), 600_000);
        adminToken = tokens.issue("boss", System.currentTimeMillis());
        consumerToken = tokens.issue("bob", System.currentTimeMillis());
        gateway = new BrokerGateway(List.of("localhost:1"), List.of());
        web = new WebServer("127.0.0.1", 0, users, new AppStore(null), tokens, gateway, Clock.systemUTC(), new FleetProxy("http://127.0.0.1:" + fleet.getAddress().getPort()));
        base = "http://127.0.0.1:" + web.port();
    }

    @AfterEach
    void stop() throws Exception {
        web.close();
        gateway.close();
        fleet.stop(0);
    }

    private static void reply(com.sun.net.httpserver.HttpExchange ex, int status, String body) throws java.io.IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, b.length);
        ex.getResponseBody().write(b);
        ex.close();
    }

    private HttpResponse<String> call(String method, String path, String token, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path));
        if (token != null) b.header("Authorization", "Bearer " + token);
        return http.send((method.equals("GET") ? b.GET() : b.method(method, HttpRequest.BodyPublishers.ofString(body == null ? "{}" : body))).build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void administratorsReachTheFleetAndTheirBodiesArePassedThrough() throws Exception {
        HttpResponse<String> state = call("GET", "/api/admin/fleet/state", adminToken, null);
        assertEquals(200, state.statusCode());
        Map<String, Object> parsed = Json.parseObject(state.body());
        assertEquals(7, ((Number) parsed.get("uptimeSec")).intValue());
        assertEquals(200, call("POST", "/api/admin/fleet/control", adminToken, "{\"action\":\"bank\",\"down\":true}").statusCode());
        assertTrue(lastPost.get().contains("\"action\":\"bank\"") && lastPost.get().contains("\"down\":true"), lastPost.get());
        assertEquals(200, call("GET", "/api/admin/fleet/compare", adminToken, null).statusCode());
    }

    @Test
    void theFleetsOwnErrorsAreRelayedWithTheirStatus() throws Exception {
        HttpResponse<String> r = call("POST", "/api/admin/fleet/control", adminToken, "{\"action\":\"boom\"}");
        assertEquals(400, r.statusCode());
        assertEquals("unknown action", Json.parseObject(r.body()).get("error"));
    }

    @Test
    void consumersAndVisitorsAreRefusedBeforeAnythingIsForwarded() throws Exception {
        assertEquals(403, call("GET", "/api/admin/fleet/state", consumerToken, null).statusCode());
        assertEquals(403, call("POST", "/api/admin/fleet/control", consumerToken, "{\"action\":\"bank\"}").statusCode());
        assertEquals(401, call("GET", "/api/admin/fleet/state", null, null).statusCode());
        assertNull(lastPost.get(), "nothing reached the fleet");
    }

    @Test
    void whenTheFleetIsNotRunningTheAnswerSaysSoAndNotAGenericError() throws Exception {
        fleet.stop(0);
        HttpResponse<String> r = call("GET", "/api/admin/fleet/state", adminToken, null);
        assertEquals(503, r.statusCode());
        assertTrue(Json.parseObject(r.body()).get("error").toString().contains("not running"));
        WebServer unconfigured = new WebServer("127.0.0.1", 0, new UserStore(null), new AppStore(null), new Tokens("secret-secret-secret".getBytes(), 1000), gateway, Clock.systemUTC());
        unconfigured.close();
        assertEquals(404, call("GET", "/api/admin/fleet/nope", adminToken, null).statusCode());
    }
}
