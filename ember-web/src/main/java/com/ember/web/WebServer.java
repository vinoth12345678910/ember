package com.ember.web;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** HTTP front: static pages from the classpath (/web) and the JSON API under /api. */
final class WebServer implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(WebServer.class.getName());
    private static final int MAX_BODY = 1 << 20;
    private static final Pattern TOPIC = Pattern.compile("[a-zA-Z0-9._-]{1,128}");
    private static final Pattern APP_MESSAGES = Pattern.compile("/api/app/topics/([^/]+)/messages");
    private static final Pattern MESSAGES = Pattern.compile("/api/topics/([^/]+)/messages");
    private static final Pattern USER = Pattern.compile("/api/admin/users/([^/]+)");
    private static final Pattern APP = Pattern.compile("/api/admin/apps/([^/]+)(/rotate-key)?");

    private static final Map<String, String> TYPES = Map.of(
            "html", "text/html; charset=utf-8", "css", "text/css; charset=utf-8", "js", "text/javascript; charset=utf-8",
            "svg", "image/svg+xml", "json", "application/json; charset=utf-8", "ico", "image/x-icon", "png", "image/png");

    /** An error with an HTTP status, thrown by handlers and rendered as {"error": message}. */
    private static final class ApiException extends RuntimeException {
        final int status;

        ApiException(int status, String message) {
            super(message);
            this.status = status;
        }
    }

    private final UserStore users;
    private final AppStore apps;
    private final Tokens tokens;
    private final BrokerGateway gateway;
    private final Clock clock;
    private final HttpServer http;
    private final ExecutorService pool = Executors.newFixedThreadPool(8);
    private final Map<String, ArrayDeque<Long>> failures = new HashMap<>();      // "ip|user" -> times of recent failed logins

    WebServer(String host, int port, UserStore users, AppStore apps, Tokens tokens, BrokerGateway gateway, Clock clock) throws IOException {
        this.users = users;
        this.apps = apps;
        this.tokens = tokens;
        this.gateway = gateway;
        this.clock = clock;
        this.http = HttpServer.create(new InetSocketAddress(host, port), 64);
        http.setExecutor(pool);
        http.createContext("/api/", this::api);
        http.createContext("/", this::statik);
        http.start();
    }

    int port() {
        return http.getAddress().getPort();
    }

    @Override
    public void close() {
        http.stop(0);
        pool.shutdownNow();
    }

    // ------------------------------------------------------------------ static files

    private void statik(HttpExchange ex) throws IOException {
        try {
            String path = ex.getRequestURI().getPath();
            if (!ex.getRequestMethod().equals("GET") && !ex.getRequestMethod().equals("HEAD")) {
                send(ex, 405, "text/plain; charset=utf-8", "GET only".getBytes(StandardCharsets.UTF_8));
                return;
            }
            if (path.equals("/")) path = "/index.html";
            if (path.contains("..") || path.contains("//")) {
                send(ex, 400, "text/plain; charset=utf-8", "bad path".getBytes(StandardCharsets.UTF_8));
                return;
            }
            try (InputStream in = WebServer.class.getResourceAsStream("/web" + path)) {
                if (in == null) {
                    send(ex, 404, "text/plain; charset=utf-8", "not found".getBytes(StandardCharsets.UTF_8));
                    return;
                }
                String ext = path.substring(path.lastIndexOf('.') + 1);
                send(ex, 200, TYPES.getOrDefault(ext, "application/octet-stream"), in.readAllBytes());
            }
        } finally {
            ex.close();
        }
    }

    private static void send(HttpExchange ex, int status, String type, byte[] body) throws IOException {
        var h = ex.getResponseHeaders();
        h.set("Content-Type", type);
        h.set("X-Content-Type-Options", "nosniff");
        h.set("X-Frame-Options", "DENY");
        h.set("Referrer-Policy", "no-referrer");
        h.set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'");
        h.set("Cache-Control", "no-store");
        boolean head = ex.getRequestMethod().equals("HEAD");
        ex.sendResponseHeaders(status, head ? -1 : body.length);
        if (!head) {
            try (OutputStream out = ex.getResponseBody()) {
                out.write(body);
            }
        }
    }

    // ------------------------------------------------------------------ API

    private void api(HttpExchange ex) throws IOException {
        try {
            Object result = route(ex);
            send(ex, 200, "application/json; charset=utf-8", Json.write(result).getBytes(StandardCharsets.UTF_8));
        } catch (ApiException e) {
            send(ex, e.status, "application/json; charset=utf-8", Json.write(Json.obj("error", e.getMessage())).getBytes(StandardCharsets.UTF_8));
        } catch (AppStore.AppException e) {
            send(ex, e.status, "application/json; charset=utf-8", Json.write(Json.obj("error", e.getMessage())).getBytes(StandardCharsets.UTF_8));
        } catch (UserStore.UserException e) {
            send(ex, e.status, "application/json; charset=utf-8", Json.write(Json.obj("error", e.getMessage())).getBytes(StandardCharsets.UTF_8));
        } catch (IllegalArgumentException | ClassCastException | NullPointerException e) {
            send(ex, 400, "application/json; charset=utf-8", Json.write(Json.obj("error", "bad request")).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            LOG.log(System.Logger.Level.WARNING, "cluster call failed for " + ex.getRequestURI().getPath() + ": " + e);
            send(ex, 502, "application/json; charset=utf-8", Json.write(Json.obj("error", "the Ember cluster is not reachable right now: " + e.getMessage()))
                    .getBytes(StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.ERROR, "request failed: " + ex.getRequestURI(), e);
            send(ex, 500, "application/json; charset=utf-8", Json.write(Json.obj("error", "internal error")).getBytes(StandardCharsets.UTF_8));
        } finally {
            ex.close();
        }
    }

    private Object route(HttpExchange ex) throws IOException, UserStore.UserException, AppStore.AppException {
        String method = ex.getRequestMethod();
        String path = ex.getRequestURI().getPath();
        Map<String, String> query = query(ex.getRequestURI().getRawQuery());
        long now = clock.millis();

        // public
        if (path.equals("/api/auth/login") && method.equals("POST")) return login(ex, body(ex), now);
        if (path.equals("/api/auth/register") && method.equals("POST")) return register(ex, body(ex), now);
        if (path.equals("/api/health") && method.equals("GET")) return Json.obj("ok", true);

        if (path.startsWith("/api/app/")) return appRoute(ex, method, path, query, now);

        UserStore.User me = authenticated(ex, now);

        if (path.equals("/api/me") && method.equals("GET")) return Json.obj("user", me.view());
        if (path.equals("/api/topics") && method.equals("GET")) return Json.obj("topics", gateway.topics());
        if (path.equals("/api/groups") && method.equals("GET")) return Json.obj("groups", gateway.consumerGroups());

        Matcher m = MESSAGES.matcher(path);
        if (m.matches()) {
            String topic = URLDecoder.decode(m.group(1), StandardCharsets.UTF_8);
            checkTopic(topic);
            if (method.equals("GET")) return fetch(topic, query);
            if (method.equals("POST")) return produce(topic, body(ex));
        }

        // admin only
        if (path.startsWith("/api/admin/")) {
            if (me.role() != UserStore.Role.ADMIN) throw new ApiException(403, "admin access required");
            if (path.equals("/api/admin/cluster") && method.equals("GET")) return Json.obj("nodes", gateway.nodes());
            if (path.equals("/api/admin/topics") && method.equals("POST")) return createTopic(body(ex));
            if (path.equals("/api/admin/users") && method.equals("GET")) return Json.obj("users", users.list().stream().map(UserStore.User::view).toList());
            if (path.equals("/api/admin/apps") && method.equals("GET")) return Json.obj("apps", apps.list());
            if (path.equals("/api/admin/apps") && method.equals("POST")) return registerApp(body(ex), now);
            Matcher ap = APP.matcher(path);
            if (ap.matches()) {
                String id = ap.group(1);
                if (ap.group(2) != null && method.equals("POST")) return created(apps.rotateKey(id));
                if (ap.group(2) == null && method.equals("PATCH")) return updateApp(id, body(ex));
                if (ap.group(2) == null && method.equals("DELETE")) {
                    apps.delete(id);
                    return Json.obj("ok", true);
                }
            }
            Matcher u = USER.matcher(path);
            if (u.matches()) {
                String name = URLDecoder.decode(u.group(1), StandardCharsets.UTF_8);
                if (method.equals("PATCH")) return updateUser(me, name, body(ex));
                if (method.equals("DELETE")) {
                    if (name.equalsIgnoreCase(me.username())) throw new ApiException(409, "you cannot delete your own account");
                    users.delete(name);
                    return Json.obj("ok", true);
                }
            }
        }
        throw new ApiException(404, "no such endpoint");
    }

    /** Endpoints for registered apps, authenticated by API key (Authorization: Bearer emb_... or X-Ember-Key). */
    private Object appRoute(HttpExchange ex, String method, String path, Map<String, String> query, long now) throws IOException, AppStore.AppException {
        String header = ex.getRequestHeaders().getFirst("Authorization");
        String key = header != null && header.startsWith("Bearer ") ? header.substring(7).trim() : ex.getRequestHeaders().getFirst("X-Ember-Key");
        AppStore.App app = apps.authenticate(key, now);
        if (path.equals("/api/app/me") && method.equals("GET")) return Json.obj("app", apps.view(app));
        Matcher m = APP_MESSAGES.matcher(path);
        if (m.matches()) {
            String topic = URLDecoder.decode(m.group(1), StandardCharsets.UTF_8);
            checkTopic(topic);
            if (method.equals("POST")) {
                if (!app.canProduce(topic)) throw new ApiException(403, "this app may not produce to '" + topic + "'");
                Object r = produce(topic, body(ex));
                apps.stats(app.id()).produced.incrementAndGet();
                return r;
            }
            if (method.equals("GET")) {
                if (!app.canConsume(topic)) throw new ApiException(403, "this app may not consume from '" + topic + "'");
                Object r = fetch(topic, query);
                apps.stats(app.id()).fetched.addAndGet(((List<?>) ((Map<?, ?>) r).get("records")).size());
                return r;
            }
        }
        throw new ApiException(404, "no such endpoint");
    }

    private Object registerApp(Map<String, Object> b, long now) throws AppStore.AppException, IOException {
        return created(apps.register(str(b, "name"), str(b, "description"), list(b, "produceTopics"), list(b, "consumeTopics"), now));
    }

    private Object updateApp(String id, Map<String, Object> b) throws AppStore.AppException, IOException {
        return Json.obj("app", apps.view(apps.update(id, str(b, "description"), b.containsKey("produceTopics") ? list(b, "produceTopics") : null,
                b.containsKey("consumeTopics") ? list(b, "consumeTopics") : null, (Boolean) b.get("disabled"))));
    }

    /** The key is returned here and nowhere else. */
    private Object created(AppStore.Created c) {
        return Json.obj("app", apps.view(c.app()), "apiKey", c.key());
    }

    private static List<String> list(Map<String, Object> m, String key) {
        List<String> out = new java.util.ArrayList<>();
        if (m.get(key) instanceof List<?> l) for (Object o : l) out.add((String) o);
        return out;
    }

    private UserStore.User authenticated(HttpExchange ex, long now) {
        String header = ex.getRequestHeaders().getFirst("Authorization");
        String name = header != null && header.startsWith("Bearer ") ? tokens.verify(header.substring(7).trim(), now) : null;
        UserStore.User u = users.find(name);
        if (u == null || u.disabled()) throw new ApiException(401, "please sign in");
        return u;
    }

    private Object login(HttpExchange ex, Map<String, Object> b, long now) throws UserStore.UserException {
        String username = str(b, "username");
        String bucket = ex.getRemoteAddress().getAddress().getHostAddress() + "|" + (username == null ? "" : username.toLowerCase());
        throttle(bucket, now, false);
        try {
            UserStore.User u = users.authenticate(username, str(b, "password"));
            return Json.obj("token", tokens.issue(u.username(), now), "user", u.view());
        } catch (UserStore.UserException e) {
            if (e.status == 401) throttle(bucket, now, true);
            throw e;
        }
    }

    /** At most 8 failed sign-ins per address+user per 5 minutes. record=false only checks, record=true adds a failure. */
    private void throttle(String bucket, long now, boolean record) {
        synchronized (failures) {
            ArrayDeque<Long> q = failures.computeIfAbsent(bucket, k -> new ArrayDeque<>());
            while (!q.isEmpty() && q.peekFirst() < now - 300_000) q.pollFirst();
            if (record) q.addLast(now);
            else if (q.size() >= 8) throw new ApiException(429, "too many failed attempts: try again in a few minutes");
            if (q.isEmpty()) failures.remove(bucket);
        }
    }

    private Object register(HttpExchange ex, Map<String, Object> b, long now) throws UserStore.UserException, IOException {
        UserStore.User u = users.create(str(b, "username"), str(b, "displayName"), str(b, "password"), UserStore.Role.CONSUMER, now);
        return Json.obj("token", tokens.issue(u.username(), now), "user", u.view());
    }

    private Object updateUser(UserStore.User me, String name, Map<String, Object> b) throws UserStore.UserException, IOException {
        UserStore.Role role = b.get("role") == null ? null : UserStore.Role.valueOf(str(b, "role"));
        Boolean disabled = (Boolean) b.get("disabled");
        if (name.equalsIgnoreCase(me.username()) && (role != null && role != me.role() || Boolean.TRUE.equals(disabled))) {
            throw new ApiException(409, "you cannot change your own role or disable yourself");
        }
        return Json.obj("user", users.update(name, role, disabled).view());
    }

    private Object createTopic(Map<String, Object> b) throws IOException {
        String name = str(b, "name");
        int partitions = b.get("partitions") == null ? 3 : ((Number) b.get("partitions")).intValue();
        checkTopic(name);
        if (name.startsWith("__")) throw new ApiException(400, "names starting with __ are reserved");
        if (partitions < 1 || partitions > 64) throw new ApiException(400, "partitions must be 1-64");
        try {
            return Json.obj("name", name, "partitions", gateway.createTopic(name, partitions));
        } catch (com.ember.client.EmberException e) {
            if (e.errorCode() == com.ember.protocol.ErrorCode.TOPIC_ALREADY_EXISTS) throw new ApiException(409, "that topic already exists");
            throw new ApiException(400, "the broker refused the topic: " + e.errorCode());
        }
    }

    private Object produce(String topic, Map<String, Object> b) throws IOException {
        String value = str(b, "value");
        if (value == null || value.isEmpty()) throw new ApiException(400, "a message needs a value");
        if (value.length() > 256 * 1024) throw new ApiException(413, "message too large (256 KB max)");
        return gateway.produce(topic, str(b, "key"), value);
    }

    private Object fetch(String topic, Map<String, String> q) throws IOException {
        int partition = Integer.parseInt(q.getOrDefault("partition", "0"));
        long offset = Long.parseLong(q.getOrDefault("offset", "0"));
        int limit = Math.max(1, Math.min(200, Integer.parseInt(q.getOrDefault("limit", "50"))));
        if (partition < 0 || offset < 0) throw new ApiException(400, "partition and offset must not be negative");
        return gateway.fetch(topic, partition, offset, limit);
    }

    private static void checkTopic(String name) {
        if (name == null || !TOPIC.matcher(name).matches()) throw new ApiException(400, "topic names use letters, digits, . _ - (max 128)");
    }

    private static String str(Map<String, Object> m, String key) {
        Object v = m.get(key);
        return v == null ? null : (String) v;
    }

    private static Map<String, Object> body(HttpExchange ex) throws IOException {
        byte[] raw = ex.getRequestBody().readNBytes(MAX_BODY + 1);
        if (raw.length > MAX_BODY) throw new ApiException(413, "request too large");
        if (raw.length == 0) return Map.of();
        return Json.parseObject(new String(raw, StandardCharsets.UTF_8));
    }

    private static Map<String, String> query(String raw) {
        Map<String, String> q = new HashMap<>();
        if (raw == null) return q;
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) q.put(URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8), URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
        }
        return q;
    }
}
