package com.ember.web;

import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The Ember web console: a landing page, sign-in / sign-up, a consumer workspace and an admin console, served by one small process.
 *
 *   java -jar ember-web-all.jar --bootstrap=localhost:19092,localhost:19093,localhost:19094 --admin=localhost:18081,localhost:18082,localhost:18083
 *
 * Flags (or environment variables): --port / EMBER_WEB_PORT (8088), --host / EMBER_WEB_HOST (0.0.0.0), --bootstrap / EMBER_WEB_BOOTSTRAP,
 * --admin / EMBER_WEB_ADMIN, --data-dir / EMBER_WEB_DATA_DIR (./ember-web-data), --secret / EMBER_WEB_SECRET (random per start when unset),
 * --admin-user / --admin-password (first start only, when no user exists yet).
 */
public final class EmberWeb {

    private EmberWeb() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> flags = new HashMap<>();
        for (String a : args) {
            if (!a.startsWith("--") || !a.contains("=")) {
                System.err.println("usage: java -jar ember-web-all.jar --bootstrap=host:port,... [--admin=host:port,...] [--port=8088] [--data-dir=DIR] "
                        + "[--secret=...] [--admin-user=admin --admin-password=...]");
                System.exit(2);
            }
            flags.put(a.substring(2, a.indexOf('=')), a.substring(a.indexOf('=') + 1));
        }
        int port = Integer.parseInt(opt(flags, "port", "EMBER_WEB_PORT", "8088"));
        String host = opt(flags, "host", "EMBER_WEB_HOST", "0.0.0.0");
        List<String> bootstrap = list(opt(flags, "bootstrap", "EMBER_WEB_BOOTSTRAP", "localhost:19092,localhost:19093,localhost:19094"));
        List<String> admin = list(opt(flags, "admin", "EMBER_WEB_ADMIN", "localhost:18081,localhost:18082,localhost:18083"));
        Path dataDir = Path.of(opt(flags, "data-dir", "EMBER_WEB_DATA_DIR", "./ember-web-data"));
        String secret = opt(flags, "secret", "EMBER_WEB_SECRET", null);
        byte[] key;
        if (secret == null) {
            key = new byte[32];
            new SecureRandom().nextBytes(key);
            System.out.println("no --secret given: sessions will end when this process restarts");
        } else {
            key = secret.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }

        UserStore users = new UserStore(dataDir.resolve("users.json"));
        if (users.isEmpty()) {
            String name = opt(flags, "admin-user", "EMBER_WEB_ADMIN_USER", "admin");
            String password = opt(flags, "admin-password", "EMBER_WEB_ADMIN_PASSWORD", null);
            boolean generated = password == null;
            if (generated) {
                byte[] raw = new byte[9];
                new SecureRandom().nextBytes(raw);
                password = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
            }
            users.create(name, "Administrator", password, UserStore.Role.ADMIN, System.currentTimeMillis());
            System.out.println("created the first admin account: " + name + (generated ? " / " + password + "   (generated: note it now, it is not stored in clear)" : ""));
        }

        AppStore apps = new AppStore(dataDir.resolve("apps.json"));
        BrokerGateway gateway = new BrokerGateway(bootstrap, admin);
        WebServer server = new WebServer(host, port, users, apps, new Tokens(key, 8L * 3600_000), gateway, java.time.Clock.systemUTC());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            try {
                gateway.close();
            } catch (Exception ignored) {
            }
        }, "ember-web-shutdown"));
        System.out.println("Ember web console on http://" + (host.equals("0.0.0.0") ? "localhost" : host) + ":" + server.port()
                + "  (brokers " + bootstrap + ", admin ports " + admin + ")");
    }

    private static String opt(Map<String, String> flags, String flag, String env, String fallback) {
        String v = flags.get(flag);
        if (v == null) v = System.getenv(env);
        return v == null || v.isEmpty() ? fallback : v;
    }

    private static List<String> list(String csv) {
        return Arrays.stream(csv.split(",")).map(String::strip).filter(s -> !s.isEmpty()).toList();
    }
}
