package com.ember.web;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Registered applications (the services that produce to and consume from Ember through the console gateway). Each app has an API key
 * (shown once, stored only as a SHA-256 hash: the key is 256 random bits, so a fast hash is enough) and a list of topic patterns it may
 * produce to and consume from. A pattern is an exact topic name, a prefix ending in "*" (orders.*), or just "*".
 */
final class AppStore {

    record App(String id, String name, String description, List<String> produce, List<String> consume, boolean disabled,
               long createdAt, String keyHash, String keyPrefix) {

        boolean canProduce(String topic) {
            return matches(produce, topic);
        }

        boolean canConsume(String topic) {
            return matches(consume, topic);
        }

        Map<String, Object> view(Stats stats) {
            return Json.obj("id", id, "name", name, "description", description, "produceTopics", produce, "consumeTopics", consume,
                    "disabled", disabled, "createdAt", createdAt, "keyPrefix", keyPrefix,
                    "lastSeen", stats == null ? 0 : stats.lastSeen.get(), "produced", stats == null ? 0 : stats.produced.get(),
                    "fetched", stats == null ? 0 : stats.fetched.get());
        }
    }

    /** In-memory activity counters (reset on restart). */
    static final class Stats {
        final AtomicLong lastSeen = new AtomicLong();
        final AtomicLong produced = new AtomicLong();
        final AtomicLong fetched = new AtomicLong();
    }

    /** The new app plus its plain-text key, which exists only in this object. */
    record Created(App app, String key) {
    }

    static final class AppException extends Exception {
        final int status;

        AppException(int status, String message) {
            super(message);
            this.status = status;
        }
    }

    static final String KEY_PREFIX = "emb_";
    private static final Pattern NAME = Pattern.compile("[a-zA-Z0-9][a-zA-Z0-9._ -]{1,46}[a-zA-Z0-9]");
    private static final Pattern TOPIC_PATTERN = Pattern.compile("\\*|[a-zA-Z0-9._-]{1,127}\\*?");
    private static final SecureRandom RANDOM = new SecureRandom();

    private final Path file;
    private final Map<String, App> apps = new LinkedHashMap<>();                       // id -> app
    private final Map<String, String> byKeyHash = new HashMap<>();                     // key hash -> id
    private final Map<String, Stats> stats = new ConcurrentHashMap<>();

    AppStore(Path file) throws IOException {
        this.file = file;
        if (file != null && Files.exists(file)) {
            for (Object o : (List<?>) Json.parse(Files.readString(file))) {
                @SuppressWarnings("unchecked") Map<String, Object> m = (Map<String, Object>) o;
                App a = new App((String) m.get("id"), (String) m.get("name"), (String) m.get("description"), strings(m.get("produce")),
                        strings(m.get("consume")), Boolean.TRUE.equals(m.get("disabled")), ((Number) m.get("createdAt")).longValue(),
                        (String) m.get("keyHash"), (String) m.get("keyPrefix"));
                index(a);
            }
        }
    }

    static boolean matches(List<String> patterns, String topic) {
        for (String p : patterns) {
            if (p.equals("*") || p.equals(topic)) return true;
            if (p.endsWith("*") && topic.startsWith(p.substring(0, p.length() - 1))) return true;
        }
        return false;
    }

    synchronized Created register(String name, String description, List<String> produce, List<String> consume, long now) throws AppException, IOException {
        validateName(name, null);
        List<String> p = patterns(produce), c = patterns(consume);
        if (p.isEmpty() && c.isEmpty()) throw new AppException(400, "give the app at least one topic to produce to or consume from");
        String key = newKey();
        App a = new App("app_" + randomId(), name.strip(), description == null ? "" : description.strip(), p, c, false, now, hash(key), prefix(key));
        index(a);
        save();
        return new Created(a, key);
    }

    /** Replaces whichever of these is non-null. */
    synchronized App update(String id, String description, List<String> produce, List<String> consume, Boolean disabled) throws AppException, IOException {
        App a = get(id);
        List<String> p = produce == null ? a.produce() : patterns(produce), c = consume == null ? a.consume() : patterns(consume);
        if (p.isEmpty() && c.isEmpty()) throw new AppException(400, "an app needs at least one topic to produce to or consume from");
        App n = new App(a.id(), a.name(), description == null ? a.description() : description.strip(), p, c,
                disabled == null ? a.disabled() : disabled, a.createdAt(), a.keyHash(), a.keyPrefix());
        apps.put(id, n);
        save();
        return n;
    }

    /** Issues a new key; the old one stops working at once. */
    synchronized Created rotateKey(String id) throws AppException, IOException {
        App a = get(id);
        byKeyHash.remove(a.keyHash());
        String key = newKey();
        App n = new App(a.id(), a.name(), a.description(), a.produce(), a.consume(), a.disabled(), a.createdAt(), hash(key), prefix(key));
        index(n);
        save();
        return new Created(n, key);
    }

    synchronized void delete(String id) throws AppException, IOException {
        App a = get(id);
        apps.remove(id);
        byKeyHash.remove(a.keyHash());
        stats.remove(id);
        save();
    }

    /** The app for this key, or throws 401 (unknown key) / 403 (disabled). Records that the app was seen. */
    synchronized App authenticate(String key, long now) throws AppException {
        String id = key == null || !key.startsWith(KEY_PREFIX) ? null : byKeyHash.get(hash(key));
        if (id == null) throw new AppException(401, "unknown or revoked API key");
        App a = apps.get(id);
        if (a.disabled()) throw new AppException(403, "this app is disabled");
        stats(id).lastSeen.set(now);
        return a;
    }

    Stats stats(String id) {
        return stats.computeIfAbsent(id, k -> new Stats());
    }

    synchronized List<Map<String, Object>> list() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (App a : apps.values()) out.add(a.view(stats.get(a.id())));
        return out;
    }

    synchronized Map<String, Object> view(App a) {
        return a.view(stats.get(a.id()));
    }

    // ------------------------------------------------------------------ internals

    private App get(String id) throws AppException {
        App a = apps.get(id);
        if (a == null) throw new AppException(404, "no such app");
        return a;
    }

    private void index(App a) {
        apps.put(a.id(), a);
        byKeyHash.put(a.keyHash(), a.id());
    }

    private void validateName(String name, String exceptId) throws AppException {
        if (name == null || !NAME.matcher(name.strip()).matches()) {
            throw new AppException(400, "app name must be 3-48 characters: letters, digits, space . _ -");
        }
        for (App a : apps.values()) {
            if (a.name().equalsIgnoreCase(name.strip()) && !a.id().equals(exceptId)) throw new AppException(409, "an app with that name already exists");
        }
    }

    private static List<String> patterns(List<String> in) throws AppException {
        List<String> out = new ArrayList<>();
        if (in == null) return out;
        for (String s : in) {
            String t = s == null ? "" : s.strip();
            if (t.isEmpty()) continue;
            if (!TOPIC_PATTERN.matcher(t).matches()) throw new AppException(400, "bad topic pattern '" + t + "': use a topic name, a prefix like orders.* or *");
            if (!out.contains(t)) out.add(t);
        }
        return out;
    }

    private static List<String> strings(Object o) {
        List<String> out = new ArrayList<>();
        if (o instanceof List<?> l) for (Object x : l) out.add((String) x);
        return out;
    }

    private static String newKey() {
        byte[] b = new byte[32];
        RANDOM.nextBytes(b);
        return KEY_PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }

    private static String randomId() {
        byte[] b = new byte[6];
        RANDOM.nextBytes(b);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b).toLowerCase().replace('-', 'x').replace('_', 'y');
    }

    private static String prefix(String key) {
        return key.substring(0, KEY_PREFIX.length() + 6);
    }

    static String hash(String key) {
        try {
            return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private void save() throws IOException {
        if (file == null) return;
        List<Object> out = new ArrayList<>();
        for (App a : apps.values()) {
            out.add(Json.obj("id", a.id(), "name", a.name(), "description", a.description(), "produce", a.produce(), "consume", a.consume(),
                    "disabled", a.disabled(), "createdAt", a.createdAt(), "keyHash", a.keyHash(), "keyPrefix", a.keyPrefix()));
        }
        if (file.getParent() != null) Files.createDirectories(file.getParent());
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.writeString(tmp, Json.write(out));
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
