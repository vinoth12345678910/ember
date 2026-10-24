package com.ember.web;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** Users with PBKDF2-hashed passwords, kept in one JSON file (rewritten atomically on every change). */
final class UserStore {

    enum Role { ADMIN, CONSUMER }

    record User(String username, String displayName, Role role, boolean disabled, long createdAt, String salt, String hash) {
        Map<String, Object> view() {
            return Json.obj("username", username, "displayName", displayName, "role", role.name(), "disabled", disabled, "createdAt", createdAt);
        }
    }

    static final class UserException extends Exception {
        final int status;

        UserException(int status, String message) {
            super(message);
            this.status = status;
        }
    }

    private static final Pattern NAME = Pattern.compile("[a-zA-Z0-9._-]{3,32}");
    private static final int ITERATIONS = 120_000;
    private static final SecureRandom RANDOM = new SecureRandom();

    private final Path file;
    private final Map<String, User> users = new LinkedHashMap<>();

    UserStore(Path file) throws IOException {
        this.file = file;
        if (file != null && Files.exists(file)) {
            Object parsed = Json.parse(Files.readString(file));
            for (Object o : (List<?>) parsed) {
                @SuppressWarnings("unchecked") Map<String, Object> m = (Map<String, Object>) o;
                User u = new User((String) m.get("username"), (String) m.get("displayName"), Role.valueOf((String) m.get("role")),
                        Boolean.TRUE.equals(m.get("disabled")), ((Number) m.get("createdAt")).longValue(), (String) m.get("salt"), (String) m.get("hash"));
                users.put(u.username().toLowerCase(), u);
            }
        }
    }

    synchronized boolean isEmpty() {
        return users.isEmpty();
    }

    synchronized User create(String username, String displayName, String password, Role role, long now) throws UserException, IOException {
        validate(username, password);
        String key = username.toLowerCase();
        if (users.containsKey(key)) throw new UserException(409, "that username is taken");
        byte[] salt = new byte[16];
        RANDOM.nextBytes(salt);
        String saltB64 = Base64.getEncoder().encodeToString(salt);
        String name = displayName == null || displayName.isBlank() ? username : displayName.strip();
        User u = new User(username, name.length() > 60 ? name.substring(0, 60) : name, role, false, now, saltB64, hash(password, salt));
        users.put(key, u);
        save();
        return u;
    }

    /** The user if the password matches and the account is enabled; throws 401 (same message for unknown user and wrong password). */
    synchronized User authenticate(String username, String password) throws UserException {
        User u = username == null ? null : users.get(username.toLowerCase());
        byte[] salt = u == null ? new byte[16] : Base64.getDecoder().decode(u.salt());
        String candidate = hash(password == null ? "" : password, salt);     // hash even for unknown users: no timing hint
        if (u == null || !MessageDigest.isEqual(candidate.getBytes(StandardCharsets.UTF_8), u.hash().getBytes(StandardCharsets.UTF_8))) {
            throw new UserException(401, "wrong username or password");
        }
        if (u.disabled()) throw new UserException(403, "this account is disabled");
        return u;
    }

    synchronized User find(String username) {
        return username == null ? null : users.get(username.toLowerCase());
    }

    synchronized List<User> list() {
        return new ArrayList<>(users.values());
    }

    /** Changes role and/or disabled flag. The last enabled admin can neither be demoted nor disabled. */
    synchronized User update(String username, Role role, Boolean disabled) throws UserException, IOException {
        User u = find(username);
        if (u == null) throw new UserException(404, "no such user");
        Role newRole = role == null ? u.role() : role;
        boolean newDisabled = disabled == null ? u.disabled() : disabled;
        if (u.role() == Role.ADMIN && !u.disabled() && (newRole != Role.ADMIN || newDisabled) && enabledAdmins() <= 1) {
            throw new UserException(409, "cannot demote or disable the last active admin");
        }
        User n = new User(u.username(), u.displayName(), newRole, newDisabled, u.createdAt(), u.salt(), u.hash());
        users.put(username.toLowerCase(), n);
        save();
        return n;
    }

    synchronized void delete(String username) throws UserException, IOException {
        User u = find(username);
        if (u == null) throw new UserException(404, "no such user");
        if (u.role() == Role.ADMIN && !u.disabled() && enabledAdmins() <= 1) throw new UserException(409, "cannot delete the last active admin");
        users.remove(username.toLowerCase());
        save();
    }

    private long enabledAdmins() {
        return users.values().stream().filter(x -> x.role() == Role.ADMIN && !x.disabled()).count();
    }

    private static void validate(String username, String password) throws UserException {
        if (username == null || !NAME.matcher(username).matches()) {
            throw new UserException(400, "username must be 3-32 characters: letters, digits, . _ -");
        }
        if (password == null || password.length() < 8 || password.length() > 128) {
            throw new UserException(400, "password must be 8-128 characters");
        }
    }

    private static String hash(String password, byte[] salt) {
        try {
            byte[] h = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
                    .generateSecret(new PBEKeySpec(password.toCharArray(), salt, ITERATIONS, 256)).getEncoded();
            return Base64.getEncoder().encodeToString(h);
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private void save() throws IOException {
        if (file == null) return;
        List<Object> out = new ArrayList<>();
        for (User u : users.values()) {
            out.add(Json.obj("username", u.username(), "displayName", u.displayName(), "role", u.role().name(), "disabled", u.disabled(),
                    "createdAt", u.createdAt(), "salt", u.salt(), "hash", u.hash()));
        }
        if (file.getParent() != null) Files.createDirectories(file.getParent());
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.writeString(tmp, Json.write(out));
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
