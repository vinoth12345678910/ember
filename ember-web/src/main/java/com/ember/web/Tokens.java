package com.ember.web;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Map;

/** Stateless session tokens: base64url(payload JSON) + "." + base64url(HMAC-SHA256). Payload holds the user name and expiry. */
final class Tokens {

    private final byte[] secret;
    private final long ttlMillis;

    Tokens(byte[] secret, long ttlMillis) {
        this.secret = secret;
        this.ttlMillis = ttlMillis;
    }

    String issue(String username, long nowMillis) {
        String payload = Json.write(Json.obj("sub", username, "exp", nowMillis + ttlMillis));
        String body = Base64.getUrlEncoder().withoutPadding().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        return body + "." + sign(body);
    }

    /** The user name if the token is genuine and not expired, else null. */
    String verify(String token, long nowMillis) {
        if (token == null) return null;
        int dot = token.indexOf('.');
        if (dot < 1) return null;
        String body = token.substring(0, dot);
        byte[] expected = sign(body).getBytes(StandardCharsets.UTF_8);
        byte[] given = token.substring(dot + 1).getBytes(StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(expected, given)) return null;
        try {
            Map<String, Object> p = Json.parseObject(new String(Base64.getUrlDecoder().decode(body), StandardCharsets.UTF_8));
            if (((Number) p.get("exp")).longValue() < nowMillis) return null;
            return (String) p.get("sub");
        } catch (RuntimeException e) {
            return null;
        }
    }

    private String sign(String body) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(mac.doFinal(body.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
