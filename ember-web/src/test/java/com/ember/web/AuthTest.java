package com.ember.web;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AuthTest {

    @Test
    void tokenRoundTripsExpiresAndRejectsTampering() {
        Tokens t = new Tokens("secret-secret-secret".getBytes(), 1000);
        String token = t.issue("ada", 5_000);
        assertEquals("ada", t.verify(token, 5_500));
        assertNull(t.verify(token, 6_001), "expired");
        assertNull(t.verify(token + "x", 5_500), "tampered signature");
        assertNull(new Tokens("other-secret-other".getBytes(), 1000).verify(token, 5_500), "other key");
        assertNull(t.verify(null, 0));
        assertNull(t.verify("garbage", 0));
    }

    @Test
    void usersAuthenticateAndLastAdminIsProtected() throws Exception {
        UserStore s = new UserStore(null);
        UserStore.User admin = s.create("root", "Root", "password1", UserStore.Role.ADMIN, 1);
        s.create("bob", null, "password2", UserStore.Role.CONSUMER, 2);
        assertEquals("root", s.authenticate("ROOT", "password1").username());
        assertEquals(401, assertThrows(UserStore.UserException.class, () -> s.authenticate("root", "nope")).status);
        assertEquals(401, assertThrows(UserStore.UserException.class, () -> s.authenticate("ghost", "nope")).status);
        assertEquals(409, assertThrows(UserStore.UserException.class, () -> s.create("Bob", null, "password3", UserStore.Role.CONSUMER, 3)).status);
        assertEquals(400, assertThrows(UserStore.UserException.class, () -> s.create("x", null, "password3", UserStore.Role.CONSUMER, 3)).status);
        assertEquals(409, assertThrows(UserStore.UserException.class, () -> s.update(admin.username(), UserStore.Role.CONSUMER, null)).status);
        s.update("bob", UserStore.Role.ADMIN, null);
        s.update("root", UserStore.Role.CONSUMER, null);      // fine now: bob is an admin too
        assertEquals(409, assertThrows(UserStore.UserException.class, () -> s.update("bob", null, true)).status, "bob is the last admin");
        s.update("root", null, true);
        assertEquals(403, assertThrows(UserStore.UserException.class, () -> s.authenticate("root", "password1")).status);
    }

    @Test
    void jsonRoundTrip() {
        var m = Json.parseObject("{\"a\":[1,2.5,\"x\\n\\u0041\"],\"b\":null,\"c\":true}");
        assertEquals("{\"a\":[1,2.5,\"x\\nA\"],\"b\":null,\"c\":true}", Json.write(m));
    }
}
