package com.ember.web;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AppStoreTest {

    @Test
    void topicPatternsMatchExactPrefixAndWildcard() {
        assertTrue(AppStore.matches(List.of("orders"), "orders"));
        assertFalse(AppStore.matches(List.of("orders"), "orders2"));
        assertTrue(AppStore.matches(List.of("ride.*"), "ride.events"));
        assertFalse(AppStore.matches(List.of("ride.*"), "payments"));
        assertTrue(AppStore.matches(List.of("*"), "anything"));
        assertFalse(AppStore.matches(List.of(), "orders"));
    }

    @Test
    void keyWorksOnceIssuedAndRotationRevokesTheOldOne() throws Exception {
        AppStore s = new AppStore(null);
        AppStore.Created c = s.register("matcher", "pairs riders", List.of("ride-events"), List.of("ride-requests"), 1);
        assertTrue(c.key().startsWith("emb_"));
        assertEquals(c.app().id(), s.authenticate(c.key(), 5).id());
        assertTrue(c.app().canProduce("ride-events"));
        assertFalse(c.app().canProduce("ride-requests"));
        assertTrue(c.app().canConsume("ride-requests"));
        assertFalse(c.app().keyHash().contains(c.key()), "only a hash is stored");

        AppStore.Created r = s.rotateKey(c.app().id());
        assertEquals(401, assertThrows(AppStore.AppException.class, () -> s.authenticate(c.key(), 6)).status);
        assertEquals(c.app().id(), s.authenticate(r.key(), 6).id());
    }

    @Test
    void disabledDeletedAndInvalidAppsAreRejected() throws Exception {
        AppStore s = new AppStore(null);
        AppStore.Created c = s.register("billing", null, List.of(), List.of("payments"), 1);
        s.update(c.app().id(), null, null, null, true);
        assertEquals(403, assertThrows(AppStore.AppException.class, () -> s.authenticate(c.key(), 2)).status);
        s.update(c.app().id(), null, null, null, false);
        s.authenticate(c.key(), 3);
        s.delete(c.app().id());
        assertEquals(401, assertThrows(AppStore.AppException.class, () -> s.authenticate(c.key(), 4)).status);

        assertEquals(409, assertThrows(AppStore.AppException.class, () -> { s.register("dup", null, List.of("a"), List.of(), 1); s.register("DUP", null, List.of("a"), List.of(), 1); }).status);
        assertEquals(400, assertThrows(AppStore.AppException.class, () -> s.register("no-topics", null, List.of(), List.of(), 1)).status);
        assertEquals(400, assertThrows(AppStore.AppException.class, () -> s.register("bad-topic", null, List.of("a/b"), List.of(), 1)).status);
        assertEquals(401, assertThrows(AppStore.AppException.class, () -> s.authenticate("not-a-key", 1)).status);
        assertEquals(401, assertThrows(AppStore.AppException.class, () -> s.authenticate(null, 1)).status);
    }

    @Test
    void appsSurviveARestart(@TempDir Path dir) throws Exception {
        Path f = dir.resolve("apps.json");
        AppStore a = new AppStore(f);
        AppStore.Created c = a.register("tracker", "d", List.of("gps"), List.of("gps"), 1);
        AppStore b = new AppStore(f);
        assertEquals("tracker", b.authenticate(c.key(), 2).name());
        assertEquals(List.of("gps"), b.authenticate(c.key(), 2).consume());
    }
}
