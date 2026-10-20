package com.ember.client;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MiniJsonTest {

    @Test
    void readsNestedObjectsArraysAndEscapes() {
        Map<String, Object> m = MiniJson.parseObject("{\"a\": 1, \"b\": [\"x\", true, null, {\"k\": 2.5}], \"c\": \"say \\\"hi\\\"\\n\"}");
        assertEquals(1.0, m.get("a"));
        assertEquals(java.util.Arrays.asList("x", true, null, Map.of("k", 2.5)), m.get("b"));
        assertEquals("say \"hi\"\n", m.get("c"));
    }

    @Test
    void rejectsGarbageAndNonObjects() {
        assertThrows(IllegalArgumentException.class, () -> MiniJson.parseObject("[1,2]"));
        assertThrows(IllegalArgumentException.class, () -> MiniJson.parseObject("{\"a\":1} trailing"));
        assertThrows(RuntimeException.class, () -> MiniJson.parseObject("{\"a\":"));
    }
}
