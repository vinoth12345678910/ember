package com.ember.broker;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonWriterTest {

    @Test
    void nestedStructuresGetTheirCommasRight() {
        String json = new JsonWriter().beginObject()
                .name("a").value(1)
                .name("list").beginArray().value("x").value(2.5).value(true).beginObject().name("k").value("v").endObject().beginArray().endArray().endArray()
                .name("empty").beginObject().endObject()
                .name("n").value((String) null)
                .endObject().toString();
        Map<String, Object> m = TestJson.object(json);
        assertEquals(1.0, m.get("a"));
        assertEquals(List.of("x", 2.5, true, Map.of("k", "v"), List.of()), m.get("list"));
        assertEquals(Map.of(), m.get("empty"));
        assertNull(m.get("n"));
        assertTrue(m.containsKey("n"));
    }

    @Test
    void quotesBackslashesNewlinesAndControlCharactersAreEscaped() {
        String nasty = "say \"hi\"\\path\nline2\ttab\u0001end";
        String json = new JsonWriter().beginObject().name("quote\"key").value(nasty).endObject().toString();
        assertFalse(json.contains("\n"), "no raw newline inside the JSON text");
        assertEquals(nasty, TestJson.object(json).get("quote\"key"));
    }

    @Test
    void notANumberAndInfinityBecomeNullBecauseJsonHasNoSuchValues() {
        String json = new JsonWriter().beginObject().name("a").value(Double.NaN).name("b").value(Double.POSITIVE_INFINITY).endObject().toString();
        Map<String, Object> m = TestJson.object(json);
        assertNull(m.get("a"));
        assertNull(m.get("b"));
    }

    @Test
    void anUnclosedContainerIsAnError() {
        assertThrows(IllegalStateException.class, () -> new JsonWriter().beginObject().name("a").value(1).toString());
        assertThrows(IllegalStateException.class, () -> new JsonWriter().name("outside"));
    }

    @Test
    void largeNumbersKeepTheirDigits() {
        String json = new JsonWriter().beginObject().name("n").value(1792414852177L).endObject().toString();
        assertTrue(json.contains("1792414852177"));
    }
}
