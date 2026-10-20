package com.ember.chaos;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.ember.chaos.H.row;
import static org.junit.jupiter.api.Assertions.*;

/**
 * A checker that passes everything is worthless. Take a known-good history, inject exactly ONE bug, and require the right
 * violation code; the untouched history must report nothing. (Like the Phase 4 mutation tests of Raft.)
 */
class CheckerMutationTest {

    /** 2 producers, 3 partitions, keyed records, unknown outcomes, readers, 2 group consumers, commits, 3 identical replicas. */
    static History goodHistory() {
        H h = new H();
        long[] next = new long[3];
        Map<Integer, List<Object[]>> logs = new LinkedHashMap<>();
        for (int p = 0; p < 3; p++) logs.put(p, new ArrayList<>());
        List<Object[]> deliveries = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            for (String producer : new String[]{"p1", "p2"}) {
                String key = "key-" + (i % 5);
                int partition = (i % 5) % 3;
                String value = producer + "-" + i;
                if (i == 7 && producer.equals("p1")) {
                    h.produce(producer, "t", partition, key, value, Outcome.INFO, -1);                  // unknown, and it WAS stored
                    long o = next[partition]++;
                    logs.get(partition).add(row(o, key, value));
                    deliveries.add(new Object[]{"t", partition, o, value});
                } else if (i == 9 && producer.equals("p2")) {
                    h.produce(producer, "t", partition, key, value, Outcome.INFO, -1);                  // unknown, and it was NOT stored
                } else if (i == 11 && producer.equals("p1")) {
                    h.produce(producer, "t", partition, key, value, Outcome.FAIL, -1);                  // refused
                } else {
                    long o = next[partition]++;
                    h.produce(producer, "t", partition, key, value, Outcome.OK, o);
                    logs.get(partition).add(row(o, key, value));
                    deliveries.add(new Object[]{"t", partition, o, value});
                }
            }
            if (i == 14) {                                                                                // a mid-run read by reader-1
                List<Object[]> rows = logs.get(0).subList(0, Math.min(6, logs.get(0).size()));
                h.read("reader-1", "t", 0, false, 0, logs.get(0).size(), rows.toArray(new Object[0][]));
            }
        }
        h.poll("c1", 1, deliveries.subList(0, 40).toArray(new Object[0][]));
        h.commit("m1", 1, Outcome.OK, new Object[]{"t", 0, 3L});
        h.poll("c2", 2, deliveries.subList(30, deliveries.size()).toArray(new Object[0][]));
        h.poll("c2", 2, deliveries.subList(35, 40).toArray(new Object[0][]));                           // some duplicates
        h.commit("m2", 2, Outcome.OK, new Object[]{"t", 1, 4L}, new Object[]{"t", 2, 4L});
        for (int p = 0; p < 3; p++) {
            h.read("final", "t", p, true, 0, next[p], logs.get(p).toArray(new Object[0][]));
        }
        for (int r = 1; r <= 3; r++) {
            for (int p = 0; p < 3; p++) h.replicaData("t-" + p, r, 0, next[p], 1000 + p);
            h.replicaRaft("t-0", r, 60, 4242, "1:1,20:2");
            h.replicaState("groupstate", r, 5);
            h.replicaState("metastate", r, 6);
        }
        return h.history();
    }

    private static History mutate(History h, Function<List<HistoryEvent>, List<HistoryEvent>> change) {
        return new History(change.apply(new ArrayList<>(h.events())));
    }

    private static HistoryEvent withData(HistoryEvent e, Map<String, Object> data) {
        return new HistoryEvent(e.timeNanos(), e.kind(), e.opId(), e.process(), e.type(), e.outcome(), data);
    }

    private static Map<String, Object> copy(HistoryEvent e) {
        return new LinkedHashMap<>(e.data());
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> rowsOf(HistoryEvent e) {
        return new ArrayList<>((List<List<Object>>) e.data().get("records"));
    }

    private static int indexOfFinalRead(List<HistoryEvent> events, int partition) {
        for (int i = 0; i < events.size(); i++) {
            HistoryEvent e = events.get(i);
            if (e.kind().equals("complete") && e.type().equals("read") && e.process().equals("final")) {
                if (partition-- == 0) return i;
            }
        }
        throw new AssertionError("no final read");
    }

    @Test
    void theUntouchedHistoryReportsNothing() {
        Checker.Result r = Checker.check(goodHistory());
        assertTrue(r.ok(), r.violations().toString());
        assertTrue(r.stats().get("produce.ok") > 50);
        assertTrue(r.stats().get("deliveries.duplicates") > 0, "the good history contains harmless duplicate deliveries");
    }

    @Test
    void m1DroppingAnAcknowledgedRecordFromTheFinalReadIsCaught() {
        History bad = mutate(goodHistory(), ev -> {
            int i = indexOfFinalRead(ev, 1);
            Map<String, Object> d = copy(ev.get(i));
            List<List<Object>> rows = rowsOf(ev.get(i));
            rows.remove(2);
            d.put("records", rows);
            ev.set(i, withData(ev.get(i), d));
            return ev;
        });
        assertTrue(Checker.check(bad).has("P1_LOST_ACK"), Checker.check(bad).violations().toString());
    }

    @Test
    void m2ReturningTwoDifferentValuesForOneOffsetIsCaught() {
        History bad = mutate(goodHistory(), ev -> {
            for (int i = 0; i < ev.size(); i++) {
                HistoryEvent e = ev.get(i);
                if (e.kind().equals("complete") && e.type().equals("read") && e.process().equals("reader-1")) {
                    Map<String, Object> d = copy(e);
                    List<List<Object>> rows = rowsOf(e);
                    List<Object> first = new ArrayList<>(rows.get(0));
                    first.set(2, "p1-0");                                   // the final read says p2-0 or whatever is at offset 0
                    List<Object> original = rows.get(0);
                    first.set(2, original.get(2).equals("p1-0") ? "p2-0" : "p1-0");
                    rows.set(0, first);
                    d.put("records", rows);
                    ev.set(i, withData(e, d));
                    break;
                }
            }
            return ev;
        });
        assertTrue(Checker.check(bad).has("P2_DIVERGENCE"), Checker.check(bad).violations().toString());
    }

    @Test
    void m3AcknowledgingTheSameOffsetTwiceIsCaught() {
        History bad = mutate(goodHistory(), ev -> {
            List<Integer> okCompletes = new ArrayList<>();
            for (int i = 0; i < ev.size(); i++) {
                HistoryEvent e = ev.get(i);
                if (e.kind().equals("complete") && e.type().equals("produce") && e.outcome() == Outcome.OK) okCompletes.add(i);
            }
            long firstOffset = ev.get(okCompletes.get(0)).num("offset");
            Map<String, Object> d = copy(ev.get(okCompletes.get(3)));
            d.put("offset", firstOffset);
            ev.set(okCompletes.get(3), withData(ev.get(okCompletes.get(3)), d));
            return ev;
        });
        Checker.Result r = Checker.check(bad);
        assertTrue(r.has("P4_DUPLICATE_OFFSET") || r.has("P1_LOST_ACK"), r.violations().toString());
    }

    @Test
    void m3bAcknowledgingTheSameOffsetOnTheSamePartitionIsCaughtAsDuplicateOffset() {
        History bad = mutate(goodHistory(), ev -> {
            int firstIndex = -1;
            for (int i = 0; i < ev.size(); i++) {
                HistoryEvent e = ev.get(i);
                if (e.kind().equals("complete") && e.type().equals("produce") && e.outcome() == Outcome.OK) {
                    if (firstIndex < 0) {
                        firstIndex = i;
                    } else if (ev.get(i - 1).str("partition") != null && ev.get(i - 1).num("partition") == ev.get(firstIndex - 1).num("partition")) {
                        Map<String, Object> d = copy(e);
                        d.put("offset", ev.get(firstIndex).num("offset"));
                        ev.set(i, withData(e, d));
                        return ev;
                    }
                }
            }
            throw new AssertionError("no second acknowledgement on the same partition");
        });
        assertTrue(Checker.check(bad).has("P4_DUPLICATE_OFFSET"), Checker.check(bad).violations().toString());
    }

    @Test
    void m4InventingAValueIsCaught() {
        History bad = mutate(goodHistory(), ev -> {
            int i = indexOfFinalRead(ev, 0);
            Map<String, Object> d = copy(ev.get(i));
            List<List<Object>> rows = rowsOf(ev.get(i));
            rows.add(List.of(9999L, "key-x", "never-sent-by-anyone"));
            d.put("records", rows);
            ev.set(i, withData(ev.get(i), d));
            return ev;
        });
        assertTrue(Checker.check(bad).has("P3_PHANTOM"), Checker.check(bad).violations().toString());
    }

    @Test
    void m5SwappingTwoKeyedRecordsIsCaught() {
        History bad = mutate(goodHistory(), ev -> {
            int i = indexOfFinalRead(ev, 0);                        // partition 0 holds keys key-0 and key-3
            Map<String, Object> d = copy(ev.get(i));
            List<List<Object>> rows = rowsOf(ev.get(i));
            // find two records of the same producer and the same key and swap their values
            int a = -1, b = -1;
            for (int x = 0; x < rows.size() && b < 0; x++) {
                for (int y = x + 1; y < rows.size(); y++) {
                    String vx = (String) rows.get(x).get(2), vy = (String) rows.get(y).get(2);
                    if (rows.get(x).get(1).equals(rows.get(y).get(1)) && vx.substring(0, 2).equals(vy.substring(0, 2))) {
                        a = x;
                        b = y;
                        break;
                    }
                }
            }
            assertTrue(b > 0, "the history must contain two records of one producer and key in partition 0");
            List<Object> ra = new ArrayList<>(rows.get(a));
            List<Object> rb = new ArrayList<>(rows.get(b));
            Object tmp = ra.get(2);
            ra.set(2, rb.get(2));
            rb.set(2, tmp);
            rows.set(a, ra);
            rows.set(b, rb);
            d.put("records", rows);
            ev.set(i, withData(ev.get(i), d));
            return ev;
        });
        assertTrue(Checker.check(bad).has("P5_KEY_ORDER"), Checker.check(bad).violations().toString());
    }

    @Test
    void m6AReadThatShrinksIsCaught() {
        History bad = mutate(goodHistory(), ev -> {
            for (int i = 0; i < ev.size(); i++) {
                HistoryEvent e = ev.get(i);
                if (e.kind().equals("complete") && e.type().equals("read") && e.process().equals("reader-1")) {
                    Map<String, Object> d = new LinkedHashMap<>();
                    d.put("records", List.of());
                    d.put("start", 0L);
                    d.put("end", 2L);                                 // the same reader saw offset 5 earlier
                    HistoryEvent inv = new HistoryEvent(e.timeNanos() + 1, "invoke", 888_001, "reader-1", "read", null,
                            Map.of("topic", "t", "partition", 0L, "from", 0L, "final", false));
                    HistoryEvent done = new HistoryEvent(e.timeNanos() + 2, "complete", 888_001, "reader-1", "read", Outcome.OK, d);
                    ev.add(i + 1, inv);
                    ev.add(i + 2, done);
                    break;
                }
            }
            return ev;
        });
        assertTrue(Checker.check(bad).has("P6_SHRINK"), Checker.check(bad).violations().toString());
    }

    @Test
    void m7AcceptingAStaleGenerationCommitIsCaught() {
        History bad = mutate(goodHistory(), ev -> {
            long late = ev.get(ev.size() - 1).timeNanos() + 10;
            ev.add(new HistoryEvent(late, "invoke", 888_002, "m1", "commit", null,
                    Map.of("group", "g", "member", "m1", "generation", 1L, "offsets", List.of(List.of("t", 0L, 1L)))));
            ev.add(new HistoryEvent(late + 1, "complete", 888_002, "m1", "commit", Outcome.OK, Map.of()));
            return ev;
        });
        assertTrue(Checker.check(bad).has("P8_ZOMBIE_COMMIT"), Checker.check(bad).violations().toString());
    }

    @Test
    void m8ReplicaDataThatDiffersByOneByteIsCaught() {
        History bad = mutate(goodHistory(), ev -> {
            for (int i = 0; i < ev.size(); i++) {
                HistoryEvent e = ev.get(i);
                if (e.kind().equals("replica") && "data".equals(e.str("scope")) && "2".equals(e.str("replica")) && "t-1".equals(e.str("group"))) {
                    Map<String, Object> d = copy(e);
                    d.put("crc", e.num("crc") ^ 1);
                    ev.set(i, withData(e, d));
                    break;
                }
            }
            return ev;
        });
        assertTrue(Checker.check(bad).has("P9_DATA_DIVERGENCE"), Checker.check(bad).violations().toString());
    }

    @Test
    void m9ADeliveryThatNeverHappenedIsCaught() {
        History bad = mutate(goodHistory(), ev -> {
            ev.removeIf(e -> e.type().equals("poll"));              // nobody was ever delivered anything, yet records were acknowledged
            long late = ev.get(ev.size() - 1).timeNanos() + 10;
            ev.add(new HistoryEvent(late, "invoke", 888_003, "c9", "poll", null, Map.of("consumer", "c9", "group", "g")));
            ev.add(new HistoryEvent(late + 1, "complete", 888_003, "c9", "poll", Outcome.OK, Map.of("records", List.of(), "generation", 1L, "joinCount", 1L)));
            return ev;
        });
        assertTrue(Checker.check(bad).has("P7_NOT_DELIVERED"), Checker.check(bad).violations().toString());
    }

    @Test
    void m10ACommitBeyondTheLogEndIsCaught() {
        History bad = mutate(goodHistory(), ev -> {
            long late = ev.get(ev.size() - 1).timeNanos() + 10;
            ev.add(new HistoryEvent(late, "invoke", 888_004, "m2", "commit", null,
                    Map.of("group", "g", "member", "m2", "generation", 2L, "offsets", List.of(List.of("t", 1L, 100_000L)))));
            ev.add(new HistoryEvent(late + 1, "complete", 888_004, "m2", "commit", Outcome.OK, Map.of()));
            return ev;
        });
        assertTrue(Checker.check(bad).has("P8_COMMIT_BEYOND_END"), Checker.check(bad).violations().toString());
    }

    @Test
    void m11ADivergentRaftLogOrGroupStateIsCaught() {
        History raft = mutate(goodHistory(), ev -> {
            for (int i = 0; i < ev.size(); i++) {
                HistoryEvent e = ev.get(i);
                if (e.kind().equals("replica") && "raftlog".equals(e.str("scope")) && "3".equals(e.str("replica"))) {
                    Map<String, Object> d = copy(e);
                    d.put("digest", e.num("digest") + 1);
                    ev.set(i, withData(e, d));
                }
            }
            return ev;
        });
        assertTrue(Checker.check(raft).has("P9_RAFT_DIVERGENCE"));
        History state = mutate(goodHistory(), ev -> {
            for (int i = 0; i < ev.size(); i++) {
                HistoryEvent e = ev.get(i);
                if (e.kind().equals("replica") && "groupstate".equals(e.str("scope")) && "1".equals(e.str("replica"))) {
                    Map<String, Object> d = copy(e);
                    d.put("digest", 12345L);
                    ev.set(i, withData(e, d));
                }
            }
            return ev;
        });
        assertTrue(Checker.check(state).has("P9_GROUP_STATE_DIVERGENCE"));
    }

    @Test
    void m12AFailedWriteThatBecameVisibleIsCaught() {
        History bad = mutate(goodHistory(), ev -> {
            String failedValue = null;
            for (HistoryEvent e : ev) {
                if (e.kind().equals("complete") && e.type().equals("produce") && e.outcome() == Outcome.FAIL) {
                    for (HistoryEvent in : ev) if (in.kind().equals("invoke") && in.opId() == e.opId()) failedValue = in.str("value");
                }
            }
            assertNotNull(failedValue);
            int i = indexOfFinalRead(ev, 0);
            Map<String, Object> d = copy(ev.get(i));
            List<List<Object>> rows = rowsOf(ev.get(i));
            rows.add(List.of(5000L, "key-1", failedValue));
            d.put("records", rows);
            ev.set(i, withData(ev.get(i), d));
            return ev;
        });
        assertTrue(Checker.check(bad).has("P3_FAILED_WRITE_VISIBLE"), Checker.check(bad).violations().toString());
    }

    @Test
    void aHistoryFileRoundTripsAndStillChecksTheSame(@org.junit.jupiter.api.io.TempDir Path dir) throws Exception {
        History good = goodHistory();
        Path file = dir.resolve("history.jsonl");
        good.write(file);
        History back = History.read(file);
        assertEquals(good.events().size(), back.events().size());
        assertTrue(Files.readAllLines(file).get(0).startsWith("{\"t\":"));
        assertTrue(Checker.check(back).ok(), Checker.check(back).violations().toString());
        // a history recorded on the fly into a file is checkable offline too
        Path live = dir.resolve("live.jsonl");
        try (HistoryRecorder rec = new HistoryRecorder(live)) {
            long id = rec.invoke("p1", "produce", HistoryRecorder.data("topic", "t", "partition", 0, "key", "k", "value", "v\n\"quoted\""));
            rec.complete(id, "p1", "produce", Outcome.OK, HistoryRecorder.data("offset", 0, "retried", false));
            rec.nemesis("crash", "broker-1", "start");
        }
        History fromFile = History.read(live);
        assertEquals(3, fromFile.events().size());
        assertEquals("v\n\"quoted\"", fromFile.events().get(0).str("value"));
        assertTrue(Checker.check(fromFile).has("P1_NO_FINAL_READ"), "the acknowledged write has no final read in this tiny history");
    }

    @Test
    void jsonRoundTripsNestedValuesAndRejectsGarbage() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("a", 1L);
        m.put("b", List.of("x", 2.5, true, List.of()));
        m.put("c", null);
        m.put("d", "line1\nline2 \"q\" \\ \u0001");
        Object back = Json.parse(Json.write(m));
        assertEquals(Json.write(m), Json.write(back));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"a\":1} extra"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"a\":"));
    }
}
