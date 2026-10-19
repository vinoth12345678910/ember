package com.ember.replication;

import com.ember.protocol.PartitionRef;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class AssignorTest {

    private static Map<String, List<String>> subs(Object... pairs) {
        Map<String, List<String>> m = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            m.put((String) pairs[i], (List<String>) pairs[i + 1]);
        }
        return m;
    }

    @Test
    void sixPartitionsThreeMembersGetTwoContiguousEach() {
        Map<String, List<PartitionRef>> a = Assignor.assign(subs("a", List.of("t"), "b", List.of("t"), "c", List.of("t")), Map.of("t", 6));
        assertEquals(List.of(new PartitionRef("t", 0), new PartitionRef("t", 1)), a.get("a"));
        assertEquals(List.of(new PartitionRef("t", 2), new PartitionRef("t", 3)), a.get("b"));
        assertEquals(List.of(new PartitionRef("t", 4), new PartitionRef("t", 5)), a.get("c"));
    }

    @Test
    void theFirstMembersGetTheExtraPartitions() {
        Map<String, List<PartitionRef>> a = Assignor.assign(subs("a", List.of("t"), "b", List.of("t"), "c", List.of("t")), Map.of("t", 7));
        assertEquals(3, a.get("a").size());
        assertEquals(2, a.get("b").size());
        assertEquals(2, a.get("c").size());
    }

    @Test
    void membersAreOrderedByIdNotByInsertionOrder() {
        Map<String, List<PartitionRef>> a = Assignor.assign(subs("z", List.of("t"), "a", List.of("t")), Map.of("t", 4));
        assertEquals(List.of(new PartitionRef("t", 0), new PartitionRef("t", 1)), a.get("a"));
        assertEquals(List.of(new PartitionRef("t", 2), new PartitionRef("t", 3)), a.get("z"));
    }

    @Test
    void oneMemberGetsEverything() {
        Map<String, List<PartitionRef>> a = Assignor.assign(subs("only", List.of("t")), Map.of("t", 5));
        assertEquals(5, a.get("only").size());
    }

    @Test
    void extraMembersGetNothingButStayInTheResult() {
        Map<String, List<PartitionRef>> a = Assignor.assign(subs("a", List.of("t"), "b", List.of("t"), "c", List.of("t")), Map.of("t", 2));
        assertEquals(1, a.get("a").size());
        assertEquals(1, a.get("b").size());
        assertEquals(List.of(), a.get("c"));
    }

    @Test
    void differentSubscriptionsAreRespected() {
        Map<String, List<PartitionRef>> a = Assignor.assign(
                subs("a", List.of("x", "y"), "b", List.of("y"), "c", List.of("x")), Map.of("x", 4, "y", 2));
        assertEquals(Set.of(new PartitionRef("x", 0), new PartitionRef("x", 1), new PartitionRef("y", 0)), new HashSet<>(a.get("a")));
        assertEquals(List.of(new PartitionRef("y", 1)), a.get("b"));
        assertEquals(List.of(new PartitionRef("x", 2), new PartitionRef("x", 3)), a.get("c"));
    }

    @Test
    void aTopicThatDoesNotExistYetAndATopicNobodyReadsAreSimplyUnassigned() {
        Map<String, List<PartitionRef>> a = Assignor.assign(subs("a", List.of("ghost", "t")), Map.of("t", 2, "unread", 3));
        assertEquals(List.of(new PartitionRef("t", 0), new PartitionRef("t", 1)), a.get("a"));
        assertEquals(Map.of("a", List.of()), Assignor.assign(subs("a", List.of("ghost")), Map.of()));
    }

    @Test
    void noMembersGivesAnEmptyResult() {
        assertEquals(Map.of(), Assignor.assign(Map.of(), Map.of("t", 3)));
    }

    @Test
    void twoThousandRandomInputsKeepAllTheRules() {
        Random rnd = new Random(2026);
        for (int round = 0; round < 2000; round++) {
            int memberCount = rnd.nextInt(8);
            int topicCount = 1 + rnd.nextInt(4);
            Map<String, Integer> counts = new TreeMap<>();
            for (int t = 0; t < topicCount; t++) {
                if (rnd.nextInt(6) != 0) counts.put("t" + t, 1 + rnd.nextInt(13));       // some topics do not exist
            }
            Map<String, List<String>> subscriptions = new LinkedHashMap<>();
            for (int m = 0; m < memberCount; m++) {
                List<String> topics = new ArrayList<>();
                for (int t = 0; t < topicCount; t++) if (rnd.nextBoolean()) topics.add("t" + t);
                if (topics.isEmpty()) topics.add("t0");
                subscriptions.put("m" + rnd.nextInt(1000) + "-" + m, topics);
            }
            Map<String, List<PartitionRef>> a = Assignor.assign(subscriptions, counts);
            assertEquals(a, Assignor.assign(subscriptions, counts), "deterministic");
            assertEquals(subscriptions.keySet(), a.keySet(), "every member present");

            Map<PartitionRef, String> owner = new HashMap<>();
            for (var e : a.entrySet()) {
                for (PartitionRef p : e.getValue()) {
                    assertTrue(subscriptions.get(e.getKey()).contains(p.topic()), "only subscribed topics");
                    assertTrue(counts.containsKey(p.topic()) && p.partition() < counts.get(p.topic()), "only existing partitions");
                    assertNull(owner.put(p, e.getKey()), "exactly one owner for " + p);
                }
            }
            for (var topic : counts.entrySet()) {
                List<String> readers = subscriptions.entrySet().stream().filter(s -> s.getValue().contains(topic.getKey())).map(Map.Entry::getKey).sorted().toList();
                if (readers.isEmpty()) continue;
                int min = Integer.MAX_VALUE, max = 0, previousEnd = 0;
                for (String reader : readers) {
                    List<Integer> mine = a.get(reader).stream().filter(p -> p.topic().equals(topic.getKey())).map(PartitionRef::partition).toList();
                    min = Math.min(min, mine.size());
                    max = Math.max(max, mine.size());
                    for (int i = 0; i < mine.size(); i++) assertEquals(previousEnd + i, mine.get(i), "contiguous ranges in member order");
                    previousEnd += mine.size();
                }
                assertTrue(max - min <= 1, "balanced within one");
                assertEquals(topic.getValue(), previousEnd, "every partition assigned");
            }
        }
    }
}
