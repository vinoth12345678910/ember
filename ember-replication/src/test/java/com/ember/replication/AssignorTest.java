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

    // ------------------------------------------------------------------ sticky

    private static Map<String, List<PartitionRef>> prev(Map<String, List<PartitionRef>> a) {
        return a;
    }

    private static int moved(Map<String, List<PartitionRef>> before, Map<String, List<PartitionRef>> after) {
        int n = 0;
        for (var e : after.entrySet()) {
            Set<PartitionRef> had = new HashSet<>(before.getOrDefault(e.getKey(), List.of()));
            for (PartitionRef p : e.getValue()) if (!had.contains(p)) n++;
        }
        return n;
    }

    @Test
    void withNoHistoryStickyIsAsBalancedAsRange() {
        Map<String, List<PartitionRef>> a = Assignor.assignSticky(subs("a", List.of("t"), "b", List.of("t"), "c", List.of("t")), Map.of("t", 7), Map.of());
        assertEquals(List.of(3, 2, 2), List.of(a.get("a").size(), a.get("b").size(), a.get("c").size()));
    }

    @Test
    void aNewMemberTakesOnlyItsFairShareFromTheOthers() {
        Map<String, List<String>> two = subs("a", List.of("t"), "b", List.of("t"));
        Map<String, List<PartitionRef>> before = Assignor.assignSticky(two, Map.of("t", 12), Map.of());
        Map<String, List<String>> three = subs("a", List.of("t"), "b", List.of("t"), "c", List.of("t"));
        Map<String, List<PartitionRef>> after = Assignor.assignSticky(three, Map.of("t", 12), prev(before));
        assertEquals(4, after.get("a").size());
        assertEquals(4, after.get("b").size());
        assertEquals(4, after.get("c").size());
        assertEquals(4, moved(before, after), "only the newcomer's four partitions changed owner");
        Map<String, List<PartitionRef>> range = Assignor.assign(three, Map.of("t", 12));
        assertTrue(moved(before, range) > 4, "the range assignor moves more than the minimum: " + moved(before, range));
    }

    @Test
    void aLeavingMemberHandsItsPartitionsToTheRestWithoutShufflingThem() {
        Map<String, List<String>> three = subs("a", List.of("t"), "b", List.of("t"), "c", List.of("t"));
        Map<String, List<PartitionRef>> before = Assignor.assignSticky(three, Map.of("t", 12), Map.of());
        Map<String, List<PartitionRef>> after = Assignor.assignSticky(subs("a", List.of("t"), "c", List.of("t")), Map.of("t", 12), prev(before));
        assertEquals(6, after.get("a").size());
        assertEquals(6, after.get("c").size());
        assertTrue(after.get("a").containsAll(before.get("a")));
        assertTrue(after.get("c").containsAll(before.get("c")));
        assertEquals(4, moved(before, after), "exactly b's four partitions moved");
    }

    @Test
    void stickyIsAFixedPointAndDeterministic() {
        Map<String, List<String>> s = subs("a", List.of("t", "u"), "b", List.of("t"), "c", List.of("u"));
        Map<String, Integer> counts = Map.of("t", 5, "u", 4);
        Map<String, List<PartitionRef>> first = Assignor.assignSticky(s, counts, Map.of());
        assertEquals(first, Assignor.assignSticky(s, counts, first));
        assertEquals(first, Assignor.assignSticky(s, counts, Map.of()));
    }

    @Test
    void everyPartitionHasExactlyOneOwnerAndBalanceIsWithinOneForRandomChurn() {
        Random random = new Random(42);
        Map<String, Integer> counts = Map.of("t", 13, "u", 5);
        Map<String, List<PartitionRef>> current = Map.of();
        for (int round = 0; round < 300; round++) {
            Map<String, List<String>> s = new LinkedHashMap<>();
            for (int m = 0; m < 1 + random.nextInt(6); m++) {
                List<String> topics = random.nextBoolean() ? List.of("t") : random.nextBoolean() ? List.of("u") : List.of("t", "u");
                s.put("m" + m, topics);
            }
            Map<String, List<PartitionRef>> next = Assignor.assignSticky(s, counts, current);
            for (var topic : counts.entrySet()) {
                List<String> readers = s.entrySet().stream().filter(e -> e.getValue().contains(topic.getKey())).map(Map.Entry::getKey).toList();
                if (readers.isEmpty()) continue;
                Set<PartitionRef> seen = new HashSet<>();
                int min = Integer.MAX_VALUE, max = 0;
                for (String r : readers) {
                    List<PartitionRef> mine = next.get(r).stream().filter(p -> p.topic().equals(topic.getKey())).toList();
                    for (PartitionRef p : mine) assertTrue(seen.add(p), "owned twice: " + p);
                    min = Math.min(min, mine.size());
                    max = Math.max(max, mine.size());
                }
                assertEquals(topic.getValue(), seen.size(), "every partition assigned");
                assertTrue(max - min <= 1, "balanced within one in round " + round);
            }
            current = next;
        }
    }

    @Test
    void historyOfUnknownMembersOrUnsubscribedTopicsIsIgnored() {
        Map<String, List<PartitionRef>> previous = Map.of("ghost", List.of(new PartitionRef("t", 0)), "a", List.of(new PartitionRef("u", 0), new PartitionRef("t", 99)));
        Map<String, List<PartitionRef>> a = Assignor.assignSticky(subs("a", List.of("t")), Map.of("t", 3), previous);
        assertEquals(List.of(new PartitionRef("t", 0), new PartitionRef("t", 1), new PartitionRef("t", 2)), a.get("a"));
    }
}
