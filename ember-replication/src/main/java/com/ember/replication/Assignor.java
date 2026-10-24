package com.ember.replication;

import com.ember.protocol.PartitionRef;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Range assignor, computed by the coordinator leader (never inside apply: see GroupStateMachine).
 *
 * For each topic: the members subscribed to it, sorted by member id, get contiguous ranges of partitions
 * 0..P-1; with m members each gets floor(P/m) and the first (P mod m) members get one extra.
 *
 * Known downside: it is not sticky. Adding one member shifts the ranges of many members, so many partitions
 * change owner and their uncommitted work is redone. Sticky / cooperative assignors fix that, at the price of
 * remembering the previous assignment and a more complex protocol.
 */
public final class Assignor {

    private Assignor() {
    }

    /**
     * @param subscriptions memberId -> subscribed topics
     * @param partitionCounts topic -> partition count; a subscribed topic missing here does not exist yet and is skipped
     * @return memberId -> assigned partitions (sorted by topic, then partition); every member is present, possibly with none
     */
    public static Map<String, List<PartitionRef>> assign(Map<String, List<String>> subscriptions, Map<String, Integer> partitionCounts) {
        Map<String, List<PartitionRef>> result = new TreeMap<>();
        for (String member : subscriptions.keySet()) {
            result.put(member, new ArrayList<>());
        }
        for (Map.Entry<String, Integer> topic : new TreeMap<>(partitionCounts).entrySet()) {
            List<String> members = new ArrayList<>();
            for (Map.Entry<String, List<String>> s : subscriptions.entrySet()) {
                if (s.getValue().contains(topic.getKey())) {
                    members.add(s.getKey());
                }
            }
            if (members.isEmpty()) {
                continue;                                   // nobody reads this topic
            }
            members.sort(null);
            int partitions = topic.getValue();
            int base = partitions / members.size();
            int extra = partitions % members.size();
            int next = 0;
            for (int i = 0; i < members.size(); i++) {
                int count = base + (i < extra ? 1 : 0);
                for (int p = 0; p < count; p++) {
                    result.get(members.get(i)).add(new PartitionRef(topic.getKey(), next++));
                }
            }
        }
        return result;
    }

    /**
     * STICKY assignor: the same balance as {@link #assign} (every member of a topic gets floor(P/m) or ceil(P/m) partitions) but a
     * partition stays with its current owner whenever the balance allows it, so adding or removing a member moves the minimum
     * number of partitions instead of shifting every range.
     *
     * Deterministic: same inputs, same output. Members that hold more of a topic keep the larger quota; ties go by member id.
     *
     * @param previous memberId -> the partitions it holds now; entries for unknown members, partitions of topics the member
     *                 no longer subscribes to, or partitions that do not exist are ignored
     */
    public static Map<String, List<PartitionRef>> assignSticky(Map<String, List<String>> subscriptions, Map<String, Integer> partitionCounts,
                                                              Map<String, List<PartitionRef>> previous) {
        Map<String, List<PartitionRef>> result = new TreeMap<>();
        for (String member : subscriptions.keySet()) {
            result.put(member, new ArrayList<>());
        }
        for (Map.Entry<String, Integer> topic : new TreeMap<>(partitionCounts).entrySet()) {
            String name = topic.getKey();
            int partitions = topic.getValue();
            List<String> members = new ArrayList<>();
            for (Map.Entry<String, List<String>> s : subscriptions.entrySet()) {
                if (s.getValue().contains(name)) {
                    members.add(s.getKey());
                }
            }
            if (members.isEmpty()) {
                continue;
            }
            members.sort(null);
            // what each member holds of THIS topic, ascending, only valid partitions
            Map<String, List<Integer>> held = new TreeMap<>();
            for (String m : members) {
                List<Integer> mine = new ArrayList<>();
                for (PartitionRef p : previous.getOrDefault(m, List.of())) {
                    if (p.topic().equals(name) && p.partition() >= 0 && p.partition() < partitions && !mine.contains(p.partition())) {
                        mine.add(p.partition());
                    }
                }
                mine.sort(null);
                held.put(m, mine);
            }
            // quotas: the members that hold most get the larger quota (partitions mod members of them)
            int base = partitions / members.size();
            int extra = partitions % members.size();
            List<String> byHolding = new ArrayList<>(members);
            byHolding.sort((a, b) -> {
                int c = Integer.compare(held.get(b).size(), held.get(a).size());
                return c != 0 ? c : a.compareTo(b);
            });
            Map<String, Integer> quota = new TreeMap<>();
            for (int i = 0; i < byHolding.size(); i++) {
                quota.put(byHolding.get(i), base + (i < extra ? 1 : 0));
            }
            // keep what fits the quota; each partition has at most one claimant because the previous assignment was valid
            boolean[] taken = new boolean[partitions];
            Map<String, List<Integer>> keep = new TreeMap<>();
            for (String m : members) {
                List<Integer> kept = new ArrayList<>();
                for (int p : held.get(m)) {
                    if (kept.size() < quota.get(m) && !taken[p]) {
                        kept.add(p);
                        taken[p] = true;
                    }
                }
                keep.put(m, kept);
            }
            // everything else (new partitions, released ones) fills the members below their quota, in order
            int cursor = 0;
            for (String m : members) {
                List<Integer> mine = keep.get(m);
                while (mine.size() < quota.get(m)) {
                    while (taken[cursor]) {
                        cursor++;
                    }
                    taken[cursor] = true;
                    mine.add(cursor);
                }
                mine.sort(null);
                for (int p : mine) {
                    result.get(m).add(new PartitionRef(name, p));
                }
            }
        }
        return result;
    }
}
