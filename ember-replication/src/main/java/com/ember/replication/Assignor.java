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
}
