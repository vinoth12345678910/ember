package com.ember.chaos;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * The independent exam marker: a PURE function from a recorded history to a list of violations. It trusts nothing the brokers
 * say about themselves, only what clients were told and what they read.
 *
 *  P1 no acknowledged write is lost        P2 no two reads disagree about one offset     P3 no phantom (or failed) writes
 *  P4 offsets unique and increasing        P5 per-key order is kept                       P6 reads never shrink
 *  P7 every acknowledged record reaches the group   P8 commits stay inside the log and no zombie commit is accepted
 *  P9 replicas agree (data logs, Raft logs, group and topic state)
 *
 * It is a test, not a proof: it can only find violations that this history makes visible.
 */
public final class Checker {

    public record Result(List<Violation> violations, Map<String, Long> stats) {
        public boolean ok() {
            return violations.isEmpty();
        }

        public boolean has(String code) {
            return violations.stream().anyMatch(v -> v.code().equals(code));
        }
    }

    private static final int MAX_PER_CODE = 40;

    private record Rec(String key, String value) {
    }

    private record Produce(long opId, String process, String topic, int partition, String key, String value, Outcome outcome,
                           long offset, boolean retried, long completeTime) {
        String tp() {
            return topic + "-" + partition;
        }
    }

    private final List<Violation> violations = new ArrayList<>();
    private final Map<String, Integer> perCode = new HashMap<>();
    private final Map<String, Long> stats = new LinkedHashMap<>();
    private final History history;

    private final List<Produce> produces = new ArrayList<>();
    private final Map<String, TreeMap<Long, Rec>> finalLog = new HashMap<>();     // topic-partition -> offset -> record
    private final Map<String, Long> finalStart = new HashMap<>();
    private final Map<String, Long> finalEnd = new HashMap<>();

    private Checker(History history) {
        this.history = history;
    }

    public static Result check(History history) {
        Checker c = new Checker(history);
        c.load();
        c.p1LostAcknowledgedWrite();
        c.p2Divergence();
        c.p3Phantoms();
        c.p4Offsets();
        c.p5KeyOrder();
        c.p6ReadsNeverShrink();
        c.p7GroupDelivery();
        c.p8Commits();
        c.p9ReplicaAgreement();
        return new Result(List.copyOf(c.violations), c.stats);
    }

    private void violate(String code, String message) {
        int n = perCode.merge(code, 1, Integer::sum);
        if (n <= MAX_PER_CODE) {
            violations.add(new Violation(code, message));
        } else if (n == MAX_PER_CODE + 1) {
            violations.add(new Violation(code, "... and more of the same kind (output limited)"));
        }
    }

    // ------------------------------------------------------------------ loading

    private void load() {
        long ok = 0, info = 0, fail = 0;
        for (History.Op op : history.ops("produce")) {
            HistoryEvent in = op.invoke();
            HistoryEvent done = op.complete();
            Outcome outcome = op.outcome();
            produces.add(new Produce(in.opId(), in.process(), in.str("topic"), (int) in.num("partition"), in.str("key"), in.str("value"), outcome,
                    done == null ? -1 : done.num("offset"), done != null && done.flag("retried"), done == null ? Long.MAX_VALUE : done.timeNanos()));
            switch (outcome) {
                case OK -> ok++;
                case FAIL -> fail++;
                case INFO -> info++;
            }
        }
        stats.put("produce.ok", ok);
        stats.put("produce.fail", fail);
        stats.put("produce.info", info);
        for (History.Op op : history.ops("read")) {
            if (op.complete() == null || op.outcome() != Outcome.OK || !op.invoke().flag("final")) continue;
            String tp = op.invoke().str("topic") + "-" + op.invoke().num("partition");
            TreeMap<Long, Rec> log = finalLog.computeIfAbsent(tp, k -> new TreeMap<>());
            for (List<Object> row : op.complete().rows("records")) {
                log.put(((Number) row.get(0)).longValue(), new Rec((String) row.get(1), (String) row.get(2)));
            }
            finalStart.merge(tp, op.complete().num("start"), Math::min);
            finalEnd.merge(tp, op.complete().num("end"), Math::max);
        }
        long nemesis = 0;
        for (HistoryEvent e : history.ofKind(HistoryEvent.NEMESIS)) {
            if ("start".equals(e.str("phase"))) {
                nemesis++;
                stats.merge("nemesis." + e.str("fault"), 1L, Long::sum);
            }
        }
        stats.put("nemesis.total", nemesis);
        stats.put("reads", (long) history.ops("read").size());
        stats.put("polls", (long) history.ops("poll").size());
        stats.put("commits", (long) history.ops("commit").size());
    }

    // ------------------------------------------------------------------ P1

    private void p1LostAcknowledgedWrite() {
        Set<String> reportedMissingRead = new HashSet<>();
        for (Produce p : produces) {
            if (p.outcome() != Outcome.OK) continue;
            TreeMap<Long, Rec> log = finalLog.get(p.tp());
            if (log == null) {
                if (reportedMissingRead.add(p.tp())) violate("P1_NO_FINAL_READ", "no final read of " + p.tp() + ": acknowledged writes cannot be verified");
                continue;
            }
            Long start = finalStart.get(p.tp());
            if (start != null && p.offset() < start) continue;                  // retention removed it legitimately
            Rec found = log.get(p.offset());
            if (found == null) {
                violate("P1_LOST_ACK", "acknowledged value " + p.value() + " at " + p.tp() + "@" + p.offset() + " is missing from the final read");
            } else if (!Objects.equals(found.value(), p.value()) || !Objects.equals(found.key(), p.key())) {
                violate("P1_LOST_ACK", "acknowledged " + p.value() + " at " + p.tp() + "@" + p.offset() + " but the log holds " + found.value()
                        + " (key " + found.key() + " instead of " + p.key() + ")");
            }
        }
    }

    // ------------------------------------------------------------------ P2

    private void p2Divergence() {
        Map<String, Rec> seen = new HashMap<>();             // "tp@offset" -> first record read there
        Map<String, String> seenBy = new HashMap<>();
        for (History.Op op : history.ops("read")) {
            if (op.complete() == null) continue;
            String tp = op.invoke().str("topic") + "-" + op.invoke().num("partition");
            for (List<Object> row : op.complete().rows("records")) {
                String at = tp + "@" + ((Number) row.get(0)).longValue();
                Rec rec = new Rec((String) row.get(1), (String) row.get(2));
                Rec before = seen.putIfAbsent(at, rec);
                if (before != null && !before.equals(rec)) {
                    violate("P2_DIVERGENCE", at + " was read as " + before.value() + " (by " + seenBy.get(at) + ") and as " + rec.value() + " (by " + op.invoke().process() + ")");
                } else if (before == null) {
                    seenBy.put(at, op.invoke().process());
                }
            }
        }
        for (History.Op op : history.ops("poll")) {                       // deliveries to the group must agree with reads too
            if (op.complete() == null) continue;
            for (List<Object> row : op.complete().rows("records")) {
                String at = row.get(0) + "-" + ((Number) row.get(1)).longValue() + "@" + ((Number) row.get(2)).longValue();
                Rec before = seen.get(at);
                if (before != null && !Objects.equals(before.value(), row.get(3))) {
                    violate("P2_DIVERGENCE", at + " was delivered to " + op.invoke().str("consumer") + " as " + row.get(3) + " but read as " + before.value());
                }
            }
        }
    }

    // ------------------------------------------------------------------ P3

    private void p3Phantoms() {
        Set<String> possible = new HashSet<>();                           // OK or unknown: allowed to exist
        Set<String> failed = new HashSet<>();
        for (Produce p : produces) {
            (p.outcome() == Outcome.FAIL ? failed : possible).add(p.value());
        }
        for (History.Op op : history.ops("read")) {
            if (op.complete() == null) continue;
            for (List<Object> row : op.complete().rows("records")) {
                checkInvented((String) row.get(2), possible, failed, "read by " + op.invoke().process());
            }
        }
    }

    private void checkInvented(String value, Set<String> possible, Set<String> failed, String where) {
        if (possible.contains(value)) return;
        if (failed.contains(value)) {
            violate("P3_FAILED_WRITE_VISIBLE", "value " + value + " (a produce that definitely FAILED) was " + where);
        } else {
            violate("P3_PHANTOM", "value " + value + " was " + where + " but no produce ever sent it");
        }
    }

    // ------------------------------------------------------------------ P4

    private void p4Offsets() {
        Map<String, Produce> byOffset = new HashMap<>();
        for (Produce p : produces) {
            if (p.outcome() != Outcome.OK) continue;
            Produce other = byOffset.putIfAbsent(p.tp() + "@" + p.offset(), p);
            if (other != null) {
                violate("P4_DUPLICATE_OFFSET", p.value() + " and " + other.value() + " were both acknowledged at " + p.tp() + "@" + p.offset());
            }
        }
        Map<String, List<Produce>> perProducerPartition = new HashMap<>();
        for (Produce p : produces) {
            if (p.outcome() == Outcome.OK && !p.retried()) {
                perProducerPartition.computeIfAbsent(p.process() + "/" + p.tp(), k -> new ArrayList<>()).add(p);
            }
        }
        perProducerPartition.forEach((who, list) -> {
            list.sort(Comparator.comparingLong(Produce::completeTime));
            for (int i = 1; i < list.size(); i++) {
                if (list.get(i).offset() <= list.get(i - 1).offset()) {
                    violate("P4_NOT_INCREASING", who + ": " + list.get(i).value() + " acknowledged at offset " + list.get(i).offset()
                            + " after " + list.get(i - 1).value() + " at " + list.get(i - 1).offset());
                }
            }
        });
    }

    // ------------------------------------------------------------------ P5

    private void p5KeyOrder() {
        Map<String, Map<String, Long>> firstPosition = new HashMap<>();   // tp -> value -> first offset in the final log
        finalLog.forEach((tp, log) -> {
            Map<String, Long> m = new HashMap<>();
            log.forEach((offset, rec) -> m.putIfAbsent(rec.value(), offset));
            firstPosition.put(tp, m);
        });
        Map<String, List<Produce>> perProducerKey = new HashMap<>();
        for (Produce p : produces) {
            if (p.outcome() == Outcome.OK && p.key() != null) {
                perProducerKey.computeIfAbsent(p.process() + "/" + p.key(), k -> new ArrayList<>()).add(p);
            }
        }
        perProducerKey.forEach((who, list) -> {
            list.sort(Comparator.comparingLong(Produce::opId));           // send order (a producer sends one at a time)
            Produce previous = null;
            long previousPosition = -1;
            for (Produce p : list) {
                Map<String, Long> positions = firstPosition.get(p.tp());
                Long position = positions == null ? null : positions.get(p.value());
                if (position == null) continue;                           // missing: P1 reports it
                if (previous != null && (!previous.tp().equals(p.tp()) || position <= previousPosition)) {
                    violate("P5_KEY_ORDER", who + ": " + p.value() + " (position " + p.tp() + "@" + position + ") must come after " + previous.value()
                            + " (position " + previous.tp() + "@" + previousPosition + ")");
                }
                previous = p;
                previousPosition = position;
            }
        });
    }

    // ------------------------------------------------------------------ P6

    private void p6ReadsNeverShrink() {
        Map<String, Long> highestSeen = new HashMap<>();                  // reader/tp -> highest offset it has been given
        List<History.Op> reads = new ArrayList<>(history.ops("read"));
        reads.removeIf(r -> r.complete() == null || r.outcome() != Outcome.OK);
        reads.sort(Comparator.comparingLong(r -> r.complete().timeNanos()));
        for (History.Op op : reads) {
            String who = op.invoke().process() + "/" + op.invoke().str("topic") + "-" + op.invoke().num("partition");
            long end = op.complete().num("end");
            long start = op.complete().num("start");
            Long before = highestSeen.get(who);
            if (before != null && end <= before && start <= before) {
                violate("P6_SHRINK", who + " saw offset " + before + " earlier but a later read reports the log ending at " + end);
            }
            long max = -1;
            for (List<Object> row : op.complete().rows("records")) max = Math.max(max, ((Number) row.get(0)).longValue());
            if (max >= 0) highestSeen.merge(who, max, Math::max);
        }
    }

    // ------------------------------------------------------------------ P7

    private void p7GroupDelivery() {
        List<History.Op> polls = history.ops("poll");
        if (polls.isEmpty()) return;
        Set<String> possible = new HashSet<>();
        for (Produce p : produces) if (p.outcome() != Outcome.FAIL) possible.add(p.value());
        Set<String> delivered = new HashSet<>();
        long deliveries = 0;
        for (History.Op op : polls) {
            if (op.complete() == null) continue;
            for (List<Object> row : op.complete().rows("records")) {
                String value = (String) row.get(3);
                deliveries++;
                delivered.add(value);
                if (!possible.contains(value)) {
                    violate("P7_PHANTOM_DELIVERY", "value " + value + " was delivered to " + op.invoke().str("consumer") + " but no produce ever sent it");
                }
            }
        }
        for (Produce p : produces) {
            if (p.outcome() == Outcome.OK && !delivered.contains(p.value())) {
                violate("P7_NOT_DELIVERED", "acknowledged value " + p.value() + " (" + p.tp() + "@" + p.offset() + ") was never delivered to the group");
            }
        }
        stats.put("deliveries", deliveries);
        stats.put("deliveries.distinct", (long) delivered.size());
        stats.put("deliveries.duplicates", deliveries - delivered.size());
    }

    // ------------------------------------------------------------------ P8

    private void p8Commits() {
        List<History.Op> commits = history.ops("commit");
        if (commits.isEmpty()) return;
        List<HistoryEvent> polls = new ArrayList<>();
        for (History.Op op : history.ops("poll")) if (op.complete() != null) polls.add(op.complete());
        for (History.Op op : commits) {
            if (op.outcome() != Outcome.OK) continue;
            HistoryEvent in = op.invoke();
            for (List<Object> row : in.rows("offsets")) {
                String tp = row.get(0) + "-" + ((Number) row.get(1)).longValue();
                long offset = ((Number) row.get(2)).longValue();
                Long end = finalEnd.get(tp);
                TreeMap<Long, Rec> log = finalLog.get(tp);
                if (log != null && !log.isEmpty()) end = Math.max(end == null ? 0 : end, log.lastKey() + 1);
                if (end != null && offset > end) {
                    violate("P8_COMMIT_BEYOND_END", "group " + in.str("group") + " committed " + tp + "@" + offset + " but the log ends at " + end);
                }
            }
            long generation = Math.max(in.num("generation"), op.complete() == null ? -1 : op.complete().num("generationAfter"));   // lenient: the consumer may have moved on during the call
            for (HistoryEvent poll : polls) {
                if (poll.timeNanos() < in.timeNanos() && poll.num("generation") > generation) {
                    violate("P8_ZOMBIE_COMMIT", "member " + in.str("member") + " committed in generation " + generation + " and it was ACKNOWLEDGED, although "
                            + poll.process() + " had already seen generation " + poll.num("generation"));
                    break;
                }
            }
        }
    }

    // ------------------------------------------------------------------ P9

    private void p9ReplicaAgreement() {
        Map<String, Map<String, List<String>>> byScopeGroup = new LinkedHashMap<>();    // "scope|group" -> digest -> replicas
        Map<String, String> detail = new HashMap<>();
        for (HistoryEvent e : history.ofKind(HistoryEvent.REPLICA)) {
            String scope = e.str("scope");
            String group = e.str("group") == null ? "" : e.str("group");
            String digest = switch (scope) {
                case "data" -> "start=" + e.num("start") + " end=" + e.num("end") + " crc=" + e.num("crc");
                case "raftlog" -> "upTo=" + e.num("index") + " digest=" + e.num("digest") + " terms=" + e.str("terms");
                default -> "digest=" + e.num("digest");
            };
            byScopeGroup.computeIfAbsent(scope + "|" + group, k -> new LinkedHashMap<>())
                    .computeIfAbsent(digest, k -> new ArrayList<>()).add("replica " + e.str("replica"));
            detail.put(scope + "|" + group, scope);
        }
        byScopeGroup.forEach((key, digests) -> {
            if (digests.size() > 1) {
                String scope = detail.get(key);
                String code = switch (scope) {
                    case "data" -> "P9_DATA_DIVERGENCE";
                    case "raftlog" -> "P9_RAFT_DIVERGENCE";
                    case "metastate" -> "P9_META_STATE_DIVERGENCE";
                    default -> "P9_GROUP_STATE_DIVERGENCE";
                };
                violate(code, key.replace('|', ' ') + ": replicas disagree: " + digests);
            }
        });
        stats.put("replica.checks", (long) byScopeGroup.size());
    }
}
