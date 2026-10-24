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
 *  P9 replicas agree (data logs, Raft logs, group, ack-queue and topic state)
 *
 * Ack queues (only checked when the history has queue operations):
 *  P10 an acknowledged message is never delivered again      P11 no delivery is handed out twice (attempt numbers are unique)
 *  P12 a stale delivery cannot settle a message another worker holds (fencing)
 *  P13 nothing is stuck or lost: after the drain every message is finished or dead-lettered, and after the operator's fix nothing is dead
 *  P14 the dead-letter list agrees with the progress counters and the log, and each entry used up its attempts
 *  P15 every delivered message is the record that is really in the log
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
        c.queueProperties();
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
                    case "ackstate" -> "P9_ACK_STATE_DIVERGENCE";
                    default -> "P9_GROUP_STATE_DIVERGENCE";
                };
                violate(code, key.replace('|', ' ') + ": replicas disagree: " + digests);
            }
        });
        stats.put("replica.checks", (long) byScopeGroup.size());
    }

    // ------------------------------------------------------------------ ack queues (P10-P15)

    private record Delivery(String msg, long attempt, String value, long invokeTime, long completeTime, String worker) {
    }

    private record Settle(String msg, String action, long attempt, long settled, long rejected, long invokeTime, long completeTime, boolean ok) {
    }

    private static String msgKey(Object partition, Object offset) {
        return ((Number) partition).longValue() + ":" + ((Number) offset).longValue();
    }

    private void queueProperties() {
        List<History.Op> claimOps = history.ops("claim");
        List<History.Op> settleOps = history.ops("settle");
        if (claimOps.isEmpty() && settleOps.isEmpty()) return;

        List<Delivery> deliveries = new ArrayList<>();
        long claims = 0;
        for (History.Op op : claimOps) {
            if (op.complete() == null || op.outcome() != Outcome.OK) continue;
            claims++;
            for (List<Object> row : op.complete().rows("messages")) {
                deliveries.add(new Delivery(msgKey(row.get(0), row.get(1)), ((Number) row.get(2)).longValue(), (String) row.get(3), op.invoke().timeNanos(),
                        op.complete().timeNanos(), op.invoke().process()));
            }
        }
        List<Settle> settles = new ArrayList<>();
        long nacks = 0, releases = 0, rejected = 0, acksAccepted = 0;
        for (History.Op op : settleOps) {
            HistoryEvent in = op.invoke();
            if (op.complete() == null) continue;
            boolean ok = op.outcome() == Outcome.OK;
            String action = in.str("action");
            if (ok) rejected += Math.max(0, op.complete().num("rejected"));
            List<List<Object>> handles = in.rows("handles");
            if (handles.size() != 1 || action.equals("dead-retry")) continue;                // exact per-handle reasoning needs one handle per operation
            List<Object> h = handles.get(0);
            Settle s = new Settle(msgKey(h.get(0), h.get(1)), action, ((Number) h.get(2)).longValue(), ok ? op.complete().num("settled") : 0, ok ? op.complete().num("rejected") : 0,
                    in.timeNanos(), op.complete().timeNanos(), ok);
            settles.add(s);
            if (ok && s.settled() > 0) {
                switch (action) {
                    case "ack" -> acksAccepted++;
                    case "nack" -> nacks++;
                    case "release" -> releases++;
                    default -> { }
                }
            }
        }
        stats.put("queue.claims", claims);
        stats.put("queue.deliveries", (long) deliveries.size());
        stats.put("queue.redeliveries", deliveries.stream().filter(d -> d.attempt() > 1).count());
        stats.put("queue.acks.accepted", acksAccepted);
        stats.put("queue.nacks", nacks);
        stats.put("queue.releases", releases);
        stats.put("queue.rejected", rejected);

        // P11: a delivery (message + attempt number) is handed out once
        Map<String, Delivery> seenAttempt = new HashMap<>();
        for (Delivery d : deliveries) {
            Delivery before = seenAttempt.putIfAbsent(d.msg() + "#" + d.attempt(), d);
            if (before != null) {
                violate("P11_ATTEMPT_REUSED", "message " + d.msg() + " attempt " + d.attempt() + " was delivered to both " + before.worker() + " and " + d.worker());
            }
            if (d.attempt() < 1) violate("P11_ATTEMPT_REUSED", "message " + d.msg() + " was delivered with attempt " + d.attempt());
        }

        // P10: nothing is delivered after an acknowledged ack of it completed
        Map<String, Long> firstAckDone = new HashMap<>();
        for (Settle s : settles) {
            if (s.ok() && s.settled() > 0 && s.action().equals("ack")) firstAckDone.merge(s.msg(), s.completeTime(), Math::min);
        }
        for (Delivery d : deliveries) {
            Long done = firstAckDone.get(d.msg());
            if (done != null && d.invokeTime() > done) {
                violate("P10_REDELIVERED_AFTER_ACK", "message " + d.msg() + " (" + d.value() + ") was acknowledged and then delivered again to " + d.worker() + " as attempt " + d.attempt());
            }
        }

        // P12: a settle for attempt a is refused once attempt a' > a has been delivered, unless the message was already finished
        Map<String, List<Delivery>> byMsg = new HashMap<>();
        for (Delivery d : deliveries) byMsg.computeIfAbsent(d.msg(), k -> new ArrayList<>()).add(d);
        for (Settle s : settles) {
            if (!s.ok() || s.settled() < 1) continue;
            boolean finishedBefore = s.action().equals("ack") && firstAckDone.getOrDefault(s.msg(), Long.MAX_VALUE) < s.invokeTime();
            if (finishedBefore) continue;                                                   // acking something already finished is an accepted no-op
            for (Delivery d : byMsg.getOrDefault(s.msg(), List.of())) {
                if (d.attempt() > s.attempt() && d.completeTime() < s.invokeTime()) {
                    violate("P12_FENCED_SETTLE_ACCEPTED", "message " + s.msg() + ": " + s.action() + " of attempt " + s.attempt() + " was accepted although attempt " + d.attempt()
                            + " had already been delivered to " + d.worker());
                    break;
                }
            }
        }

        // P15: what was delivered is what the log holds
        String topic = produces.isEmpty() ? null : produces.get(0).topic();
        Set<String> possible = new HashSet<>();
        for (Produce p : produces) if (p.outcome() != Outcome.FAIL) possible.add(p.value());
        for (Delivery d : deliveries) {
            if (!possible.contains(d.value())) {
                violate("P15_PHANTOM_DELIVERY", "value " + d.value() + " was delivered by the queue (" + d.msg() + ") but no produce ever sent it");
                continue;
            }
            if (topic == null) continue;
            String[] po = d.msg().split(":");
            TreeMap<Long, Rec> log = finalLog.get(topic + "-" + po[0]);
            Rec at = log == null ? null : log.get(Long.parseLong(po[1]));
            if (at != null && !Objects.equals(at.value(), d.value())) {
                violate("P15_PHANTOM_DELIVERY", "the queue delivered " + d.value() + " as " + d.msg() + " but the log holds " + at.value() + " there");
            }
        }

        // P13 / P14: the final states
        List<History.Op> finals = history.ops("queue-final");
        if (finals.isEmpty()) {
            if (claims > 0) violate("P13_NO_FINAL_STATE", "the queue was used but its final state was never recorded: stuck messages cannot be ruled out");
            return;
        }
        long deadAtFirstFinal = 0;
        for (History.Op op : finals) {
            if (op.complete() == null || op.outcome() != Outcome.OK) {
                violate("P13_NO_FINAL_STATE", "the final state of the queue could not be read");
                continue;
            }
            HistoryEvent in = op.invoke(), done = op.complete();
            boolean afterFix = in.flag("afterFix");
            long deadSum = 0;
            for (List<Object> row : done.rows("partitions")) {
                long partition = ((Number) row.get(0)).longValue(), floor = ((Number) row.get(1)).longValue(), next = ((Number) row.get(2)).longValue(),
                        logEnd = ((Number) row.get(3)).longValue(), inFlight = ((Number) row.get(4)).longValue(), redeliver = ((Number) row.get(5)).longValue(),
                        doneAbove = ((Number) row.get(6)).longValue(), dead = ((Number) row.get(7)).longValue();
                String where = "partition " + partition + (afterFix ? " after the fix" : " after the drain");
                deadSum += dead;
                if (next != logEnd) violate("P13_UNCLAIMED", where + ": the log ends at " + logEnd + " but only " + next + " offsets were ever handed out");
                if (inFlight > 0 || redeliver > 0) violate("P13_STUCK", where + ": " + inFlight + " still claimed and " + redeliver + " still waiting to be redelivered");
                if (inFlight == 0 && redeliver == 0 && next - floor != doneAbove + dead) {
                    violate("P13_UNACCOUNTED", where + ": " + (next - floor) + " offsets above the floor, but only " + doneAbove + " finished and " + dead + " dead-lettered");
                }
                if (afterFix && dead > 0) violate("P13_DEAD_AFTER_FIX", where + ": " + dead + " messages are still dead-lettered after the fix and the operator's retry");
            }
            if (!afterFix) {
                deadAtFirstFinal = done.num("deadTotal");
                stats.put("queue.dead", deadAtFirstFinal);
            }
            if (done.num("deadTotal") != deadSum) {
                violate("P14_DEAD_COUNT", "the dead-letter list says " + done.num("deadTotal") + " but the partitions count " + deadSum);
            }
            if (done.rows("dead").size() != Math.min(done.num("deadTotal"), 1000)) {
                violate("P14_DEAD_COUNT", "the dead-letter list returned " + done.rows("dead").size() + " entries for a total of " + done.num("deadTotal"));
            }
            long maxAttempts = done.num("maxAttempts");
            for (List<Object> row : done.rows("dead")) {
                String tpKey = topic == null ? null : topic + "-" + ((Number) row.get(0)).longValue();
                long failures = ((Number) row.get(2)).longValue();
                String value = (String) row.get(3);
                if (failures < maxAttempts) violate("P14_DEAD_EARLY", "message " + row.get(0) + ":" + row.get(1) + " (" + value + ") is dead-lettered after only " + failures + " failures (limit " + maxAttempts + ")");
                TreeMap<Long, Rec> log = tpKey == null ? null : finalLog.get(tpKey);
                Rec at = log == null ? null : log.get(((Number) row.get(1)).longValue());
                if (at != null && !Objects.equals(at.value(), value)) {
                    violate("P14_DEAD_CONTENT", "the dead letter " + row.get(0) + ":" + row.get(1) + " shows " + value + " but the log holds " + at.value());
                }
            }
        }
    }
}
