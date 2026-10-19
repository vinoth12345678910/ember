package com.ember.raft.sim;

import java.util.List;

/** A safety property broke. Carries everything needed to replay and understand it. */
public final class InvariantViolation extends RuntimeException {

    private final String invariant;
    private final long seed;
    private final List<String> traceTail;

    public InvariantViolation(String invariant, String detail, long seed, List<String> traceTail) {
        super(format(invariant, detail, seed, traceTail));
        this.invariant = invariant;
        this.seed = seed;
        this.traceTail = List.copyOf(traceTail);
    }

    private static String format(String invariant, String detail, long seed, List<String> tail) {
        StringBuilder sb = new StringBuilder();
        sb.append("INVARIANT VIOLATED: ").append(invariant).append(" (seed=").append(seed).append(")\n");
        sb.append(detail).append('\n');
        sb.append("--- last ").append(tail.size()).append(" trace lines ---\n");
        for (String line : tail) {
            sb.append(line).append('\n');
        }
        return sb.toString();
    }

    public String invariant() {
        return invariant;
    }

    public long seed() {
        return seed;
    }

    public List<String> traceTail() {
        return traceTail;
    }
}
