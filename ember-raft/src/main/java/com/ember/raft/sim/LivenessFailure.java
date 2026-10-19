package com.ember.raft.sim;

import java.util.List;

/** The cluster was healthy and calm but did not do what a working Raft cluster must do. */
public final class LivenessFailure extends RuntimeException {

    private final long seed;
    private final List<String> traceTail;

    public LivenessFailure(String detail, long seed, List<String> traceTail) {
        super("LIVENESS FAILURE (seed=" + seed + ")\n" + detail + "\n--- last " + traceTail.size() + " trace lines ---\n"
                + String.join("\n", traceTail) + "\n");
        this.seed = seed;
        this.traceTail = List.copyOf(traceTail);
    }

    public long seed() {
        return seed;
    }

    public List<String> traceTail() {
        return traceTail;
    }
}
