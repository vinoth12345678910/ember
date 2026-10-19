package com.ember.raft;

public record ProposeResult(Status status, long index, long term, int leaderHint) {

    public enum Status {
        ACCEPTED, NOT_LEADER
    }

    public boolean accepted() {
        return status == Status.ACCEPTED;
    }
}
