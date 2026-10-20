package com.ember.chaos;

/** One thing the checker found wrong. code is stable (P1_LOST_ACK ...); message says exactly what and where. */
public record Violation(String code, String message) {
    @Override
    public String toString() {
        return code + ": " + message;
    }
}
