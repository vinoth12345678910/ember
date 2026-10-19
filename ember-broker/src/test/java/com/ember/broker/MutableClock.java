package com.ember.broker;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/** A clock tests can move by hand, so nothing ever has to sleep. */
public class MutableClock extends Clock {

    private volatile long millis;

    public MutableClock(long startMillis) {
        this.millis = startMillis;
    }

    public void advance(long ms) {
        millis += ms;
    }

    public void set(long ms) {
        millis = ms;
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }

    @Override
    public long millis() {
        return millis;
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(millis);
    }
}
