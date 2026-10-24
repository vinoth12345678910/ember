package com.ember.client;

import java.io.IOException;

/** The producer's queue is full: the caller is sending faster than the cluster accepts. Back off and retry, or shed load. */
public class ProducerOverloadedException extends IOException {

    public ProducerOverloadedException(String message) {
        super(message);
    }
}
