package com.ember.protocol;

public record RequestHeader(ApiKey apiKey, short apiVersion, int correlationId) {

    /** apiKey(2) + apiVersion(2) + correlationId(4) */
    public static final int SIZE = 8;
}
