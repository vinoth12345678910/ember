package com.ember.client;

import com.ember.protocol.ErrorCode;

/** The broker answered with a non-zero error code. */
public class EmberException extends RuntimeException {

    private final ErrorCode errorCode;

    public EmberException(ErrorCode errorCode) {
        super("broker returned " + errorCode);
        this.errorCode = errorCode;
    }

    public ErrorCode errorCode() {
        return errorCode;
    }
}
