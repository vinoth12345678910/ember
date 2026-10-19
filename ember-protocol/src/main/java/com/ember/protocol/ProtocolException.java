package com.ember.protocol;

/**
 * Bytes that do not follow the protocol. Carries the error code the broker should answer with
 * and the correlation id if the header was readable (null otherwise: then the broker can only
 * close the connection, because it has nobody to address a reply to).
 */
public class ProtocolException extends RuntimeException {

    private final ErrorCode errorCode;
    private final Integer correlationId;

    public ProtocolException(String message) {
        this(message, ErrorCode.INVALID_REQUEST, null);
    }

    public ProtocolException(String message, ErrorCode errorCode, Integer correlationId) {
        super(message);
        this.errorCode = errorCode;
        this.correlationId = correlationId;
    }

    public ErrorCode errorCode() {
        return errorCode;
    }

    public Integer correlationId() {
        return correlationId;
    }

    ProtocolException withCorrelationId(int id) {
        return new ProtocolException(getMessage(), errorCode, id);
    }
}
