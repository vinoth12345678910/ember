package com.ember.protocol;

public record FindCoordinatorResponse(int nodeId, String host, int port) implements Response {
}
