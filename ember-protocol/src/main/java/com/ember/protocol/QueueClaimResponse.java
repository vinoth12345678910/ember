package com.ember.protocol;

import java.util.List;

public record QueueClaimResponse(List<QueueMessage> messages) implements Response {
    public QueueClaimResponse {
        messages = List.copyOf(messages);
    }
}
