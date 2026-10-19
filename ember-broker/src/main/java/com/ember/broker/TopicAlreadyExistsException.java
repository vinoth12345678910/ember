package com.ember.broker;

public class TopicAlreadyExistsException extends RuntimeException {
    public TopicAlreadyExistsException(String topic) {
        super("topic already exists: " + topic);
    }
}
