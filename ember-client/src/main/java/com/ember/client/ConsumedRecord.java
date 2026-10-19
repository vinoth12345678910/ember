package com.ember.client;

import com.ember.storage.LogRecord;

public record ConsumedRecord(String topic, int partition, LogRecord record) {
}
