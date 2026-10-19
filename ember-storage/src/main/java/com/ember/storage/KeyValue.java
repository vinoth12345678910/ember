package com.ember.storage;

/** One record to append, before the broker has given it an offset and timestamp. */
public record KeyValue(byte[] key, byte[] value) {
}
