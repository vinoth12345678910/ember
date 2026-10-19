package com.ember.protocol;

/** A record as the client sends it: no offset, no timestamp. The broker assigns both. */
public record ProducerRecord(byte[] key, byte[] value) {
}
