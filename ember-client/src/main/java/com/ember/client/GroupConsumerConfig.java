package com.ember.client;

import java.util.List;

/**
 * sessionTimeoutMs: how long the coordinator waits for a heartbeat before it removes this member.
 * heartbeatIntervalMs: how often the background thread beats; must be at most sessionTimeoutMs / 3 so that two
 * beats can be lost before the session expires.
 * autoCommit: poll() commits the positions of the PREVIOUS poll every autoCommitIntervalMs (process, then commit).
 * autoOffsetReset: what to do when there is no committed offset (or it fell below the log start): earliest, latest, none.
 * maxPollIntervalMs: a consumer whose user thread does not call poll() for this long leaves the group.
 */
public record GroupConsumerConfig(ClientConfig client, String groupId, String clientId, int sessionTimeoutMs, int heartbeatIntervalMs,
                                  boolean autoCommit, int autoCommitIntervalMs, OffsetReset autoOffsetReset, int maxPollBytes,
                                  int maxPollIntervalMs) {

    public enum OffsetReset {
        EARLIEST, LATEST, NONE
    }

    public GroupConsumerConfig {
        if (groupId == null || groupId.isEmpty()) {
            throw new IllegalArgumentException("groupId is required");
        }
        if (heartbeatIntervalMs * 3 > sessionTimeoutMs) {
            throw new IllegalArgumentException("heartbeatIntervalMs must be <= sessionTimeoutMs / 3");
        }
        if (client.bootstrap().isEmpty()) {
            throw new IllegalArgumentException("a bootstrap list is needed");
        }
    }

    public static GroupConsumerConfig of(List<String> bootstrap, String groupId) {
        return new GroupConsumerConfig(ClientConfig.defaults().withBootstrap(bootstrap), groupId, "ember-consumer", 10_000, 3_000,
                true, 5_000, OffsetReset.EARLIEST, 1024 * 1024, 300_000);
    }

    public GroupConsumerConfig withClientId(String v) {
        return new GroupConsumerConfig(client, groupId, v, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs, autoOffsetReset, maxPollBytes, maxPollIntervalMs);
    }

    public GroupConsumerConfig withSession(int sessionMs, int heartbeatMs) {
        return new GroupConsumerConfig(client, groupId, clientId, sessionMs, heartbeatMs, autoCommit, autoCommitIntervalMs, autoOffsetReset, maxPollBytes, maxPollIntervalMs);
    }

    public GroupConsumerConfig withAutoCommit(boolean enabled, int intervalMs) {
        return new GroupConsumerConfig(client, groupId, clientId, sessionTimeoutMs, heartbeatIntervalMs, enabled, intervalMs, autoOffsetReset, maxPollBytes, maxPollIntervalMs);
    }

    public GroupConsumerConfig withOffsetReset(OffsetReset v) {
        return new GroupConsumerConfig(client, groupId, clientId, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs, v, maxPollBytes, maxPollIntervalMs);
    }

    public GroupConsumerConfig withMaxPollBytes(int v) {
        return new GroupConsumerConfig(client, groupId, clientId, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs, autoOffsetReset, v, maxPollIntervalMs);
    }

    public GroupConsumerConfig withMaxPollInterval(int v) {
        return new GroupConsumerConfig(client, groupId, clientId, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs, autoOffsetReset, maxPollBytes, v);
    }

    public GroupConsumerConfig withClient(ClientConfig v) {
        return new GroupConsumerConfig(v, groupId, clientId, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs, autoOffsetReset, maxPollBytes, maxPollIntervalMs);
    }
}
