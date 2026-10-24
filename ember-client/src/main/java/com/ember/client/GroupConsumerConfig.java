package com.ember.client;

import java.util.List;

/**
 * sessionTimeoutMs: how long the coordinator waits for a heartbeat before it removes this member.
 * heartbeatIntervalMs: how often the background thread beats; must be at most sessionTimeoutMs / 3 so that two
 * beats can be lost before the session expires.
 * autoCommit: poll() commits the positions of the PREVIOUS poll every autoCommitIntervalMs (process, then commit).
 * autoOffsetReset: what to do when there is no committed offset (or it fell below the log start): earliest, latest, none.
 * maxPollIntervalMs: a consumer whose user thread does not call poll() for this long leaves the group.
 * incrementalRejoin: after a rebalance keep consuming (and keep the position of) every partition that stays assigned, commit the
 * positions of the ones taken away, and read the committed offset only for the new ones. Meant for brokers started with
 * --group-rebalance=cooperative; harmless (just repeats a little work) against an eager one. false = drop everything and
 * start from the committed offsets, which is what Kafka's eager protocol does.
 */
public record GroupConsumerConfig(ClientConfig client, String groupId, String clientId, int sessionTimeoutMs, int heartbeatIntervalMs,
                                  boolean autoCommit, int autoCommitIntervalMs, OffsetReset autoOffsetReset, int maxPollBytes,
                                  int maxPollIntervalMs, boolean incrementalRejoin) {

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
                true, 5_000, OffsetReset.EARLIEST, 1024 * 1024, 300_000, false);
    }

    public GroupConsumerConfig withClientId(String v) {
        return new GroupConsumerConfig(client, groupId, v, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs, autoOffsetReset, maxPollBytes, maxPollIntervalMs, incrementalRejoin);
    }

    public GroupConsumerConfig withSession(int sessionMs, int heartbeatMs) {
        return new GroupConsumerConfig(client, groupId, clientId, sessionMs, heartbeatMs, autoCommit, autoCommitIntervalMs, autoOffsetReset, maxPollBytes, maxPollIntervalMs, incrementalRejoin);
    }

    public GroupConsumerConfig withAutoCommit(boolean enabled, int intervalMs) {
        return new GroupConsumerConfig(client, groupId, clientId, sessionTimeoutMs, heartbeatIntervalMs, enabled, intervalMs, autoOffsetReset, maxPollBytes, maxPollIntervalMs, incrementalRejoin);
    }

    public GroupConsumerConfig withOffsetReset(OffsetReset v) {
        return new GroupConsumerConfig(client, groupId, clientId, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs, v, maxPollBytes, maxPollIntervalMs, incrementalRejoin);
    }

    public GroupConsumerConfig withMaxPollBytes(int v) {
        return new GroupConsumerConfig(client, groupId, clientId, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs, autoOffsetReset, v, maxPollIntervalMs, incrementalRejoin);
    }

    public GroupConsumerConfig withMaxPollInterval(int v) {
        return new GroupConsumerConfig(client, groupId, clientId, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs, autoOffsetReset, maxPollBytes, v, incrementalRejoin);
    }

    public GroupConsumerConfig withClient(ClientConfig v) {
        return new GroupConsumerConfig(v, groupId, clientId, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs, autoOffsetReset, maxPollBytes, maxPollIntervalMs, incrementalRejoin);
    }

    public GroupConsumerConfig withIncrementalRejoin(boolean v) {
        return new GroupConsumerConfig(client, groupId, clientId, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs, autoOffsetReset, maxPollBytes, maxPollIntervalMs, v);
    }
}
