package com.ember.raft;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SmokeTest {
    @Test
    void moduleBuilds() {
        assertEquals(2, RaftConfig.of(1, java.util.List.of(1, 2, 3)).majority());
    }
}
