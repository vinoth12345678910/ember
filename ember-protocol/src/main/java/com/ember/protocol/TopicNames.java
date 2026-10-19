package com.ember.protocol;

import java.util.regex.Pattern;

/**
 * The topic-name rule. It is strict on purpose: a topic becomes a directory name on the broker,
 * so no '/', no '..', nothing that could point outside the data directory.
 */
public final class TopicNames {

    private static final Pattern VALID = Pattern.compile("[a-zA-Z0-9._-]{1,128}");

    private TopicNames() {
    }

    public static boolean isValid(String topic) {
        return topic != null && VALID.matcher(topic).matches() && !topic.equals(".") && !topic.equals("..");
    }
}
