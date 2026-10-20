package com.ember.chaos;

import java.util.Locale;

/** The kinds of fault the nemesis can inject (CLIENT_PAUSE acts on a group consumer, the others on brokers). */
public enum Fault {
    CRASH_RESTART, PAUSE, ISOLATE, LINK, SLOW_FSYNC, DROP, DELAY, CLIENT_PAUSE;

    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** "crash,pause,isolate", "all" or "none" (no faults: a calm run, for measuring capacity). */
    public static java.util.Set<Fault> parse(String list) {
        java.util.Set<Fault> out = new java.util.LinkedHashSet<>();
        for (String name : list.split(",")) {
            String n = name.trim().toLowerCase(Locale.ROOT);
            switch (n) {
                case "all" -> out.addAll(java.util.List.of(values()));
                case "crash", "crash_restart" -> out.add(CRASH_RESTART);
                case "pause" -> out.add(PAUSE);
                case "isolate" -> out.add(ISOLATE);
                case "link" -> out.add(LINK);
                case "slow_fsync", "fsync" -> out.add(SLOW_FSYNC);
                case "drop" -> out.add(DROP);
                case "delay" -> out.add(DELAY);
                case "client_pause", "client" -> out.add(CLIENT_PAUSE);
                case "", "none" -> {
                }
                default -> throw new IllegalArgumentException("unknown fault '" + name + "' (crash, pause, isolate, link, slow_fsync, drop, delay, client_pause, all)");
            }
        }
        return out;
    }
}
