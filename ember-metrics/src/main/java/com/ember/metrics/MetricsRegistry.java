package com.ember.metrics;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.DoubleSupplier;
import java.util.regex.Pattern;

/**
 * Owns every metric of one process. register* is idempotent per (name, labels). The registry holds at most
 * MAX_CHILDREN metrics in total: past that, new ones are handed out working but UNREGISTERED (they are simply never
 * exported) and the problem is logged once, because a metrics bug must never take a broker down.
 */
public final class MetricsRegistry {

    public static final int MAX_CHILDREN = 10_000;

    private static final System.Logger LOG = System.getLogger(MetricsRegistry.class.getName());
    private static final Pattern METRIC_NAME = Pattern.compile("[a-zA-Z_:][a-zA-Z0-9_:]*");
    private static final Pattern LABEL_NAME = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

    public enum Type {
        COUNTER, GAUGE, HISTOGRAM
    }

    /** All children (label sets) of one metric name. */
    public static final class Family {
        private final String name;
        private final String help;
        private final Type type;
        private final Map<Labels, Object> children = new TreeMap<>();

        private Family(String name, String help, Type type) {
            this.name = name;
            this.help = help;
            this.type = type;
        }

        public String name() {
            return name;
        }

        public String help() {
            return help;
        }

        public Type type() {
            return type;
        }

        /** A copy, safe to iterate while others register. */
        public synchronized Map<Labels, Object> children() {
            return new TreeMap<>(children);
        }
    }

    private final Map<String, Family> families = new LinkedHashMap<>();
    private int childCount;
    private boolean warned;

    static boolean isValidLabelName(String name) {
        return name != null && LABEL_NAME.matcher(name).matches() && !name.startsWith("__");
    }

    public Counter counter(String name, String help, Labels labels) {
        return register(name, help, Type.COUNTER, labels, Counter::new, Counter.class);
    }

    /** A counter that mirrors a number kept elsewhere (read at scrape time). */
    public Counter counterFromSupplier(String name, String help, Labels labels, java.util.function.LongSupplier source) {
        return register(name, help, Type.COUNTER, labels, () -> Counter.fromSupplier(source), Counter.class);
    }

    public Gauge gauge(String name, String help, Labels labels, DoubleSupplier supplier) {
        return register(name, help, Type.GAUGE, labels, () -> new Gauge(supplier), Gauge.class);
    }

    /** bounds in the recorded unit, scale converts to the exported unit (1e-6: microseconds -> seconds). */
    public Histogram histogram(String name, String help, Labels labels, long[] bounds, double scale) {
        return register(name, help, Type.HISTOGRAM, labels, () -> new Histogram(bounds, scale), Histogram.class);
    }

    public Histogram latencyHistogram(String name, String help, Labels labels) {
        return histogram(name, help, labels, Histogram.DEFAULT_LATENCY_BOUNDS_MICROS, 1e-6);
    }

    private synchronized <T> T register(String name, String help, Type type, Labels labels, java.util.function.Supplier<T> factory, Class<T> cls) {
        if (!METRIC_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("invalid metric name: " + name);
        }
        Family family = families.get(name);
        if (family != null && family.type != type) {
            throw new IllegalArgumentException("metric " + name + " is already registered as " + family.type);
        }
        if (family != null) {
            Object existing = family.children.get(labels);
            if (existing != null) {
                return cls.cast(existing);                        // idempotent
            }
        }
        if (childCount >= MAX_CHILDREN) {
            if (!warned) {
                warned = true;
                LOG.log(System.Logger.Level.WARNING, "metrics: more than {0} children registered; new ones are not exported (check label cardinality, last: {1}{2})",
                        MAX_CHILDREN, name, labels);
            }
            return factory.get();                                  // works, but nobody will ever see it
        }
        if (family == null) {
            family = new Family(name, help, type);
            families.put(name, family);
        }
        T created = factory.get();
        synchronized (family) {
            family.children.put(labels, created);
        }
        childCount++;
        return created;
    }

    public synchronized int childCount() {
        return childCount;
    }

    /** A stable snapshot of the families, in registration order. */
    public synchronized List<Family> families() {
        return new ArrayList<>(families.values());
    }
}
