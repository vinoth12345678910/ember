package com.ember.metrics;

import java.util.Locale;
import java.util.Map;

/** Renders a registry in the Prometheus text exposition format (version 0.0.4). */
public final class PrometheusFormat {

    private PrometheusFormat() {
    }

    public static String render(MetricsRegistry registry) {
        StringBuilder out = new StringBuilder(4096);
        for (MetricsRegistry.Family family : registry.families()) {
            Map<Labels, Object> children = family.children();
            if (children.isEmpty()) {
                continue;
            }
            out.append("# HELP ").append(family.name()).append(' ').append(escapeHelp(family.help())).append('\n');
            out.append("# TYPE ").append(family.name()).append(' ').append(family.type().name().toLowerCase(Locale.ROOT)).append('\n');
            for (Map.Entry<Labels, Object> child : children.entrySet()) {
                switch (family.type()) {
                    case COUNTER -> sample(out, family.name(), child.getKey(), null, ((Counter) child.getValue()).value());
                    case GAUGE -> {
                        double v = ((Gauge) child.getValue()).value();
                        if (!Double.isNaN(v)) {
                            sample(out, family.name(), child.getKey(), null, v);
                        }
                    }
                    case HISTOGRAM -> histogram(out, family.name(), child.getKey(), ((Histogram) child.getValue()).snapshot());
                }
            }
        }
        return out.toString();
    }

    private static void histogram(StringBuilder out, String name, Labels labels, Histogram.Snapshot s) {
        for (int i = 0; i < s.bounds().length; i++) {
            sample(out, name + "_bucket", labels, "le=" + scaled(s.bounds()[i], s.scale()), s.cumulative()[i]);
        }
        sample(out, name + "_bucket", labels, "le=+Inf", s.total());
        sample(out, name + "_sum", labels, null, s.sum() * s.scale());
        sample(out, name + "_count", labels, null, s.count());
    }

    /** extra is "key=value" (the value is escaped here), or null */
    private static void sample(StringBuilder out, String name, Labels labels, String extra, double value) {
        out.append(name);
        if (labels.size() > 0 || extra != null) {
            out.append('{');
            boolean first = true;
            for (int i = 0; i < labels.size(); i++) {
                if (!first) out.append(',');
                first = false;
                out.append(labels.key(i)).append("=\"").append(escapeLabel(labels.value(i))).append('"');
            }
            if (extra != null) {
                if (!first) out.append(',');
                int eq = extra.indexOf('=');
                out.append(extra, 0, eq).append("=\"").append(escapeLabel(extra.substring(eq + 1))).append('"');
            }
            out.append('}');
        }
        out.append(' ').append(formatNumber(value)).append('\n');
    }

    /** 50 * 1e-6 must print as 0.00005, not 5.0E-5 or 5.000000000000001E-5: bucket labels have to be stable text. */
    static String scaled(long value, double scale) {
        return java.math.BigDecimal.valueOf(value).multiply(java.math.BigDecimal.valueOf(scale)).stripTrailingZeros().toPlainString();
    }

    static String formatNumber(double v) {
        if (Double.isInfinite(v)) {
            return v > 0 ? "+Inf" : "-Inf";
        }
        if (v == Math.rint(v) && Math.abs(v) < 1e15) {
            return Long.toString((long) v);
        }
        if (Double.isNaN(v)) {
            return "NaN";
        }
        return new java.math.BigDecimal(Double.toString(v)).toPlainString();          // never 1.0E-6
    }

    static String escapeLabel(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 4);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '"' -> sb.append("\\\"");
                case '\n' -> sb.append("\\n");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String escapeHelp(String s) {
        return s.replace("\\", "\\\\").replace("\n", "\\n");
    }
}
