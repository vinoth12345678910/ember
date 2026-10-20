package com.ember.chaos;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.function.ToDoubleFunction;

/**
 * Is anything leaking? For each metric of each broker: the least-squares slope over the samples after a warm-up, projected to an
 * hour. HEAP uses a LOW QUANTILE, not the median: a leak raises the FLOOR of the live set, while single readings of the heap pools'
 * usage after their last collection jump up and down (G1 updates each pool's reading only when that pool is collected), so a median
 * of few samples can be fooled by a transient. WARM-UP IS PER PROCESS LIFETIME: a broker that the nemesis crashed and restarted starts again with few threads and a small
 * heap and grows to its normal size, which is not a leak. So samples from the first `warmupSeconds` of a broker's uptime are
 * ignored (the uptime comes from the broker itself). The VERDICT compares the median of the first third of the samples (after the warm-up) with the median of the last third,
 * which ignores wiggles and one-off steps but catches a steady climb:
 *   threads and open file descriptors: leak if last > first * 1.10 + 5;
 *   heap after GC (the live set; heap "used" is a sawtooth): compares the 10th percentile of the first and last third (the floor):
 *     leak if last > first * 1.25 + 1 MB.
 * A broker that was down most of the run has few usable samples; then the table says "too few samples to judge" for it.
 */
public final class SoakAnalysis {

    public record Trend(String metric, int node, int samples, double first, double last, double median, double min, double max, double slopePerHour, boolean flat, String note, boolean gate) {
    }

    private SoakAnalysis() {
    }

    /** warmupSeconds = how long after each (re)start of a broker its samples are ignored. */
    public static List<Trend> analyze(List<MetricsSampler.Row> rows, double warmupSeconds) {
        List<Trend> out = new ArrayList<>();
        int nodes = rows.stream().mapToInt(MetricsSampler.Row::node).max().orElse(0);
        for (int node = 1; node <= nodes; node++) {
            final int n = node;
            List<MetricsSampler.Row> mine = rows.stream().filter(r -> r.node() == n && !(r.uptimeSeconds() < warmupSeconds)).sorted(Comparator.comparingDouble(MetricsSampler.Row::elapsedSeconds)).toList();
            out.add(trend("threads", node, mine, MetricsSampler.Row::threads, 0.10, 5, 0.5, true));
            out.add(trend("open_fds", node, mine, MetricsSampler.Row::openFds, 0.10, 5, 0.5, true));
            out.add(trend("heap_after_gc", node, mine, MetricsSampler.Row::heapAfterGc, 0.25, 1_000_000, 0.10, false));
        }
        return out;
    }

    static Trend trend(String metric, int node, List<MetricsSampler.Row> rows, ToDoubleFunction<MetricsSampler.Row> f, double relative, double absolute, double quantile, boolean gate) {
        List<double[]> points = new ArrayList<>();
        for (MetricsSampler.Row r : rows) {
            double v = f.applyAsDouble(r);
            if (!Double.isNaN(v)) points.add(new double[]{r.elapsedSeconds(), v});
        }
        if (points.size() < 6) {
            return new Trend(metric, node, points.size(), Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, true, "too few samples to judge", gate);
        }
        double[] values = points.stream().mapToDouble(p -> p[1]).toArray();
        double median = median(values);
        double min = java.util.Arrays.stream(values).min().orElse(0);
        double max = java.util.Arrays.stream(values).max().orElse(0);
        double slope = slope(points) * 3600;
        int third = values.length / 3;
        double early = quantile(java.util.Arrays.copyOfRange(values, 0, third), quantile);
        double late = quantile(java.util.Arrays.copyOfRange(values, values.length - third, values.length), quantile);
        boolean flat = late <= early * (1 + relative) + absolute;
        String note = String.format(Locale.ROOT, "%s of the first third %.0f, of the last third %.0f (leak if more than +%.0f%% +%.0f)", quantile == 0.5 ? "median" : "p" + (int) (quantile * 100), early, late, relative * 100, absolute);
        return new Trend(metric, node, points.size(), values[0], values[values.length - 1], median, min, max, slope, flat, note, gate);
    }

    static double slope(List<double[]> points) {
        double n = points.size(), sx = 0, sy = 0, sxx = 0, sxy = 0;
        for (double[] p : points) {
            sx += p[0];
            sy += p[1];
            sxx += p[0] * p[0];
            sxy += p[0] * p[1];
        }
        double denominator = n * sxx - sx * sx;
        return denominator == 0 ? 0 : (n * sxy - sx * sy) / denominator;
    }

    static double quantile(double[] values, double q) {
        if (q == 0.5) {
            return median(values);
        }
        double[] sorted = values.clone();
        java.util.Arrays.sort(sorted);
        return sorted[(int) Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
    }

    /** Reads a samples CSV written by MetricsSampler (so an old run can be analyzed again). */
    public static List<MetricsSampler.Row> readCsv(java.nio.file.Path file) throws java.io.IOException {
        List<String> lines = java.nio.file.Files.readAllLines(file);
        if (lines.isEmpty() || !lines.get(0).equals(MetricsSampler.HEADER)) {
            throw new IllegalArgumentException(file + ": not a samples CSV with the current header (" + MetricsSampler.HEADER + ")");
        }
        List<MetricsSampler.Row> rows = new ArrayList<>();
        for (String line : lines.subList(1, lines.size())) {
            String[] f = line.split(",");
            rows.add(new MetricsSampler.Row(Double.parseDouble(f[0]), Integer.parseInt(f[1]), Double.parseDouble(f[2]), Double.parseDouble(f[3]), Double.parseDouble(f[4]),
                    Double.parseDouble(f[5]), Double.parseDouble(f[6]), Double.parseDouble(f[7]), Double.parseDouble(f[8]), Double.parseDouble(f[9]), Double.parseDouble(f[10])));
        }
        return rows;
    }

    static double median(double[] values) {
        double[] sorted = values.clone();
        java.util.Arrays.sort(sorted);
        return sorted.length % 2 == 1 ? sorted[sorted.length / 2] : (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2;
    }

    /**
     * The verdict covers only the metrics that are RELIABLE gates (threads, file descriptors). heap_after_gc is shown but does not
     * decide: the gauge sums each heap pool's usage after ITS OWN last collection, and a pool is updated only when it is collected,
     * so over minutes and a few dozen samples its verdict flips between nodes and between rules (seen in three soak runs). A real
     * heap verdict needs a forced full GC before and after (jcmd GC.run); see docs/phase7/step8-soak.md.
     */
    public static boolean allFlat(List<Trend> trends) {
        return trends.stream().filter(Trend::gate).allMatch(Trend::flat);
    }

    public static String table(List<Trend> trends) {
        StringBuilder sb = new StringBuilder(String.format(Locale.ROOT, "%-14s %-5s %-8s %-14s %-14s %-16s %-6s %s%n", "METRIC", "NODE", "SAMPLES", "FIRST", "LAST", "SLOPE/HOUR", "FLAT", "NOTE (heap_after_gc is informational, not a gate)"));
        for (Trend t : trends) {
            sb.append(String.format(Locale.ROOT, "%-14s %-5d %-8d %-14.0f %-14.0f %-16.1f %-6s %s%n", t.metric(), t.node(), t.samples(), t.first(), t.last(), t.slopePerHour(),
                    t.flat() ? "yes" : "NO", t.note()));
        }
        return sb.toString();
    }
}
