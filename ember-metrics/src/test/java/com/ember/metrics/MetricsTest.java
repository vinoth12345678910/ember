package com.ember.metrics;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MetricsTest {

    // ------------------------------------------------------------------ counter

    @Test
    void aCounterOnlyGoesUp() {
        Counter c = new Counter();
        c.inc();
        c.add(41);
        assertEquals(42, c.value());
        assertThrows(IllegalArgumentException.class, () -> c.add(-1));
    }

    @Test
    void sixteenThreadsAddUpExactly() throws Exception {
        Counter counter = new Counter();
        Histogram histogram = new Histogram(new long[]{10, 100}, 1);
        int threads = 16;
        int each = 100_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                go.await();
                for (int i = 0; i < each; i++) {
                    counter.inc();
                    histogram.record(i % 200);
                }
                return null;
            }));
        }
        go.countDown();
        for (Future<?> f : futures) f.get(60, TimeUnit.SECONDS);
        pool.shutdown();
        assertEquals((long) threads * each, counter.value());
        assertEquals((long) threads * each, histogram.snapshot().count());
        assertEquals((long) threads * each, histogram.snapshot().total());
    }

    // ------------------------------------------------------------------ gauge

    @Test
    void aGaugeIsReadWhenAskedAndAFailingSupplierIsSkipped() {
        AtomicInteger v = new AtomicInteger(1);
        Gauge g = new Gauge(v::get);
        assertEquals(1, g.value());
        v.set(7);
        assertEquals(7, g.value());
        assertTrue(Double.isNaN(new Gauge(() -> {
            throw new IllegalStateException("boom");
        }).value()));

        MetricsRegistry r = new MetricsRegistry();
        r.gauge("ok", "fine", Labels.NONE, () -> 3);
        r.gauge("broken", "bad", Labels.NONE, () -> {
            throw new IllegalStateException("boom");
        });
        PromParser.Parsed p = PromParser.parse(PrometheusFormat.render(r));
        assertEquals(3, p.value("ok"));
        assertTrue(p.named("broken").isEmpty(), "the failing gauge has no sample, but the scrape works");
    }

    // ------------------------------------------------------------------ histogram

    @Test
    void aValueEqualToABoundBelongsToThatBucket() {
        Histogram h = new Histogram(new long[]{10, 20, 30}, 1);
        h.record(10);      // <= 10
        h.record(11);      // <= 20
        h.record(20);      // <= 20
        h.record(30);      // <= 30
        h.record(31);      // +Inf
        h.record(0);       // <= 10
        Histogram.Snapshot s = h.snapshot();
        assertArrayEquals(new long[]{2, 4, 5, 6}, s.cumulative(), "cumulative: le=10, le=20, le=30, +Inf");
        assertEquals(6, s.count());
        assertEquals(102, s.sum());
    }

    @Test
    void histogramsRejectBadBounds() {
        assertThrows(IllegalArgumentException.class, () -> new Histogram(new long[0], 1));
        assertThrows(IllegalArgumentException.class, () -> new Histogram(new long[]{5, 5}, 1));
        assertThrows(IllegalArgumentException.class, () -> new Histogram(new long[]{5, 4}, 1));
    }

    @Test
    void defaultLatencyBoundsCover50MicrosTo30Seconds() {
        long[] b = Histogram.DEFAULT_LATENCY_BOUNDS_MICROS;
        assertEquals(50, b[0]);
        assertEquals(30_000_000, b[b.length - 1]);
        for (int i = 1; i < b.length; i++) assertTrue(b[i] > b[i - 1]);
    }

    // ------------------------------------------------------------------ registry

    @Test
    void registeringTwiceReturnsTheSameObject() {
        MetricsRegistry r = new MetricsRegistry();
        Counter a = r.counter("requests_total", "help", Labels.of("api", "produce"));
        Counter b = r.counter("requests_total", "help", Labels.of("api", "produce"));
        Counter c = r.counter("requests_total", "help", Labels.of("api", "fetch"));
        assertSame(a, b);
        assertNotSame(a, c);
        assertEquals(2, r.childCount());
        assertSame(r.gauge("g", "h", Labels.NONE, () -> 1), r.gauge("g", "h", Labels.NONE, () -> 2));
    }

    @Test
    void aNameCannotChangeTypeAndNamesAreValidated() {
        MetricsRegistry r = new MetricsRegistry();
        r.counter("x_total", "h", Labels.NONE);
        assertThrows(IllegalArgumentException.class, () -> r.gauge("x_total", "h", Labels.NONE, () -> 1));
        assertThrows(IllegalArgumentException.class, () -> r.counter("bad name", "h", Labels.NONE));
        assertThrows(IllegalArgumentException.class, () -> Labels.of("bad-label", "v"));
        assertThrows(IllegalArgumentException.class, () -> Labels.of("k"));
        assertThrows(IllegalArgumentException.class, () -> Labels.of("a", "1", "a", "2"));
        assertEquals(Labels.of("a", "1", "b", "2"), Labels.of("b", "2", "a", "1"), "label order does not matter");
    }

    @Test
    void theTenThousandChildrenGuardStopsExportingButNeverFails() {
        MetricsRegistry r = new MetricsRegistry();
        for (int i = 0; i < MetricsRegistry.MAX_CHILDREN; i++) {
            r.counter("many_total", "h", Labels.of("i", Integer.toString(i)));
        }
        assertEquals(MetricsRegistry.MAX_CHILDREN, r.childCount());
        Counter overflow = r.counter("many_total", "h", Labels.of("i", "one-too-many"));
        overflow.inc();                                           // usable ...
        assertEquals(1, overflow.value());
        assertEquals(MetricsRegistry.MAX_CHILDREN, r.childCount(), "... but not registered");
        assertNotSame(overflow, r.counter("many_total", "h", Labels.of("i", "one-too-many")));
        assertEquals(MetricsRegistry.MAX_CHILDREN, PromParser.parse(PrometheusFormat.render(r)).named("many_total").size());
        Counter existing = r.counter("many_total", "h", Labels.of("i", "5"));
        existing.inc();
        assertEquals(1, PromParser.parse(PrometheusFormat.render(r)).value("many_total", "i", "5"), "already registered ones still work");
    }

    // ------------------------------------------------------------------ Prometheus text

    @Test
    void theTextFormatCanBeParsedBackWithHelpAndTypeOncePerFamily() {
        MetricsRegistry r = new MetricsRegistry();
        r.counter("ember_requests_total", "Requests.", Labels.of("api", "produce")).add(5);
        r.counter("ember_requests_total", "Requests.", Labels.of("api", "fetch")).add(2);
        r.gauge("ember_connections", "Open connections.", Labels.NONE, () -> 3);
        r.gauge("ember_ratio", "A fraction.", Labels.NONE, () -> 0.25);
        PromParser.Parsed p = PromParser.parse(PrometheusFormat.render(r));
        assertEquals(5, p.value("ember_requests_total", "api", "produce"));
        assertEquals(2, p.value("ember_requests_total", "api", "fetch"));
        assertEquals(3, p.value("ember_connections"));
        assertEquals(0.25, p.value("ember_ratio"));
        assertEquals("counter", p.types().get("ember_requests_total"));
        assertEquals("gauge", p.types().get("ember_connections"));
        assertEquals(1, p.helpCount().get("ember_requests_total"), "one HELP line per family, not per child");
        assertEquals(1, p.typeCount().get("ember_requests_total"));
    }

    @Test
    void labelValuesWithQuotesBackslashesAndNewlinesSurvive() {
        MetricsRegistry r = new MetricsRegistry();
        String nasty = "say \"hi\"\\path\nnext line";
        r.counter("c_total", "h", Labels.of("v", nasty)).inc();
        String text = PrometheusFormat.render(r);
        assertFalse(text.substring(text.indexOf("c_total{")).startsWith("c_total{v=\"say \"hi"), "quotes must be escaped");
        PromParser.Parsed p = PromParser.parse(text);
        assertEquals(nasty, p.named("c_total").get(0).labels().get("v"));
        assertEquals(1, p.named("c_total").size(), "a newline in a label value must not split the line");
    }

    @Test
    void histogramsAreCumulativeEndWithInfAndCountEqualsInf() {
        MetricsRegistry r = new MetricsRegistry();
        Histogram h = r.histogram("ember_size_bytes", "Sizes.", Labels.of("kind", "x"), new long[]{10, 100}, 1);
        for (long v : new long[]{1, 10, 11, 100, 101, 5000}) h.record(v);
        PromParser.Parsed p = PromParser.parse(PrometheusFormat.render(r));
        assertEquals("histogram", p.types().get("ember_size_bytes"));
        List<PromParser.Sample> buckets = p.named("ember_size_bytes_bucket");
        assertEquals(3, buckets.size());
        assertEquals("10", buckets.get(0).labels().get("le"));
        assertEquals("100", buckets.get(1).labels().get("le"));
        assertEquals("+Inf", buckets.get(2).labels().get("le"), "the last bucket is +Inf");
        double previous = 0;
        for (PromParser.Sample s : buckets) {
            assertTrue(s.value() >= previous, "cumulative");
            previous = s.value();
            assertEquals("x", s.labels().get("kind"), "extra labels are kept next to le");
        }
        assertEquals(2, buckets.get(0).value());
        assertEquals(4, buckets.get(1).value());
        assertEquals(6, buckets.get(2).value());
        assertEquals(p.value("ember_size_bytes_count"), buckets.get(2).value(), "_count equals the +Inf bucket");
        assertEquals(1 + 10 + 11 + 100 + 101 + 5000, p.value("ember_size_bytes_sum"));
    }

    @Test
    void latencyHistogramsAreExportedInSeconds() {
        MetricsRegistry r = new MetricsRegistry();
        Histogram h = r.latencyHistogram("ember_latency_seconds", "Latency.", Labels.NONE);
        h.record(50);                 // exactly the first bound: 50 microseconds
        h.record(2_000_000);          // 2 s
        PromParser.Parsed p = PromParser.parse(PrometheusFormat.render(r));
        assertEquals(1, p.named("ember_latency_seconds_bucket").stream().filter(s -> s.labels().get("le").equals("0.00005")).findFirst().orElseThrow().value());
        assertEquals(2.00005, p.value("ember_latency_seconds_sum"), 1e-9);
        assertEquals(2, p.value("ember_latency_seconds_count"));
    }

    @Test
    void bucketLabelsAreExactDecimalsWithoutFloatNoise() {
        MetricsRegistry r = new MetricsRegistry();
        r.latencyHistogram("ember_l_seconds", "L.", Labels.NONE).record(1);
        String text = PrometheusFormat.render(r);
        for (String le : new String[]{"0.00005", "0.0001", "0.0004", "0.0008", "0.0016", "0.1024", "1.6384", "30"}) {
            assertTrue(text.contains("le=\"" + le + "\""), "missing le=" + le + "\n" + text);
        }
        assertFalse(text.contains("E-"), "no scientific notation in bucket bounds");
    }

    @Test
    void anEmptyRegistryRendersNothing() {
        assertEquals("", PrometheusFormat.render(new MetricsRegistry()));
    }
}
