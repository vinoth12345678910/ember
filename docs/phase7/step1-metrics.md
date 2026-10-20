# Step 1 - The ember-metrics module

## What we are building
A tiny metrics library with no dependencies: `Counter`, `Gauge`, `Histogram`, `Labels`, `MetricsRegistry` and `PrometheusFormat` (the Prometheus text exposition format). Thread-safe, and the hot path (increment a counter, record a latency) allocates nothing.

## Why it is designed this way
Think of the dashboard of a car. You do not open the engine to know if it is healthy: the speedometer (a **gauge**, read when you look), the trip counter (a **counter**, only goes up) and, for latency, a small chart of "how many trips took 0-1 ms, 1-2 ms, 2-4 ms ..." (a **histogram**).

* **Counter** = a `LongAdder`. Many threads increment it without fighting over one memory cell; the sum is computed only when somebody reads it.
* **Gauge** = a function that is called at *scrape* time. It must be cheap and must never block (it is called from the HTTP thread). If it throws, that one sample is skipped; the scrape still works.
* **Histogram** = a sorted array of bucket upper bounds and one counter per bucket. `record(v)` does a binary search and three `LongAdder` increments. A value *equal* to a bound belongs to that bucket (`le` = "less than or equal"). Buckets are exported **cumulative** (each bucket includes all smaller ones) and the last one is `+Inf`, whose value equals `_count`. Latencies are recorded as integer microseconds and exported in seconds (a *scale* factor), because Prometheus convention is base units. Default latency bounds: 50 us to 30 s in roughly x2 steps.
* **Labels** are a small fixed set of `key=value` pairs per metric child. The caller controls cardinality (never label by record key or offset); the registry refuses more than **10,000 children** in total and logs once, because an unbounded label set is the classic way to kill a metrics system.
* `register` is **idempotent** per (name, labels): asking twice returns the same object, so code that creates a partition's metrics when a replica starts can safely run twice.

**Why our own library?** Zero dependencies (a project rule), and a student building a broker should know what a histogram really is. We rejected Micrometer: it is excellent, but it pulls in a tree of dependencies and hides exactly the parts (bucketing, cumulative export, label cardinality) that are the lesson.

## Alternatives I rejected and why
* Quantiles computed inside the process (e.g. t-digest): they cannot be added across brokers, histograms can.
* Gauges stored as pushed values: easy to forget to update; a supplier read at scrape time is always current.
* Throwing when the 10,000-children limit is hit: a metrics bug must never take down a broker. The 10,001st registration returns a working but *unregistered* object.

## What goes wrong if you get it wrong
Non-cumulative buckets make every Prometheus quantile query wrong. A gauge supplier that takes a lock held by the Raft thread would make `/metrics` hang during an election, exactly when you need it. Unescaped quotes or newlines in a label value make the whole scrape unparsable.

## Tradeoffs and honest limits
No summaries, no exemplars, no native histograms, no remove/unregister of children (partitions never go away in Ember). Gauge values are doubles. A histogram read while it is being written is not a perfectly consistent snapshot (count and buckets can differ by in-flight records); Prometheus tolerates that.

## How it is tested
Counter/gauge/histogram semantics; bucket boundary placement; 16 threads x 100,000 increments add up exactly; registry idempotency, type clash and the 10,000 guard; Prometheus text validity checked by parsing it back with a small test-only parser (escaping of quotes, backslashes and newlines in label values, cumulative buckets ending with `+Inf`, `_count` equals the `+Inf` bucket, HELP/TYPE once per family).
