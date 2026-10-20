# Step 5 - Bench upgrade, cluster CLI commands, advertised addresses

## What we are building
**Bench (`ember-client` CLI `bench`).** Latency percentiles (p50 / p90 / p99 / p99.9 / max) from our own fixed-bucket histogram with 2% resolution, throughput (records/s and MB/s), error and retry counts, `--warmup-records` (excluded from the statistics), `--csv=FILE` (one row per measured request), `--acks-timeout` (request timeout in ms), and a **fixed-rate mode** `--rate=<records/s total>` in which send times follow a schedule and latency is measured from the **intended** send time. The closed-loop mode stays the default.

**CLI.** `cluster status --admin=h1:p1,h2:p2` (one table: nodes, readiness, controller, coordinator, and the role of every node in every Raft group), `cluster metrics --admin=h:p [--filter=prefix]`, and `healthcheck --admin=h:p` (exit 0 only when `/health/ready` answers 200, for the Docker `HEALTHCHECK`; the runtime image has no `curl`).

**Advertised addresses.** `--cluster=1@ember1:9092/localhost:19092,...` (the optional third part is the address clients must use) and `--advertise=host:port` for this broker. METADATA (v0, v1, v2) and FIND_COORDINATOR return the **advertised** address; brokers keep talking to each other on the `--cluster` addresses. No wire change.

## Why it is designed this way
**Coordinated omission, in a small example.** Suppose the target is one request every 10 ms and the broker stalls for 1 second at request 100. A closed-loop benchmark sends request 101 only when 100 has finished, so it records *one* slow request (about 1 s) and then fast ones: it "omits" the 99 requests that *should* have been sent during the stall and would each have waited. The honest average over the intended schedule is far worse: those 99 would have waited 990 ms, 980 ms, ... A fixed-rate benchmark that measures from the intended time records all 100 of them, and the p99 shows the stall. The closed loop answers "how fast can it go"; the fixed-rate loop answers "what does a user who arrives on schedule feel".

**Why our own histogram.** The client stays dependency-free. Bucket index = 32 sub-buckets per power of two, so any value is reported within about 2.2% of the truth, up to hours, in 1 KB of memory, and histograms from several producer threads add together exactly.

**The advertised-address problem.** A broker must be reachable at *different* names depending on who asks. Inside the Docker network the others call it `ember2:9092`; the client on your laptop must use `localhost:19093` (the host-mapped port). If METADATA told the laptop client `ember2:9092` it could not connect, and the classic symptom is "the bootstrap works but producing hangs". Kafka solves it with several listeners. We support **one advertised address per broker**; so in the compose demo clients run on the host. Multiple listeners are future work.

## Alternatives I rejected and why
* A histogram library (HdrHistogram): a dependency for 60 lines of logic.
* Measuring open-loop latency from the actual send time: that is exactly the omission we are removing.
* Making clients rewrite addresses: moves broker knowledge into every client.

## What goes wrong if you get it wrong
A benchmark that waits for the previous response before sending the next one reports a flattering p99. An advertised address that points at an unreachable name makes clients fail after a *successful* bootstrap, which is hard to diagnose. Peers must **not** use the advertised address: from inside the container `localhost:19093` is the container itself.

## Tradeoffs and honest limits
Percentiles are per **request** (a batch of N records is one request); in fixed-rate mode the intended time is that of the batch's first record. Under overload a fixed-rate run keeps sending late, so its latency grows without bound, which is the correct signal that the rate exceeds capacity. The CSV has one row per measured request, not per record.

## How it is tested
Histogram accuracy (values within 2.2%, merge, percentile edges); the fixed-rate schedule with a manual clock (intended times; latency measured from the schedule even when the sender is delayed; the backlog after a stall is visible); the CLI against a real in-process cluster (`cluster status` lists every node, `healthcheck` exit codes, `cluster metrics --filter`); bench output and CSV; `--cluster` parsing with and without the third part; and a cluster whose advertised addresses differ from its internal ones: METADATA and FIND_COORDINATOR return the advertised ones while the cluster works over the internal ones.
