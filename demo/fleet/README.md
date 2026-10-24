# Ember Fleet: a live ride-dispatch demo

Thousands of simulated drivers send GPS pings, riders request rides, a dispatcher matches each request to the nearest free driver, trips complete, and billing charges a flaky payment provider. Everything flows through Ember, and the failures that hurt real systems are injected on purpose: a **bank outage**, **cards that are always declined** (poison messages), and **workers that crash after charging but before acknowledging** (the classic duplicate).

```
 simulator ──ping──▶ driver-location ──▶ [tracker]  keeps the latest position of every driver (latest wins)
 simulator ──ride──▶ ride-requests   ──▶ [matcher]  nearest free driver, or RELEASE and try again soon
                                            │ matched
                                            ▼
                                       ride-events ──▶ [sim-events]  the simulator runs the trip, then publishes `completed`
                                                  ├──▶ [billing]     charges the bank; retries with backoff; dead-letters after 5 attempts
                                                  └──▶ [notifier]    push messages
```

Each box in brackets is an Ember **ack queue**; each service is a registered **app** with its own API key that may touch only the topics it needs.

## Run it

```bash
docker compose up -d --wait                                   # 3 brokers
mvn -q -DskipTests package -pl ember-web -am
java -jar ember-web/target/ember-web-0.1.0-SNAPSHOT-all.jar --secret=... --admin-password=...     # the console, http://localhost:8088

cd demo/fleet
export EMBER_ADMIN_USER=admin EMBER_ADMIN_PASSWORD=...
node bin/setup.js                                             # topics, queues with retry policies, one app + key per service
node bin/scenario.js --drivers 5000 --riders 20 --seconds 30 --outage 10 --ping-ms 4000
```

Open the console's **Queues** page while it runs: you can watch `billing` fill with retries during the outage and dead letters appear. Services can also run as separate processes (`node bin/simulator.js --drivers 5000`, `bin/dispatch.js`, `bin/billing.js`, `bin/notifier.js`) and the world can be changed live: `node bin/ops.js bank down` / `bank up`.

## Watch it live (console page "Live Fleet")

```bash
node bin/live.js --drivers 2000 --riders 10        # runs the whole system continuously and serves its state on 127.0.0.1:8099
# open the console (http://localhost:8088) as an administrator, Administration -> Live Fleet
```

The page shows a live map of the drivers (idle versus on a trip), pings and ride matches per second, request-to-match latency, the health of every queue, and a **Break things** panel: take the bank down, crash the billing, matcher or notifier workers for 10 s (they die without settling anything, like a killed process; their claimed messages return when the lease runs out), or change the ride rate. The console proxies the demo's port for administrators only (`--fleet-url`, default `http://127.0.0.1:8099`); the demo's own port has no login and listens on localhost.

## Why per-message tracking matters: the comparison

The same topic (3,000 messages, 6 partitions, every 100th message poison, 20 ms of work each) consumed four ways. `compare-bench` runs it (the page has a button when `EMBER_CLIENT_JAR` points at the client jar, and otherwise shows the last saved result, `results/compare-sample.json`):

```bash
java -jar ember-client/target/ember-client-0.1.0-SNAPSHOT-all.jar compare-bench --bootstrap=localhost:19092,localhost:19093,localhost:19094
```

| How the topic is consumed | Workers | Time | Healthy messages done | The 30 poison messages |
|---|---|---|---|---|
| consumer group, retry a poison message in place | 6 | stuck, stopped at the 30 s cap | 471 / 2,970 | retried for ever; everything behind them in their partition never ran |
| consumer group, skip a poison message after 3 tries | 6 | 12.3 s | 2,970 / 2,970 | **30 lost**, nowhere to put them; 69 healthy messages ran twice after rebalances |
| ack queue, same 6 workers | 6 | 11.1 s | 2,970 / 2,970 | 30 dead-lettered, kept, listed, retryable by an operator |
| ack queue, 24 workers on the same 6 partitions | 24 | 3.1 s | 2,970 / 2,970 | 30 dead-lettered |

Read it fairly: the group consumers are tuned the way an operator would (about 10 records per poll). An untuned consumer that works through hundreds of records between polls stops heartbeating after a rebalance and is expired, which triggers more rebalances and a collapse (we saw it while building this). A fifth option, building retry and dead-letter topics by hand on top of a consumer group, works but is application code; it is not measured here. One run on one cloud VM; the shape (stuck, lossy, complete, faster with more workers) is what matters, not the exact seconds.

## What the scenario proves

It plays the operator too: after the bank is back it retries the dead letters caused by the outage (and leaves the genuinely declined cards alone), then checks:

1. every ride request was resolved (matched or unmatched), and every matched ride completed;
2. every completed ride with a good card was charged, **exactly once** (the bank's ledger), even though workers "crashed" after charging and the messages were redelivered;
3. no declined card was ever charged, and only those remain dead-lettered;
4. all queues end empty: nothing stuck, nothing retrying.

Exit code 0 only if all checks pass.

## Measured

One cloud VM running 3 brokers (fsync on), the console, and the whole Node scenario in one process:

| Run | Result |
|---|---|
| 5,000 drivers pinging every 4 s, 20 ride requests/s, 30 s, 10 s bank outage | 1,242 GPS pings/s sustained (37,250, none skipped, all applied); 608 rides, all matched (request to match p50 181 ms, p99 303 ms) and all completed; 18 "crashed after charging" redeliveries absorbed by the idempotency key, 0 double charges; outage rejected 575 bank calls, 3 dead letters recovered by the operator, 30 declined cards left dead-lettered; all 9 checks pass |
| 300 drivers, 8 ride requests/s, 30 s, 20 s outage | all checks pass in 3 of 3 runs; the longer outage dead-lettered 11-32 charges that the operator then recovered |

## Honest limits

* One dispatcher process holds the driver map in memory (the matcher needs a global view). More dispatchers would need the map sharded by area; this demo does not do that.
* Pings carry the simulator's clock; the "newer ping" rule assumes the dispatcher and simulator clocks agree, as they do on one machine.
* The simulator, dispatcher and billing run in one Node process in the scenario, so the numbers include their CPU cost; the services also run as separate processes.
* The Live Fleet page and `live.js` run everything in one Node process, like the scenario; "crash" kills a worker (it stops claiming and settles nothing), not an operating-system process.
* The consumer group in the comparison is Ember's own (`EmberGroupConsumer`), which shares Kafka's position-per-partition model but is not Kafka; Ember's group consumer does eager rebalancing, so it is a conservative stand-in for a well-tuned Kafka consumer.

## Tests

`npm test` (no cluster needed) covers the monitor's rates and state, the grid index (against a brute-force scan), the bank's idempotency and failure modes, and the matcher, tracker and billing handlers with a fake Ember: duplicate requests, no-driver release and give-up, a failed announcement freeing the driver, stale pings, and a charge that is retried after a simulated crash.
