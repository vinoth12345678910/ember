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
* The comparison with a position-only (Kafka-style) consumer is not part of this branch.

## Tests

`npm test` (no cluster needed) covers the grid index (against a brute-force scan), the bank's idempotency and failure modes, and the matcher, tracker and billing handlers with a fake Ember: duplicate requests, no-driver release and give-up, a failed announcement freeing the driver, stale pings, and a charge that is retried after a simulated crash.
