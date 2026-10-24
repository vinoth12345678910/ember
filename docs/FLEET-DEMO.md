# Walkthrough: Ember Fleet from zero

A guided tour of the demo in [`demo/fleet`](../demo/fleet/README.md): what runs, what to click, what to break, and what each result means. About 15 minutes.

## 0. What you are looking at

A ride-dispatch system: thousands of simulated drivers ping their location, riders request rides, a matcher assigns the nearest free driver, trips complete, billing charges a flaky (mock) bank, a notifier sends pushes. Every service is an **ack queue** worker registered as its own **app** with its own API key that may touch only the topics it needs. The point: show what per-message acknowledgement buys you over a consumer group, and what Ember does when things fail.

## 1. Start it

```bash
docker compose up -d --wait                                   # 3 brokers
mvn -q -DskipTests package -pl ember-web -am
java -jar ember-web/target/ember-web-0.1.0-SNAPSHOT-all.jar --secret=CHANGE_ME --admin-password=CHANGE_ME
# console: http://localhost:8088   (user: admin)

cd demo/fleet
export EMBER_ADMIN_USER=admin EMBER_ADMIN_PASSWORD=CHANGE_ME
node bin/setup.js                       # topics, queues with retry policies, one app + key per service
```

`setup.js` is idempotent and writes the generated keys to `.keys.json` (git-ignored). In the console, **Administration -> Apps** now lists the services and **Queues** lists the queues.

## 2. Run the scenario (headless, 1 minute)

```bash
node bin/scenario.js --drivers 5000 --riders 20 --seconds 30 --outage 10 --ping-ms 4000
```

It runs the whole system, takes the bank down for 10 s, declines some cards for ever, crashes workers after they charged, plays the operator (retries the dead letters the outage caused), and then checks nine properties (listed in the [demo README](../demo/fleet/README.md): rides matched and completed, good cards charged exactly once, redeliveries absorbed by the idempotency key, the outage really happened, queues drained, ...). Exit code 0 means all passed.

## 3. Watch it live

```bash
node bin/live.js --drivers 2000 --riders 10
```

In the console as the administrator: **Live Fleet** in the sidebar.

* **Map**: drivers idle versus on a trip. **Rates**: pings/s, ride matches/s, request-to-match latency.
* **Queue health**: depth, in flight, waiting for retry, dead letters per queue.
* **Break things**: take the bank down; watch `billing` fill with retries and then drain. Crash the matcher: its claimed messages come back when their lease runs out and nothing is lost. Crash billing in the middle of a charge: the retry reuses the idempotency key, so nobody is charged twice.
* Open **Queues**, pick `billing` and its dead letters to see the cards that can never succeed; retry or discard them as an operator.

## 4. The comparison

Button on the Live Fleet page (or `compare-bench`, see the demo README). The same 3,000 messages, every 100th poison, through a consumer group and through an ack queue. Read the table together with [`LIMITS.md`](LIMITS.md): the shape matters, not the seconds.

## 5. Register your own app

In the console: **Apps -> Register app**, pick the topics and queues it may use. You get a key once (`emb_...`). Then:

```js
const { Ember } = require('@ember/sdk');            // sdk/node
const ember = new Ember({ baseUrl: 'http://localhost:8088', apiKey: process.env.EMBER_KEY });
await ember.publish('orders', { key: 'customer-7', value: { id: 1 } });
ember.worker('orders-queue', async message => { /* return = ack, throw = retry, return Ember.RELEASE = give back */ },
  { name: 'worker-1', concurrency: 16 }).start();
```

`return` acknowledges, `throw` schedules a retry with the queue's backoff (and dead-letters after the last attempt), `Ember.RELEASE` gives the message back now.

## 6. Where it stops being a demo

See [`LIMITS.md`](LIMITS.md).
