# Ember SDK for Node

Publish to Ember and run workers on ack queues over HTTP. Node 18+, no dependencies.

```js
const { Ember } = require('@ember/sdk');
const ember = new Ember({ baseUrl: 'http://localhost:8088', apiKey: process.env.EMBER_KEY });

await ember.publish('orders', { key: 'o-1', value: { total: 12 } });            // objects are sent as JSON
await ember.publishBatch('orders', manyMessages);                                // chunks of 500, one result per message

const worker = ember.worker('fulfil', async message => {
  const order = message.json();
  await ship(order);                       // resolves: acked
  // throw new Error('...')                // throws: nacked with that reason, retried with backoff, dead-lettered after N attempts
  // return Ember.RELEASE                  // give it back without counting a failure (e.g. shutting down)
}, { name: 'worker-1', concurrency: 20 }).start();

process.on('SIGTERM', () => worker.stop());   // lets running handlers finish, hands back what is still running
```

## Setup

1. An admin creates a **queue** on a topic (console: Queues, or `queue-create`) with its retry policy.
2. An admin registers an **app** (console: Apps) allowed to consume (and, to publish, produce to) that topic, and gives you its key `emb_...`.
3. Use the key with `new Ember({ baseUrl, apiKey })`. Every worker of your app needs a unique `name` per running copy (default `"default"`); one app can never touch another app's messages.

## What the worker does for you

* claims in batches (long-polling when the queue is empty), runs up to `concurrency` handlers at once, claims again when a quarter of the slots are free;
* groups acks (`ackDelayMs`, 5 ms) so thousands of messages a second cost few requests;
* keeps the lease of slow handlers alive (`extendEveryMs`), and reports transport errors to `onError` and retries with backoff;
* `stop()` stops claiming, waits up to `drainMs` for handlers, then releases whatever is still running so another worker takes it at once instead of after the lease.

Delivery is **at-least-once**: a worker that dies, or runs past its lease, has its messages redelivered (`message.attempt > 1`), so handlers must be safe to repeat.

## Low-level calls

`ember.claim(queue, {max, waitMs, leaseMs, worker})`, `ember.ack / nack / release / extend(queue, messages, ...)`, `ember.queue(name)` (waiting, inFlight, retrying, dead, retry policy). Types in `index.d.ts`.

## Try it

```bash
EMBER_KEY=emb_... TOPIC=jobs QUEUE=jobs-q node examples/demo.js 5000 100 3 20   # 5000 messages, every 100th poison, 3 workers, 20 ms of work each
npm test                                                                          # offline: the tests use an in-process fake server
```
