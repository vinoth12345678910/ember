'use strict';
/**
 * Publishes N messages (every Mth one is "poison": its handler always throws), runs W workers on a queue and prints what happened.
 *   EMBER_URL=http://localhost:8088 EMBER_KEY=emb_... TOPIC=jobs QUEUE=jobs-q node examples/demo.js [count=5000] [poisonEvery=100] [workers=3] [handlerMs=20]
 * The app behind the key needs produce+consume permission on the topic; the queue must exist (Queues page, or `queue-create`).
 */
const { Ember } = require('../ember-sdk');

const [count = 5000, poisonEvery = 100, workers = 3, handlerMs = 20] = process.argv.slice(2).map(Number);
const ember = new Ember({ baseUrl: process.env.EMBER_URL || 'http://localhost:8088', apiKey: process.env.EMBER_KEY });
const topic = process.env.TOPIC || 'jobs', queue = process.env.QUEUE || 'jobs-q';

(async () => {
  const before = await ember.queue(queue);
  const t0 = Date.now();
  const messages = Array.from({ length: count }, (_, i) => ({ key: 'job-' + i, value: { n: i, bad: poisonEvery > 0 && i % poisonEvery === 0 } }));
  const results = await ember.publishBatch(topic, messages);
  const failed = results.filter(r => r.error).length;
  console.log(`published ${count - failed} messages in ${Date.now() - t0} ms (${failed} failed)`);

  const seen = new Set(); let redelivered = 0;
  const t1 = Date.now();
  const running = Array.from({ length: workers }, (_, w) => ember.worker(queue, async m => {
    if (m.attempt > 1) redelivered++;
    await new Promise(r => setTimeout(r, handlerMs));
    if (m.json().bad) throw new Error('bad payload for job ' + m.key);
    seen.add(m.offset + ':' + m.partition);
  }, { name: 'demo-' + w, concurrency: 50, waitMs: 1000, onError: () => {} }).start());

  for (;;) {
    await new Promise(r => setTimeout(r, 250));
    const q = await ember.queue(queue);
    if (q.waiting === 0 && q.inFlight === 0 && q.retrying === 0) break;
    if (Date.now() - t1 > 120000) { console.log('gave up waiting'); break; }
  }
  const secs = (Date.now() - t1) / 1000;
  await Promise.all(running.map(w => w.stop()));
  const q = await ember.queue(queue);
  const good = poisonEvery > 0 ? count - Math.ceil(count / poisonEvery) : count;
  console.log(`${workers} workers x 50 concurrent handlers (${handlerMs} ms of work each) finished in ${secs.toFixed(1)} s`);
  console.log(`  healthy messages handled: ${seen.size} of about ${good}  (${Math.round(seen.size / secs)}/s)`);
  console.log(`  dead-lettered: ${q.dead - before.dead}  (poison messages, after ${q.maxAttempts} attempts each)   redeliveries seen by handlers: ${redelivered}`);
  console.log('  worker stats:', running.map(w => JSON.stringify(w.stats)).join(' '));
})().catch(e => { console.error(e); process.exit(1); });
