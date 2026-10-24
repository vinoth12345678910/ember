'use strict';
// A worker for the "fulfil" queue. Run:  EMBER_URL=http://localhost:8088 EMBER_KEY=emb_... node examples/worker.js
const { Ember } = require('../ember-sdk');

const ember = new Ember({ baseUrl: process.env.EMBER_URL || 'http://localhost:8088', apiKey: process.env.EMBER_KEY });

const worker = ember.worker('fulfil', async message => {
  const order = message.json();
  if (!order.address) throw new Error('order has no address');       // nacked: retried with backoff, then dead-lettered
  await new Promise(r => setTimeout(r, 50));                         // pretend to call a shipping API
  console.log(`shipped order ${message.key} (attempt ${message.attempt})`);
}, { name: process.env.WORKER_NAME || 'worker-1', concurrency: 20, onError: (e, ctx) => console.error(ctx.phase, e.message) }).start();

process.on('SIGINT', async () => { await worker.stop(); console.log(worker.stats); process.exit(0); });
