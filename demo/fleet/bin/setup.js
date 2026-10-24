#!/usr/bin/env node
'use strict';
/**
 * Creates everything the Fleet demo needs in a running Ember cluster + console, and writes one API key per service to .keys.json.
 *   EMBER_URL=http://localhost:8088 EMBER_ADMIN_USER=admin EMBER_ADMIN_PASSWORD=... node bin/setup.js
 * Safe to run again: existing topics and queues are kept, apps get a fresh key and their permissions are re-applied.
 */
const { Admin } = require('../lib/admin');
const { saveKeys, baseUrl } = require('../lib/config');

const TOPICS = [['driver-location', 12], ['ride-requests', 6], ['ride-events', 6]];

//        queue          topic              fromStart  attempts  backoff   max     lease
const QUEUES = [
  ['tracker',    'driver-location', false, 3, 200, 2000, 10000],     // stale GPS pings are worthless: only new ones, few retries
  ['matcher',    'ride-requests',   true,  5, 500, 5000, 20000],
  ['billing',    'ride-events',     true,  5, 1000, 20000, 15000],   // a payment outage must not lose a charge: retry for a long time, then dead-letter for an operator
  ['notifier',   'ride-events',     true,  3, 500, 5000, 10000],
  ['sim-events', 'ride-events',     false, 3, 200, 2000, 10000],
];

// service app -> [topics it may produce to, topics it may consume from]. Nothing else.
const APPS = {
  simulator: ['fleet-simulator', ['driver-location', 'ride-requests', 'ride-events'], ['ride-events']],
  dispatch: ['fleet-dispatch', ['ride-events'], ['driver-location', 'ride-requests']],
  billing: ['fleet-billing', [], ['ride-events']],
  notifier: ['fleet-notifier', [], ['ride-events']],
};

(async () => {
  const admin = new Admin();
  console.log(`setting up Fleet on ${baseUrl()}`);
  for (const [name, partitions] of TOPICS) {
    const r = await admin.call('POST', '/api/admin/topics', { name, partitions }, [409]);
    console.log(`  topic ${name}: ${r._status === 409 ? 'exists' : partitions + ' partitions'}`);
  }
  for (const [name, topic, fromStart, maxAttempts, backoffMs, backoffMaxMs, leaseMs] of QUEUES) {
    const r = await admin.call('POST', '/api/admin/queues', { name, topic, fromStart, maxAttempts, backoffMs, backoffMaxMs, leaseMs }, [409]);
    console.log(`  queue ${name} on ${topic}: ${r._status === 409 ? 'exists' : `created (dead after ${maxAttempts} attempts)`}`);
  }
  const existing = (await admin.call('GET', '/api/admin/apps')).apps;
  const keys = {};
  for (const [service, [name, produce, consume]] of Object.entries(APPS)) {
    const found = existing.find(a => a.name === name);
    let key;
    if (found) {
      await admin.call('PATCH', `/api/admin/apps/${found.id}`, { produceTopics: produce, consumeTopics: consume, disabled: false });
      key = (await admin.call('POST', `/api/admin/apps/${found.id}/rotate-key`)).apiKey;
    } else {
      key = (await admin.call('POST', '/api/admin/apps', { name, description: 'Ember Fleet demo: ' + service, produceTopics: produce, consumeTopics: consume })).apiKey;
    }
    keys[service] = key;
    console.log(`  app ${name}: produce [${produce.join(', ')}] consume [${consume.join(', ')}]`);
  }
  saveKeys(keys);
  console.log('wrote demo/fleet/.keys.json (keep it private). Next: node bin/scenario.js');
})().catch(e => { console.error('setup failed:', e.message); process.exit(1); });
