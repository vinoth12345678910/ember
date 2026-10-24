#!/usr/bin/env node
'use strict';
/**
 * The whole Fleet story in one process, with checks. Simulated drivers ping, riders request, the dispatcher matches, trips complete,
 * billing charges a flaky bank; halfway through the bank goes down, some charges exhaust their retries and are dead-lettered, an
 * operator retries them after the bank is back, and at the end the scenario proves nothing was lost or charged twice.
 *
 *   node bin/scenario.js [--drivers 300] [--seconds 30] [--riders 8] [--outage 20] [--time-scale 3] [--ping-ms 2000] [--bad-cards 0.03]
 * Needs `node bin/setup.js` first and EMBER_ADMIN_USER / EMBER_ADMIN_PASSWORD (the scenario plays the operator who retries dead letters).
 */
const { Bank } = require('../lib/bank');
const { Control } = require('../lib/control');
const { Admin } = require('../lib/admin');
const { emberFor } = require('../lib/config');
const { Simulator } = require('../lib/simulator');
const { startDispatch, startBilling, startNotifier } = require('../lib/services');

const arg = (name, fallback) => { const i = process.argv.indexOf('--' + name); return i > 0 ? Number(process.argv[i + 1]) : fallback; };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const pct = (a, p) => { if (!a.length) return 0; const s = [...a].sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.floor(s.length * p))]; };
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

(async () => {
  const drivers = arg('drivers', 300), seconds = arg('seconds', 30), riders = arg('riders', 8), outage = arg('outage', 20);
  const control = new Control(process.env.FLEET_CONTROL || '/tmp/fleet-control-scenario.json');
  control.set({ bankDown: false });
  process.env.FLEET_CONTROL = control.path;
  const admin = new Admin();
  const runId = Date.now().toString(36);

  const bank = new Bank({ control });
  const dispatch = startDispatch({ ember: emberFor('dispatch') });
  const billing = startBilling({ ember: emberFor('billing'), bank, crashAfterChargeRate: arg('crash-rate', 0.03) });
  const notifier = startNotifier({ ember: emberFor('notifier') });
  const sim = new Simulator({ ember: emberFor('simulator'), drivers, pingMs: arg('ping-ms', 2000), ridersPerSec: riders, timeScale: arg('time-scale', 3), badCardRate: arg('bad-cards', 0.03), runId });
  log(`Fleet scenario ${runId}: ${drivers} drivers (a ping each ${arg('ping-ms', 2000)} ms), ${riders} ride requests/s for ${seconds} s, bank outage of ${outage} s starting at 5 s`);
  const startedAt = Date.now();
  sim.start();

  if (outage > 0) {
    setTimeout(() => { control.set({ bankDown: true }); log('>>> the bank is DOWN'); }, 5000);
    setTimeout(() => { control.set({ bankDown: false }); log('>>> the bank is back UP'); }, 5000 + outage * 1000);
  }
  const progress = setInterval(() => log('sim', JSON.stringify({ pings: sim.stats.pingsSent, requests: sim.stats.requests, matched: sim.stats.matched, completed: sim.stats.completed }), 'tracked drivers', dispatch.grid.size, 'charged', bank.stats.charged), 5000);
  await sleep(seconds * 1000);
  sim.stopLoad();
  log('load stopped; letting trips finish');
  await sim.drain(60000);

  const queueTotals = async () => {
    const out = {};
    for (const q of (await admin.call('GET', '/api/admin/queues')).queues) {
      const t = { waiting: 0, inFlight: 0, retrying: 0, dead: 0 };
      q.partitions.forEach(p => { t.waiting += Math.max(0, p.logEnd - p.next); t.inFlight += p.inFlight; t.retrying += p.redeliver; t.dead += p.dead; });
      out[q.subscription] = t;
    }
    return out;
  };
  // "drained" = every queue empty on several consecutive readings (the status comes from one broker's applied state, which can lag the leader by a moment)
  const drained = async (timeoutMs) => {
    const t0 = Date.now();
    let clean = 0, q;
    for (;;) {
      q = await queueTotals();
      const empty = ['tracker', 'matcher', 'billing', 'notifier', 'sim-events'].every(n => !q[n] || (q[n].waiting + q[n].inFlight + q[n].retrying === 0));
      clean = empty ? clean + 1 : 0;
      if (clean >= 4 || Date.now() - t0 > timeoutMs) return q;
      await sleep(400);
    }
  };
  log('waiting for the queues to drain');
  let q = await drained(90000);
  log('queues:', JSON.stringify(q));

  // the operator's job: the bank is back, so retry what died during the outage; leave the genuinely declined cards alone
  const dead = (await admin.call('GET', '/api/admin/queues/billing/dead?max=500')).items.filter(m => m.value.includes(runId));
  const recoverable = dead.filter(m => !JSON.parse(m.value).card.startsWith('bad'));
  log(`billing dead letters from this run: ${dead.length} (${recoverable.length} caused by the outage, ${dead.length - recoverable.length} declined cards)`);
  if (recoverable.length) {
    const r = await admin.call('POST', '/api/admin/queues/billing/dead/retry', { items: recoverable.map(m => ({ partition: m.partition, offset: m.offset })) });
    log(`operator retried ${r.settled} dead letters`);
    q = await drained(90000);
  }
  const deadAfter = (await admin.call('GET', '/api/admin/queues/billing/dead?max=500')).items.filter(m => m.value.includes(runId));

  const rides = [...sim.rides.values()];
  const completed = rides.filter(r => r.outcome === 'completed');
  const goodCompleted = completed.filter(r => !r.card.startsWith('bad'));
  const badCompleted = completed.filter(r => r.card.startsWith('bad'));
  const lat = rides.filter(r => r.matchedAt).map(r => r.matchedAt - r.requestedAt);
  const checks = [
    ['every ride request was resolved (matched or unmatched)', rides.every(r => r.outcome !== 'pending')],
    ['every matched ride completed', rides.filter(r => r.outcome === 'matched').length === 0],
    ['every completed ride with a good card was charged', goodCompleted.every(r => bank.ledger.has(r.rideId))],
    ['no declined card was ever charged', badCompleted.every(r => !bank.ledger.has(r.rideId))],
    ['nobody was charged twice (ledger entries == successful charges)', bank.ledger.size === bank.stats.charged],
    ['only the declined cards are left dead-lettered', deadAfter.length === badCompleted.length && deadAfter.every(m => JSON.parse(m.value).card.startsWith('bad'))],
    ['all queues are empty (nothing stuck or retrying)', ['tracker', 'matcher', 'billing', 'notifier', 'sim-events'].every(n => !q[n] || q[n].waiting + q[n].inFlight + q[n].retrying === 0)],
  ];
  checks.push(['retries after a crash were absorbed by the idempotency key (no double charge)', bank.stats.deduplicated >= (billing.stats.crashedBeforeAck > 0 ? 1 : 0)]);
  if (outage > 0) checks.push(['the outage really happened (the bank rejected calls)', bank.stats.outageFailures > 0]);

  console.log('\n================ Fleet scenario report ================');
  console.log(`virtual users      ${drivers} drivers, ${rides.length} riders (ride requests)   run ${((Date.now() - startedAt) / 1000).toFixed(0)} s`);
  console.log(`GPS pings          ${sim.stats.pingsSent} sent (${Math.round(sim.stats.pingsSent / seconds)}/s), ${sim.stats.pingsSkipped} ticks skipped for back-pressure, tracker applied ${dispatch.stats.pings}, ignored ${dispatch.stats.stalePings} stale`);
  console.log(`rides              ${sim.stats.matched} matched, ${sim.stats.unmatched} unmatched, ${completed.length} completed   (no driver nearby: ${dispatch.stats.retriedNoDriver} retries, ${dispatch.stats.duplicateRequests} duplicate requests ignored)`);
  console.log(`request->matched   p50 ${pct(lat, 0.5)} ms   p99 ${pct(lat, 0.99)} ms`);
  console.log(`billing            ${bank.stats.charged} charged (${billing.stats.crashedBeforeAck} workers "crashed" after charging, before acking), ${bank.stats.outageFailures} calls failed in the outage, ${bank.stats.timeouts} timeouts, ${bank.stats.declined} declines, ${bank.stats.deduplicated} duplicate charge attempts absorbed by the idempotency key`);
  console.log(`dead letters       ${dead.length} appeared during the run, ${recoverable.length} recovered by the operator, ${deadAfter.length} remain (declined cards)`);
  console.log(`notifications      ${notifier.stats.sent} push messages`);
  console.log('---------------------------------------------------------');
  let failed = 0;
  for (const [name, ok] of checks) { console.log(`${ok ? '  PASS' : '  FAIL'}  ${name}`); if (!ok) failed++; }
  console.log('=========================================================');

  clearInterval(progress);
  await Promise.all([dispatch.stop(), billing.stop(), notifier.stop(), sim.stop()]);
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error('scenario failed:', e); process.exit(2); });
