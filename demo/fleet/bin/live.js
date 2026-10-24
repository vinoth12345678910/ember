#!/usr/bin/env node
'use strict';
/**
 * The Fleet demo as a long-running process, for the console's "Live Fleet" page: simulator + dispatcher + billing + notifier run here,
 * and a small HTTP port serves the state (GET /state, GET /compare) and accepts chaos commands (POST /control, POST /compare/run).
 * The port listens on localhost only; the console proxies it for administrators.
 *
 *   node bin/live.js [--drivers 2000] [--riders 10] [--ping-ms 4000] [--time-scale 2] [--bad-cards 0.02] [--port 8099]
 */
const http = require('node:http');
const { Bank } = require('../lib/bank');
const { Control } = require('../lib/control');
const { emberFor } = require('../lib/config');
const { Simulator } = require('../lib/simulator');
const { startDispatch, startBilling, startNotifier } = require('../lib/services');
const { Rate, buildState } = require('../lib/monitor');
const { Compare } = require('../lib/compare');

const arg = (name, fallback) => { const i = process.argv.indexOf('--' + name); return i > 0 ? Number(process.argv[i + 1]) : fallback; };
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

const control = new Control(process.env.FLEET_CONTROL || '/tmp/fleet-control-live.json');
control.set({ bankDown: false });

const config = { drivers: arg('drivers', 2000), ridersPerSec: arg('riders', 10), pingMs: arg('ping-ms', 4000), timeScale: arg('time-scale', 2), badCardRate: arg('bad-cards', 0.02) };
const bank = new Bank({ control });
const embers = { dispatch: emberFor('dispatch'), billing: emberFor('billing'), notifier: emberFor('notifier'), simulator: emberFor('simulator') };
const dispatch = startDispatch({ ember: embers.dispatch });
const billing = startBilling({ ember: embers.billing, bank, crashAfterChargeRate: 0.01 });
const notifier = startNotifier({ ember: embers.notifier });
const sim = new Simulator({ ember: embers.simulator, drivers: config.drivers, pingMs: config.pingMs, ridersPerSec: config.ridersPerSec, timeScale: config.timeScale, badCardRate: config.badCardRate }).start();
const compare = new Compare();
const startedAt = Date.now();

const rates = { pings: new Rate(), requests: new Rate(), matched: new Rate(), completed: new Rate(), charged: new Rate() };
setInterval(() => {
  rates.pings.push(sim.stats.pingsSent); rates.requests.push(sim.stats.requests); rates.matched.push(sim.stats.matched);
  rates.completed.push(sim.stats.completed); rates.charged.push(bank.stats.charged);
  sim.prune();
}, 1000).unref();

// queue health, from each service's own key (an app may look at the queues of topics it may consume)
let queues = {};
const QUEUE_OWNER = { tracker: 'dispatch', matcher: 'dispatch', billing: 'billing', notifier: 'notifier', 'sim-events': 'simulator' };
async function pollQueues() {
  const next = {};
  await Promise.all(Object.entries(QUEUE_OWNER).map(async ([q, owner]) => {
    try {
      const d = await embers[owner].queue(q);
      next[q] = { waiting: d.waiting, inFlight: d.inFlight, retrying: d.retrying, dead: d.dead };
    } catch { next[q] = queues[q] || null; }
  }));
  queues = next;
}
setInterval(() => pollQueues().catch(() => {}), 2000).unref();
pollQueues().catch(() => {});

const state = () => buildState({ startedAt, config: { ...config, ridersPerSec: sim.ridersPerSec, badCardRate: sim.badCardRate }, sim, dispatch, billing, notifier, bank, control, rates, queues, compare: compare.status() });

const SERVICES = { billing, notifier, matcher: dispatch, tracker: dispatch };
function act(body) {
  switch (body.action) {
    case 'bank': control.set({ bankDown: body.down === true }); log('bank is', body.down === true ? 'DOWN' : 'UP'); return {};
    case 'riders': { const n = Number(body.perSec); if (!(n >= 0 && n <= 500)) throw new Error('perSec must be 0-500'); sim.ridersPerSec = n; return {}; }
    case 'badCards': { const r = Number(body.rate); if (!(r >= 0 && r <= 1)) throw new Error('rate must be 0-1'); sim.badCardRate = r; return {}; }
    case 'crash': {
      const name = body.service, svc = SERVICES[name];
      const seconds = Math.max(1, Math.min(120, Number(body.seconds) || 10));
      if (!svc) throw new Error('service must be one of ' + Object.keys(SERVICES).join(', '));
      if (!svc.running(name)) throw new Error(name + ' is already down');
      log(`CRASH ${name} for ${seconds}s`);
      svc.crash(name);
      setTimeout(() => { svc.restart(name); log(name, 'restarted'); }, seconds * 1000).unref();
      return { restartsInSeconds: seconds };
    }
    default: throw new Error('unknown action');
  }
}

const server = http.createServer((req, res) => {
  let raw = '';
  req.on('data', d => { raw += d; if (raw.length > 10000) req.destroy(); });
  req.on('end', () => {
    const send = (status, obj) => { res.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(obj)); };
    try {
      if (req.method === 'GET' && req.url === '/state') return send(200, state());
      if (req.method === 'GET' && req.url === '/compare') return send(200, compare.status());
      if (req.method === 'POST' && req.url === '/control') return send(200, { ok: true, ...act(JSON.parse(raw || '{}')) });
      if (req.method === 'POST' && req.url === '/compare/run') { compare.run(); return send(200, { ok: true }); }
      send(404, { error: 'no such endpoint' });
    } catch (e) { send(400, { error: e.message }); }
  });
});
const port = arg('port', 8099);
server.listen(port, process.env.FLEET_HOST || '127.0.0.1', () => log(`Fleet live on http://127.0.0.1:${port}  (${config.drivers} drivers, ${config.ridersPerSec} ride requests/s)`));

process.on('SIGINT', async () => { log('stopping'); server.close(); sim.stopLoad(); await Promise.allSettled([sim.stop(), dispatch.stop(), billing.stop(), notifier.stop()]); process.exit(0); });
