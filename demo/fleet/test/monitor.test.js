'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { Rate, sampleDrivers, matchLatency, buildState } = require('../lib/monitor');
const { startDispatch, startBilling, startNotifier } = require('../lib/services');
const { Bank } = require('../lib/bank');

test('Rate turns a growing counter into a per-second figure over a sliding window', () => {
  const r = new Rate(5000);
  r.push(0, 0); r.push(100, 1000); r.push(300, 2000);
  assert.equal(Math.round(r.perSecond()), 150);                    // 300 in 2 s
  for (let t = 3; t <= 10; t++) r.push(300 + (t - 2) * 10, t * 1000);
  assert.equal(Math.round(r.perSecond()), 10, 'old samples fall out of the window');
  assert.equal(new Rate().perSecond(), 0);
});

test('sampleDrivers keeps every dot for a small fleet and spreads a cap over a big one', () => {
  const drivers = Array.from({ length: 5000 }, (_, i) => ({ lat: 12.9 + i / 1e5, lng: 77.5, state: i % 2 ? 'busy' : 'idle' }));
  assert.equal(sampleDrivers(drivers.slice(0, 100)).length, 100);
  const s = sampleDrivers(drivers, 1200);
  assert.ok(s.length <= 1200 && s.length > 800, 'got ' + s.length);
  assert.deepEqual(s[0].length, 3);
  assert.ok(s.some(d => d[2] === 1) && s.some(d => d[2] === 0));
});

test('matchLatency only looks at recently matched rides', () => {
  const now = 1_000_000;
  const rides = new Map([
    ['a', { requestedAt: now - 5000, matchedAt: now - 4900 }], ['b', { requestedAt: now - 3000, matchedAt: now - 2700 }],
    ['old', { requestedAt: now - 200000, matchedAt: now - 190000 }], ['pending', { requestedAt: now - 10 }],
  ]);
  const l = matchLatency(rides, 60000, now);
  assert.equal(l.count, 2);
  assert.equal(l.p50, 300);
  assert.equal(matchLatency(new Map(), 60000, now).count, 0);
});

test('services can be crashed and restarted, and buildState reports it', () => {
  const handlers = {};
  const fake = { worker(q, h) { const w = { crashed: false, _crashed: false, start() { return this; }, crash() { this._crashed = true; }, stop: async () => {} }; handlers[q] = w; return w; }, publish: async () => ({}) };
  const dispatch = startDispatch({ ember: fake });
  const bank = new Bank({ latencyMs: 0 });
  const billing = startBilling({ ember: fake, bank });
  const notifier = startNotifier({ ember: fake });
  assert.equal(billing.running(), true);
  billing.crash();
  assert.equal(billing.running(), false);
  billing.restart();
  assert.equal(billing.running(), true, 'a fresh worker replaces the crashed one');
  dispatch.crash('matcher');
  assert.equal(dispatch.running('matcher'), false);
  assert.equal(dispatch.running('tracker'), true, 'the tracker is unaffected');
  dispatch.restart('matcher');
  assert.equal(dispatch.running('matcher'), true);

  const sim = { stats: { pingsSent: 10, requests: 4, matched: 3, unmatched: 0, completed: 2 }, rides: new Map(), drivers: [{ lat: 12.9, lng: 77.5, state: 'idle' }], _trips: 1 };
  const rates = { pings: new Rate(), requests: new Rate(), matched: new Rate(), completed: new Rate(), charged: new Rate() };
  const state = buildState({ startedAt: Date.now() - 5000, config: { drivers: 1 }, sim, dispatch, billing, notifier, bank, control: { get: () => ({ bankDown: true }) }, rates, queues: { billing: { dead: 2 } }, compare: { available: false } });
  assert.equal(state.bank.down, true);
  assert.equal(state.totals.matched, 3);
  assert.equal(state.services.billing, true);
  assert.equal(state.queues.billing.dead, 2);
  assert.equal(state.drivers.length, 1);
  assert.ok(state.city.minLat < state.city.maxLat);
  JSON.stringify(state);                                              // plain JSON all the way down
});
