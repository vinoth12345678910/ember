'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { Grid, distKm, randomPoint } = require('../lib/geo');
const { Bank } = require('../lib/bank');
const { startDispatch, startBilling } = require('../lib/services');
const { Ember } = require('../lib/sdk');

/** Captures the handlers the services register, so tests can call them directly without a cluster. */
function fakeEmber() {
  const handlers = {}, published = [];
  return {
    handlers, published, failPublish: false,
    worker(queue, handler) { handlers[queue] = handler; return { start() { return this; }, stop: async () => {} }; },
    async publish(topic, msg) { if (this.failPublish) throw new Error('cluster unreachable'); published.push({ topic, ...msg }); return {}; },
  };
}
const msg = value => ({ json: () => value, attempt: 1 });

test('grid: nearest agrees with a brute-force scan, and stale pings never move a driver back', () => {
  const g = new Grid(), all = [];
  for (let i = 0; i < 400; i++) { const p = randomPoint(); all.push({ id: 'd' + i, ...p }); g.update('d' + i, p.lat, p.lng, 100, 'idle'); }
  for (let k = 0; k < 50; k++) {
    const q = randomPoint();
    const best = all.map(d => ({ d, km: distKm(q.lat, q.lng, d.lat, d.lng) })).filter(x => x.km <= 6).sort((a, b) => a.km - b.km)[0];
    const got = g.nearest(q.lat, q.lng, 6, () => true);
    assert.equal(got ? got.driver.id : null, best ? best.d.id : null);
  }
  assert.equal(g.update('d0', 13.0, 77.5, 50, 'idle'), false, 'an older ping is ignored');
  assert.notEqual(g.get('d0').lat, 13.0);
  assert.equal(g.update('d0', 13.0, 77.5, 200, 'busy'), true);
  assert.equal(g.get('d0').state, 'busy');
  assert.equal(g.nearest(13.0, 77.5, 0.5, d => d.state === 'idle' && d.id === 'd0'), null, 'the filter is honoured');
  assert.equal(g.size, 400, 'moving a driver does not duplicate it');
});

test('bank: idempotency key charges once, outage and declines fail, timeouts are transient', async () => {
  const control = { down: false, get() { return { bankDown: this.down }; } };
  const bank = new Bank({ control, timeoutRate: 0, latencyMs: 0 });
  await bank.charge({ idempotencyKey: 'r1', cardToken: 'tok-1', amount: 50 });
  assert.deepEqual(await bank.charge({ idempotencyKey: 'r1', cardToken: 'tok-1', amount: 50 }), { ok: true, duplicate: true });
  assert.equal(bank.ledger.size, 1);
  assert.equal(bank.stats.deduplicated, 1);
  await assert.rejects(bank.charge({ idempotencyKey: 'r2', cardToken: 'bad-9', amount: 5 }), /declined/);
  control.down = true;
  await assert.rejects(bank.charge({ idempotencyKey: 'r3', cardToken: 'tok-3', amount: 5 }), /unavailable/);
  assert.equal(bank.ledger.size, 1);
  control.down = false;
  const flaky = new Bank({ control, timeoutRate: 1, latencyMs: 0 });
  await assert.rejects(flaky.charge({ idempotencyKey: 'r4', cardToken: 'tok-4', amount: 5 }), /timeout/);
});

test('matcher: picks the nearest free driver, announces it once, and a redelivered request is not matched twice', async () => {
  const ember = fakeEmber();
  const d = startDispatch({ ember });
  const now = Date.now();
  ember.handlers.tracker(msg({ id: 'near', lat: 12.97, lng: 77.59, ts: now, state: 'idle' }));
  ember.handlers.tracker(msg({ id: 'far', lat: 12.995, lng: 77.62, ts: now, state: 'idle' }));      // about 3.6 km away: still inside the 6 km radius
  const req = { rideId: 'r1', riderId: 'u1', pickup: { lat: 12.971, lng: 77.591 }, dest: { lat: 13.0, lng: 77.6 }, card: 'tok', ts: now };
  assert.equal(await ember.handlers.matcher(msg(req)), undefined);
  assert.equal(ember.published.length, 1);
  assert.equal(ember.published[0].value.driverId, 'near');
  assert.equal(ember.published[0].value.type, 'matched');
  await ember.handlers.matcher(msg(req));                                       // the broker redelivered it
  assert.equal(ember.published.length, 1);
  assert.equal(d.stats.duplicateRequests, 1);
  // the driver is reserved: the next rider near it gets the other driver, not the same one
  await ember.handlers.matcher(msg({ ...req, rideId: 'r2', ts: Date.now() }));
  assert.equal(ember.published[1].value.driverId, 'far');
});

test('matcher: no driver nearby releases the request (no failure), and gives up with an unmatched event after maxWait', async () => {
  const ember = fakeEmber();
  startDispatch({ ember, maxWaitMs: 1000 });
  const req = { rideId: 'r9', riderId: 'u', pickup: { lat: 12.9, lng: 77.5 }, dest: { lat: 13, lng: 77.6 }, card: 'tok', ts: Date.now() };
  assert.equal(await ember.handlers.matcher(msg(req)), Ember.RELEASE);
  assert.equal(ember.published.length, 0);
  assert.equal(await ember.handlers.matcher(msg({ ...req, ts: Date.now() - 5000 })), undefined);
  assert.equal(ember.published[0].value.type, 'unmatched');
});

test('matcher: if the announcement cannot be published the driver is freed and the request is retried by the queue', async () => {
  const ember = fakeEmber();
  startDispatch({ ember });
  ember.handlers.tracker(msg({ id: 'only', lat: 12.97, lng: 77.59, ts: Date.now(), state: 'idle' }));
  const req = { rideId: 'r5', riderId: 'u', pickup: { lat: 12.97, lng: 77.59 }, dest: { lat: 13, lng: 77.6 }, card: 'tok', ts: Date.now() };
  ember.failPublish = true;
  await assert.rejects(ember.handlers.matcher(msg(req)), /unreachable/);          // thrown = nacked = retried
  ember.failPublish = false;
  await ember.handlers.matcher(msg(req));
  assert.equal(ember.published.length, 1);
  assert.equal(ember.published[0].value.driverId, 'only', 'the driver was not leaked by the failed attempt');
});

test('tracker: a driver reserved for a ride becomes free again only after a NEWER idle ping', async () => {
  const ember = fakeEmber();
  startDispatch({ ember });
  const t0 = Date.now();
  ember.handlers.tracker(msg({ id: 'd', lat: 12.97, lng: 77.59, ts: t0, state: 'idle' }));
  const req = id => ({ rideId: id, riderId: 'u', pickup: { lat: 12.97, lng: 77.59 }, dest: { lat: 13, lng: 77.6 }, card: 'tok', ts: Date.now() });
  await new Promise(r => setTimeout(r, 5));                                      // make sure the match time is later than the pings below
  await ember.handlers.matcher(msg(req('a')));
  assert.equal(ember.published.length, 1);
  ember.handlers.tracker(msg({ id: 'd', lat: 12.97, lng: 77.59, ts: t0 + 1, state: 'idle' }));   // sent before the match was decided
  assert.equal(await ember.handlers.matcher(msg(req('b'))), Ember.RELEASE, 'still reserved: that ping predates the match');
  ember.handlers.tracker(msg({ id: 'd', lat: 12.97, lng: 77.59, ts: Date.now() + 5, state: 'idle' }));
  assert.equal(await ember.handlers.matcher(msg(req('c'))), undefined, 'a newer idle ping frees it');
});

test('billing: ignores non-completed events, charges once even when the worker "crashes" after charging, and surfaces bank failures', async () => {
  const ember = fakeEmber();
  const bank = new Bank({ timeoutRate: 0, latencyMs: 0 });
  const b = startBilling({ ember, bank, crashAfterChargeRate: 1 });
  await ember.handlers.billing(msg({ type: 'matched', rideId: 'x' }));
  assert.equal(bank.ledger.size, 0);
  const done = { type: 'completed', rideId: 'r1', card: 'tok-1', fare: 80 };
  await assert.rejects(ember.handlers.billing(msg(done)), /crashed before acking/);   // charged, then died before the ack
  await assert.rejects(ember.handlers.billing(msg(done)), /crashed before acking/);   // redelivered
  assert.equal(bank.ledger.size, 1, 'charged once');
  assert.equal(bank.stats.deduplicated, 1, 'the second attempt was absorbed by the idempotency key');
  await assert.rejects(ember.handlers.billing(msg({ ...done, rideId: 'r2', card: 'bad-1' })), /declined/);
  assert.equal(b.stats.ignored, 1);
});
