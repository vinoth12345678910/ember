'use strict';
const { Ember } = require('./sdk');
const { Grid, distKm } = require('./geo');

const sleep = ms => new Promise(r => setTimeout(r, ms));

/**
 * The dispatcher: a tracker that keeps the latest position of every driver, and a matcher that pairs each ride request with the nearest
 * free driver. Both are ack-queue workers, so a slow or crashing instance never blocks the others and nothing is lost.
 *
 * Tracker: pings are "latest wins": an older ping never moves a driver back, so a replay or a backlog is harmless.
 * Matcher: idempotent per ride (a redelivered request is not matched twice); no driver nearby -> RELEASE and try again soon,
 * and after maxWaitMs the ride is reported unmatched instead of waiting forever.
 */
function startDispatch({ ember, maxKm = 6, maxWaitMs = 15000, trackerConcurrency = 100, matcherConcurrency = 50 }) {
  const grid = new Grid();
  const reserved = new Map();                         // driverId -> time the match was decided
  const decided = new Map();                          // rideId -> driverId or 'unmatched'
  const stats = { pings: 0, stalePings: 0, matched: 0, unmatched: 0, retriedNoDriver: 0, duplicateRequests: 0 };

  const makeTracker = () => ember.worker('tracker', m => {
    const p = m.json();
    if (grid.update(p.id, p.lat, p.lng, p.ts, p.state)) stats.pings++; else stats.stalePings++;
  }, { name: 'tracker', concurrency: trackerConcurrency, batchSize: 100, waitMs: 1000, onError: () => {} });

  const free = d => {
    const at = reserved.get(d.id);
    if (at === undefined) return d.state === 'idle';
    if (d.ts <= at) return false;                      // a ping from before we matched this driver says nothing about now
    if (d.state === 'idle') { reserved.delete(d.id); return true; }
    return false;
  };

  const makeMatcher = () => ember.worker('matcher', async m => {
    const req = m.json();
    if (decided.has(req.rideId)) { stats.duplicateRequests++; return; }
    const found = grid.nearest(req.pickup.lat, req.pickup.lng, maxKm, free);
    if (!found) {
      if (Date.now() - req.ts > maxWaitMs) {
        await ember.publish('ride-events', { key: req.rideId, value: { type: 'unmatched', rideId: req.rideId, riderId: req.riderId, ts: Date.now() } });
        decided.set(req.rideId, 'unmatched');
        stats.unmatched++;
        return;
      }
      stats.retriedNoDriver++;
      await sleep(250);
      return Ember.RELEASE;                            // not a failure: just not yet
    }
    const driver = found.driver;
    reserved.set(driver.id, Date.now());
    decided.set(req.rideId, driver.id);
    const fare = Math.round(30 + 12 * distKm(req.pickup.lat, req.pickup.lng, req.dest.lat, req.dest.lng));
    try {
      await ember.publish('ride-events', { key: req.rideId, value: { type: 'matched', rideId: req.rideId, riderId: req.riderId, driverId: driver.id, pickup: req.pickup, dest: req.dest, fare, card: req.card, etaKm: +found.km.toFixed(2), ts: Date.now() } });
    } catch (e) {
      reserved.delete(driver.id);                       // not announced: undo, the request will be retried
      decided.delete(req.rideId);
      throw e;
    }
    stats.matched++;
  }, { name: 'matcher', concurrency: matcherConcurrency, batchSize: 25, waitMs: 1000, onError: () => {} });

  const svc = { tracker: makeTracker().start(), matcher: makeMatcher().start() };
  const factories = { tracker: makeTracker, matcher: makeMatcher };
  return {
    grid, stats,
    running: name => !svc[name]._crashed,
    /** Chaos: the worker dies without settling anything (its claimed messages return when their lease runs out). */
    crash: name => svc[name].crash(),
    restart: name => { if (svc[name]._crashed) svc[name] = factories[name]().start(); },
    stop: () => Promise.all([svc.tracker.stop(), svc.matcher.stop()]),
  };
}

/** Charges completed rides. At-least-once delivery + the bank's idempotency key = every ride is charged exactly once. */
function startBilling({ ember, bank, concurrency = 50, crashAfterChargeRate = 0 }) {
  const stats = { charged: 0, ignored: 0, crashedBeforeAck: 0 };
  const make = () => ember.worker('billing', async m => {
    const ev = m.json();
    if (ev.type !== 'completed') { stats.ignored++; return; }
    await bank.charge({ idempotencyKey: ev.rideId, cardToken: ev.card, amount: ev.fare });   // throws on outage / timeout / declined card: the SDK nacks it
    stats.charged++;
    if (Math.random() < crashAfterChargeRate) {                           // the classic at-least-once duplicate: charged, then the worker "dies" before acking
      stats.crashedBeforeAck++;
      throw new Error('worker crashed before acking');                  // the message is retried; the bank's idempotency key makes the retry harmless
    }
  }, { name: 'billing', concurrency, batchSize: 25, waitMs: 1000, onError: () => {} });
  let worker = make().start();
  return { stats, running: () => !worker._crashed, crash: () => worker.crash(), restart: () => { if (worker._crashed) worker = make().start(); }, stop: () => worker.stop() };
}

/** Sends a push notification for each match and each completed ride (a pretend one that sometimes fails and is retried). */
function startNotifier({ ember, failRate = 0.005, concurrency = 50 }) {
  const stats = { sent: 0 };
  const make = () => ember.worker('notifier', async m => {
    const ev = m.json();
    if (ev.type === 'unmatched') return;
    if (Math.random() < failRate) throw new Error('push gateway timeout');
    stats.sent++;
  }, { name: 'notifier', concurrency, batchSize: 25, waitMs: 1000, onError: () => {} });
  let worker = make().start();
  return { stats, running: () => !worker._crashed, crash: () => worker.crash(), restart: () => { if (worker._crashed) worker = make().start(); }, stop: () => worker.stop() };
}

module.exports = { startDispatch, startBilling, startNotifier };
