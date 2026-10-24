'use strict';
const { Ember } = require('./sdk');
const { CITY, distKm, clamp, randomPoint } = require('./geo');

const poisson = lambda => {                       // Knuth: fine for the small lambdas used here
  const L = Math.exp(-lambda);
  let k = 0, p = 1;
  do { k++; p *= Math.random(); } while (p > L);
  return k - 1;
};

/**
 * Thousands of virtual drivers and riders.
 *  - every driver sends a GPS ping about every `pingMs` (spread evenly, so the load is steady, not bursty);
 *  - riders ask for rides at `ridersPerSec` (Poisson arrivals); a small share pay with a card that is always declined;
 *  - when the dispatcher matches a ride, the driver becomes busy for a trip of 3-8 s / timeScale, then the ride completes
 *    (a `completed` event is published, which the billing service charges) and the driver is idle again at the destination.
 * The simulator keeps the ground truth (`rides`) so a scenario can check that nothing was lost or charged twice.
 */
class Simulator {
  constructor({ ember, drivers = 1000, pingMs = 4000, ridersPerSec = 10, timeScale = 1, badCardRate = 0.01, tripSecs = [3, 8], runId = Date.now().toString(36) }) {
    this.ember = ember;
    this.pingMs = pingMs;
    this.ridersPerSec = ridersPerSec;
    this.timeScale = timeScale;
    this.badCardRate = badCardRate;
    this.tripSecs = tripSecs;
    this.runId = runId;
    this.tickMs = 100;
    this.drivers = Array.from({ length: drivers }, (_, i) => ({ id: `d-${String(i).padStart(5, '0')}`, ...randomPoint(), state: 'idle' }));
    this.byId = new Map(this.drivers.map(d => [d.id, d]));
    this.rides = new Map();                      // rideId -> { rideId, requestedAt, card, outcome: 'pending'|'matched'|'unmatched'|'completed', driverId, fare }
    this.stats = { pingsSent: 0, pingBatches: 0, pingsSkipped: 0, requests: 0, matched: 0, unmatched: 0, completed: 0, publishErrors: 0 };
    this._tick = 0;
    this._pinging = false;
    this._timers = new Set();
    this._trips = 0;
    this._rideSeq = 0;
    this._stopped = false;
  }

  start() {
    const buckets = Math.max(1, Math.round(this.pingMs / this.tickMs));
    this._pingTimer = setInterval(() => this._pingTick(buckets), this.tickMs);
    this._rideTimer = setInterval(() => this._rideTick(), this.tickMs);
    this._worker = this.ember.worker('sim-events', m => this._onEvent(m.json()), { name: 'simulator', concurrency: 50, waitMs: 1000, onError: () => {} }).start();
    return this;
  }

  /** Stops creating pings and rides; trips already running are allowed to finish (see drain). */
  stopLoad() {
    clearInterval(this._pingTimer);
    clearInterval(this._rideTimer);
    this._loadStopped = true;
  }

  async drain(timeoutMs = 60000) {
    const t0 = Date.now();
    while ((this._trips > 0 || [...this.rides.values()].some(r => r.outcome === 'pending')) && Date.now() - t0 < timeoutMs) await new Promise(r => setTimeout(r, 100));
  }

  async stop() {
    this.stopLoad();
    this._stopped = true;
    for (const t of this._timers) clearTimeout(t);
    await this._worker.stop();
  }

  async _pingTick(buckets) {
    const bucket = this._tick++ % buckets;
    if (this._pinging) { this.stats.pingsSkipped++; return; }          // the previous batch is still in flight: do not pile up
    const now = Date.now();
    const batch = [];
    for (let i = bucket; i < this.drivers.length; i += buckets) {
      const d = this.drivers[i];
      if (d.state === 'idle') {                                         // idle drivers cruise a little
        d.lat = clamp(d.lat + (Math.random() - 0.5) * 0.0008, CITY.minLat, CITY.maxLat);
        d.lng = clamp(d.lng + (Math.random() - 0.5) * 0.0008, CITY.minLng, CITY.maxLng);
      }
      batch.push({ key: d.id, value: { id: d.id, lat: d.lat, lng: d.lng, ts: now, state: d.state } });
    }
    if (!batch.length) return;
    this._pinging = true;
    try {
      await this.ember.publishBatch('driver-location', batch);
      this.stats.pingsSent += batch.length;
      this.stats.pingBatches++;
    } catch { this.stats.publishErrors++; } finally { this._pinging = false; }
  }

  async _rideTick() {
    const n = poisson(this.ridersPerSec * this.tickMs / 1000);
    if (!n) return;
    const batch = [];
    for (let i = 0; i < n; i++) {
      const rideId = `r-${this.runId}-${++this._rideSeq}`;
      const pickup = randomPoint(), dest = randomPoint();
      const card = Math.random() < this.badCardRate ? 'bad-' + rideId : 'tok-' + rideId;
      this.rides.set(rideId, { rideId, requestedAt: Date.now(), card, outcome: 'pending' });
      this.stats.requests++;
      batch.push({ key: 'u-' + Math.floor(Math.random() * 100000), value: { rideId, riderId: 'u-' + Math.floor(Math.random() * 100000), pickup, dest, card, ts: Date.now() } });
    }
    try { await this.ember.publishBatch('ride-requests', batch); } catch { this.stats.publishErrors++; }
  }

  async _onEvent(ev) {
    const ride = this.rides.get(ev.rideId);
    if (!ride) return;
    if (ev.type === 'matched' && ride.outcome === 'pending') {
      ride.outcome = 'matched';
      ride.matchedAt = Date.now();
      ride.driverId = ev.driverId;
      ride.fare = ev.fare;
      this.stats.matched++;
      const driver = this.byId.get(ev.driverId);
      if (driver) driver.state = 'busy';
      this._trips++;
      const tripMs = (this.tripSecs[0] + Math.random() * (this.tripSecs[1] - this.tripSecs[0])) * 1000 / this.timeScale;
      const t = setTimeout(() => { this._timers.delete(t); this._complete(ride, ev); }, tripMs);
      this._timers.add(t);
    } else if (ev.type === 'unmatched' && ride.outcome === 'pending') {
      ride.outcome = 'unmatched';
      this.stats.unmatched++;
    }
  }

  async _complete(ride, ev) {
    const driver = this.byId.get(ride.driverId);
    if (driver) { driver.lat = ev.dest.lat; driver.lng = ev.dest.lng; driver.state = 'idle'; }
    try {
      await this.ember.publish('ride-events', { key: ride.rideId, value: { type: 'completed', rideId: ride.rideId, riderId: ev.riderId, driverId: ride.driverId, fare: ride.fare, card: ride.card, ts: Date.now() } });
      ride.outcome = 'completed';
      this.stats.completed++;
    } catch { this.stats.publishErrors++; }
    this._trips--;
  }

  snapshot() {
    return { ...this.stats, drivers: this.drivers.length, busy: this.drivers.filter(d => d.state === 'busy').length, tripsRunning: this._trips };
  }
}

module.exports = { Simulator, poisson };
