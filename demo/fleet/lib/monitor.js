'use strict';
const { CITY } = require('./geo');

/** Per-second rates from a counter that only grows, over a sliding window. */
class Rate {
  constructor(windowMs = 5000) {
    this.windowMs = windowMs;
    this.samples = [];
  }

  push(value, now = Date.now()) {
    this.samples.push([now, value]);
    while (this.samples.length > 2 && now - this.samples[0][0] > this.windowMs) this.samples.shift();
  }

  perSecond() {
    if (this.samples.length < 2) return 0;
    const [t0, v0] = this.samples[0], [t1, v1] = this.samples[this.samples.length - 1];
    return t1 > t0 ? Math.max(0, (v1 - v0) * 1000 / (t1 - t0)) : 0;
  }
}

const pct = (sorted, p) => (sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))] : 0);

/** At most `max` driver dots, evenly spread over the fleet: [lat, lng, 1 if busy]. */
function sampleDrivers(drivers, max = 1200) {
  const step = Math.max(1, Math.ceil(drivers.length / max));
  const out = [];
  for (let i = 0; i < drivers.length; i += step) {
    const d = drivers[i];
    out.push([+d.lat.toFixed(5), +d.lng.toFixed(5), d.state === 'busy' ? 1 : 0]);
  }
  return out;
}

/** request -> matched time over rides matched in the last windowMs. */
function matchLatency(rides, windowMs = 60000, now = Date.now()) {
  const lat = [];
  for (const r of rides.values()) if (r.matchedAt && now - r.matchedAt <= windowMs) lat.push(r.matchedAt - r.requestedAt);
  lat.sort((a, b) => a - b);
  return { count: lat.length, p50: pct(lat, 0.5), p99: pct(lat, 0.99) };
}

/** Everything the dashboard shows, as plain JSON. */
function buildState({ startedAt, config, sim, dispatch, billing, notifier, bank, control, rates, queues, compare, now = Date.now() }) {
  const s = sim.stats;
  return {
    now,
    uptimeSec: Math.round((now - startedAt) / 1000),
    config,
    city: CITY,
    rates: Object.fromEntries(Object.entries(rates).map(([k, r]) => [k, +r.perSecond().toFixed(1)])),
    totals: { pings: s.pingsSent, requests: s.requests, matched: s.matched, unmatched: s.unmatched, completed: s.completed, charged: bank.stats.charged, pushes: notifier.stats.sent },
    latency: matchLatency(sim.rides, 60000, now),
    bank: { down: !!control.get().bankDown, ...bank.stats },
    services: { tracker: dispatch.running('tracker'), matcher: dispatch.running('matcher'), billing: billing.running(), notifier: notifier.running() },
    trackedDrivers: dispatch.grid.size,
    tripsRunning: sim._trips,
    queues,
    drivers: sampleDrivers(sim.drivers),
    compare,
  };
}

module.exports = { Rate, sampleDrivers, matchLatency, buildState };
