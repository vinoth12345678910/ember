'use strict';

/** A box around a city (roughly Bengaluru), and a grid index so "nearest idle driver" does not scan every driver. */
const CITY = { minLat: 12.85, maxLat: 13.10, minLng: 77.45, maxLng: 77.75 };

const rad = d => (d * Math.PI) / 180;

/** Great-circle distance in km. */
function distKm(aLat, aLng, bLat, bLng) {
  const dLat = rad(bLat - aLat), dLng = rad(bLng - aLng);
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(rad(aLat)) * Math.cos(rad(bLat)) * Math.sin(dLng / 2) ** 2;
  return 6371 * 2 * Math.asin(Math.min(1, Math.sqrt(h)));
}

const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));

function randomPoint(rand = Math.random) {
  return { lat: CITY.minLat + rand() * (CITY.maxLat - CITY.minLat), lng: CITY.minLng + rand() * (CITY.maxLng - CITY.minLng) };
}

/** Latest known position of every driver, indexed by grid cell (0.01 degrees, about 1.1 km). */
class Grid {
  constructor(cell = 0.01) {
    this.cell = cell;
    this.cells = new Map();          // "ix:iy" -> Set(driverId)
    this.drivers = new Map();        // id -> { id, lat, lng, ts, state, cellKey }
  }

  _key(lat, lng) { return `${Math.floor(lat / this.cell)}:${Math.floor(lng / this.cell)}`; }

  /** Applies a ping. Returns false (and changes nothing) if it is older than what we already know: late or replayed pings must not move a driver back. */
  update(id, lat, lng, ts, state = 'idle') {
    const cur = this.drivers.get(id);
    if (cur && ts < cur.ts) return false;
    const key = this._key(lat, lng);
    if (cur && cur.cellKey !== key) this.cells.get(cur.cellKey)?.delete(id);
    if (!cur || cur.cellKey !== key) {
      let set = this.cells.get(key);
      if (!set) this.cells.set(key, set = new Set());
      set.add(id);
    }
    this.drivers.set(id, { id, lat, lng, ts, state, cellKey: key });
    return true;
  }

  get(id) { return this.drivers.get(id); }
  get size() { return this.drivers.size; }

  /** The nearest driver within maxKm for which accept(driver) is true, searching outwards ring by ring; null if none. */
  nearest(lat, lng, maxKm, accept) {
    const cx = Math.floor(lat / this.cell), cy = Math.floor(lng / this.cell);
    const maxRing = Math.ceil(maxKm / 1.1) + 1;
    let best = null, bestD = Infinity;
    for (let ring = 0; ring <= maxRing; ring++) {
      for (let x = cx - ring; x <= cx + ring; x++) {
        for (let y = cy - ring; y <= cy + ring; y++) {
          if (ring > 0 && Math.abs(x - cx) !== ring && Math.abs(y - cy) !== ring) continue;      // only the ring's border is new
          const set = this.cells.get(`${x}:${y}`);
          if (!set) continue;
          for (const id of set) {
            const d = this.drivers.get(id);
            if (!accept(d)) continue;
            const dist = distKm(lat, lng, d.lat, d.lng);
            if (dist < bestD && dist <= maxKm) { best = d; bestD = dist; }
          }
        }
      }
      if (best && bestD <= ring * 1.0) break;                  // nothing in a farther ring can be closer (a cell is at least ~1.1 km wide)
    }
    return best ? { driver: best, km: bestD } : null;
  }
}

module.exports = { CITY, distKm, clamp, randomPoint, Grid };
