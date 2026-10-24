'use strict';
const fs = require('node:fs');

/**
 * A tiny control file lets an operator (or the scenario) change the world while the services run, for example "the bank is down".
 * Default path /tmp/fleet-control.json, override with FLEET_CONTROL.
 */
class Control {
  constructor(path = process.env.FLEET_CONTROL || '/tmp/fleet-control.json') {
    this.path = path;
    this._cache = {};
    this._at = 0;
  }

  get() {
    const now = Date.now();
    if (now - this._at > 200) {
      this._at = now;
      try { this._cache = JSON.parse(fs.readFileSync(this.path, 'utf8')); } catch { this._cache = {}; }
    }
    return this._cache;
  }

  set(patch) {
    const next = { ...this.get(), ...patch };
    fs.writeFileSync(this.path, JSON.stringify(next));
    this._cache = next;
    this._at = Date.now();
  }
}

module.exports = { Control };
