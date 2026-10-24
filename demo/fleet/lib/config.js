'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { Ember } = require('./sdk');

const KEYS = path.join(__dirname, '..', '.keys.json');

const baseUrl = () => process.env.EMBER_URL || 'http://localhost:8088';

/** The API keys `bin/setup.js` wrote, one per service (each service can only touch its own topics). */
function loadKeys() {
  try {
    return JSON.parse(fs.readFileSync(KEYS, 'utf8'));
  } catch {
    throw new Error('no demo keys yet: run `node bin/setup.js` first (it needs EMBER_ADMIN_USER and EMBER_ADMIN_PASSWORD)');
  }
}

function saveKeys(keys) {
  fs.writeFileSync(KEYS, JSON.stringify(keys, null, 2), { mode: 0o600 });
}

function emberFor(service) {
  const key = loadKeys()[service];
  if (!key) throw new Error(`no key for ${service}: run bin/setup.js`);
  return new Ember({ baseUrl: baseUrl(), apiKey: key });
}

module.exports = { baseUrl, loadKeys, saveKeys, emberFor };
