'use strict';

/**
 * A pretend payment provider. It behaves like real ones do:
 *  - it can be down (control file {"bankDown": true}): every call fails;
 *  - it is slow and occasionally times out (a transient error, a retry works);
 *  - a card that is permanently bad is always declined (retrying will never help: a poison message);
 *  - it honours an idempotency key: charging twice with the same key charges once, so an at-least-once system can retry safely.
 */
class Bank {
  constructor({ control, timeoutRate = 0.02, latencyMs = 8 } = {}) {
    this.control = control;
    this.timeoutRate = timeoutRate;
    this.latencyMs = latencyMs;
    this.ledger = new Map();               // idempotencyKey -> { cardToken, amount, at }
    this.stats = { calls: 0, charged: 0, deduplicated: 0, timeouts: 0, outageFailures: 0, declined: 0 };
  }

  async charge({ idempotencyKey, cardToken, amount }) {
    this.stats.calls++;
    await new Promise(r => setTimeout(r, this.latencyMs * (0.5 + Math.random())));
    if (this.control?.get().bankDown) { this.stats.outageFailures++; throw new Error('bank unavailable (503)'); }
    if (Math.random() < this.timeoutRate) { this.stats.timeouts++; throw new Error('bank timeout'); }
    if (String(cardToken).startsWith('bad')) { this.stats.declined++; throw new Error('card declined: ' + cardToken); }
    if (this.ledger.has(idempotencyKey)) { this.stats.deduplicated++; return { ok: true, duplicate: true }; }
    this.ledger.set(idempotencyKey, { cardToken, amount, at: Date.now() });
    this.stats.charged++;
    return { ok: true };
  }
}

module.exports = { Bank };
