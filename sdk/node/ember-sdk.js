'use strict';

/**
 * Ember SDK for Node (18+, no dependencies). Talks to the Ember console's app API with an API key.
 *
 *   const { Ember } = require('@ember/sdk');
 *   const ember = new Ember({ baseUrl: 'http://localhost:8088', apiKey: process.env.EMBER_KEY });
 *   await ember.publish('orders', { key: 'o-1', value: { total: 12 } });
 *   const worker = ember.worker('fulfil', async msg => { await ship(msg.json()); }, { concurrency: 20 });
 *   worker.start();                        // resolves/throws in the handler decide ack / nack
 *   process.on('SIGTERM', () => worker.stop());
 */

const RELEASE = Symbol('ember.release');

class EmberError extends Error {
  constructor(message, status, body) {
    super(message);
    this.name = 'EmberError';
    this.status = status;
    this.body = body;
  }
}

const sleep = (ms, signal) => new Promise(resolve => {
  const t = setTimeout(resolve, ms);
  if (signal) signal.addEventListener('abort', () => { clearTimeout(t); resolve(); }, { once: true });
});

class Ember {
  /**
   * @param {{baseUrl: string, apiKey: string, fetch?: Function, maxRetries?: number, timeoutMs?: number}} options
   */
  constructor({ baseUrl, apiKey, fetch: fetchImpl, maxRetries = 5, timeoutMs = 60000 }) {
    if (!baseUrl || !apiKey) throw new TypeError('baseUrl and apiKey are required');
    this.baseUrl = baseUrl.replace(/\/+$/, '');
    this.apiKey = apiKey;
    this.fetch = fetchImpl || globalThis.fetch;
    this.maxRetries = maxRetries;
    this.timeoutMs = timeoutMs;
    if (typeof this.fetch !== 'function') throw new Error('no fetch available: use Node 18+ or pass options.fetch');
  }

  /** One HTTP call. Retries network errors and 503 (overloaded, honouring Retry-After) with backoff; other errors throw EmberError. */
  async request(method, path, body, { retry = true, signal } = {}) {
    let attempt = 0;
    for (;;) {
      let res;
      try {
        res = await this.fetch(this.baseUrl + path, {
          method,
          headers: { Authorization: 'Bearer ' + this.apiKey, ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}) },
          body: body !== undefined ? JSON.stringify(body) : undefined,
          signal: signal || AbortSignal.timeout(this.timeoutMs),
        });
      } catch (e) {
        if (signal && signal.aborted) throw e;
        if (!retry || attempt >= this.maxRetries) throw new EmberError('cannot reach Ember: ' + (e.cause?.code || e.message), 0);
        await sleep(Math.min(5000, 100 * 2 ** attempt) * (0.5 + Math.random()));
        attempt++;
        continue;
      }
      let data = {};
      try { data = await res.json(); } catch { /* empty body */ }
      if (res.ok) return data;
      if (res.status === 503 && retry && attempt < this.maxRetries) {
        const wait = Number(res.headers.get('retry-after')) * 1000 || data.retryAfterMs || 1000;
        await sleep(wait * (0.8 + Math.random() * 0.4));
        attempt++;
        continue;
      }
      throw new EmberError(data.error || `request failed (${res.status})`, res.status, data);
    }
  }

  // ---------------------------------------------------------------- publishing

  /** Publishes one message. value may be a string or any JSON-serialisable value. Resolves with {topic, partition, offset}. */
  publish(topic, { key, value }) {
    return this.request('POST', `/api/app/topics/${encodeURIComponent(topic)}/messages`, { key: key == null ? '' : String(key), value: encode(value) });
  }

  /** Publishes many messages (sent in chunks of 500). Resolves with one {partition, offset} or {error} per message, in order. */
  async publishBatch(topic, messages) {
    const out = [];
    for (let i = 0; i < messages.length; i += 500) {
      const chunk = messages.slice(i, i + 500).map(m => ({ key: m.key == null ? '' : String(m.key), value: encode(m.value) }));
      const r = await this.request('POST', `/api/app/topics/${encodeURIComponent(topic)}/messages`, { records: chunk });
      out.push(...r.results);
    }
    return out;
  }

  // ---------------------------------------------------------------- queues

  /** Progress and retry policy of a queue: waiting, inFlight, retrying, dead, per-partition numbers. */
  queue(name) {
    return this.request('GET', `/api/app/queues/${encodeURIComponent(name)}`);
  }

  /** Claims up to max messages (waits up to waitMs for some to arrive). Low-level: most code wants worker(). */
  async claim(queue, { max = 10, leaseMs = 0, waitMs = 0, worker = 'default', signal } = {}) {
    const r = await this.request('POST', `/api/app/queues/${encodeURIComponent(queue)}/claim`, { max, leaseMs, waitMs, worker }, { signal });
    return r.messages.map(m => new Message(m));
  }

  ack(queue, messages, worker = 'default') { return this._settle(queue, 'ack', messages, worker); }
  release(queue, messages, worker = 'default') { return this._settle(queue, 'release', messages, worker); }
  nack(queue, messages, reason, worker = 'default') { return this._settle(queue, 'nack', messages, worker, { reason: String(reason || '').slice(0, 200) }); }
  extend(queue, messages, leaseMs = 0, worker = 'default') { return this._settle(queue, 'extend', messages, worker, { leaseMs }); }

  async _settle(queue, op, messages, worker, extra = {}) {
    let settled = 0, rejected = 0;
    for (let i = 0; i < messages.length; i += 500) {
      const handles = messages.slice(i, i + 500).map(m => m.handle);
      const r = await this.request('POST', `/api/app/queues/${encodeURIComponent(queue)}/${op}`, { worker, handles, ...extra });
      settled += r.settled;
      rejected += r.rejected;
    }
    return { settled, rejected };
  }

  /**
   * A worker: claims messages, runs your handler on each (up to `concurrency` at once) and settles them for you.
   * Handler returns/resolves: acked. Throws/rejects: nacked with the error message (retried with backoff, then dead-lettered).
   * Returns Ember.RELEASE: given back without counting as a failure. Slow handlers keep their lease automatically.
   */
  worker(queue, handler, options = {}) {
    return new Worker(this, queue, handler, options);
  }
}

class Message {
  constructor(m) {
    this.partition = m.partition;
    this.offset = m.offset;
    this.attempt = m.attempt;
    this.timestamp = m.timestamp;
    this.key = m.key;
    this.value = m.value;
  }

  /** The value parsed as JSON. */
  json() { return JSON.parse(this.value); }

  get handle() { return { partition: this.partition, offset: this.offset, attempt: this.attempt }; }
}

class Worker {
  constructor(ember, queue, handler, o) {
    this.ember = ember;
    this.queue = queue;
    this.handler = handler;
    this.name = o.name || 'default';
    this.concurrency = o.concurrency || 10;
    this.batchSize = Math.min(o.batchSize || this.concurrency, 500);
    this.waitMs = o.waitMs == null ? 20000 : o.waitMs;
    this.leaseMs = o.leaseMs || 0;                         // 0 = the queue's default
    this.extendEveryMs = o.extendEveryMs || 10000;
    this.drainMs = o.drainMs == null ? 30000 : o.drainMs;
    this.ackDelayMs = o.ackDelayMs == null ? 5 : o.ackDelayMs;
    this.onError = o.onError || (() => {});
    this.order = o.order || 'none';                        // 'key': messages with the same key run one at a time, in the order this worker received them
    if (this.order !== 'none' && this.order !== 'key') throw new TypeError("order must be 'none' or 'key'");
    this.latestWins = !!o.latestWins;                      // only the newest message of each key is handled; older ones are acked unseen
    this.latestWinsKeys = o.latestWinsKeys || 100000;      // how many keys' newest offset is remembered (least recently used are forgotten)
    this._newest = new Map();                              // key -> highest offset handled successfully
    this._tails = new Map();                               // key -> end of that key's chain (order: 'key')
    this.inFlight = new Map();                             // handle key -> Message (running or waiting to be settled)
    this.stats = { claimed: 0, acked: 0, nacked: 0, released: 0, rejected: 0, superseded: 0 };
    this._acks = []; this._ackTimer = null; this._settleChain = Promise.resolve();
    this._abort = new AbortController();
    this._stopping = false;
    this._loop = null;
    this._slotWaiters = [];
  }

  start() {
    if (this._loop) return this;
    this._loop = this._run();
    this._extender = setInterval(() => this._extendAll(), this.extendEveryMs);
    this._extender.unref?.();
    return this;
  }

  /** Stops claiming, lets running handlers finish (up to drainMs), settles everything, and gives back what never started. */
  async stop() {
    this._stopping = true;
    this._abort.abort();
    clearInterval(this._extender);
    this._wakeSlots();
    if (this._loop) await this._loop;
    const deadline = Date.now() + this.drainMs;
    while (this._running > 0 && Date.now() < deadline) await sleep(20);
    await this._flushAcks();
    const stuck = [...this.inFlight.values()].filter(e => e.running).map(e => e.message);
    if (stuck.length) await this._release(stuck);          // still running after the drain window: hand them to someone else now, not after the lease
    await this._settleChain;
  }

  /**
   * Chaos/testing helper: behaves like a process that died. Claims nothing more and settles nothing (no ack, no nack, no release), so its running
   * messages stay claimed until their lease runs out and are then redelivered to another worker.
   */
  crash() {
    this._crashed = true;
    this._stopping = true;
    this._abort.abort();
    clearInterval(this._extender);
    clearTimeout(this._ackTimer);
    this._acks.length = 0;
    this._wakeSlots();
  }

  get _running() { return [...this.inFlight.values()].filter(e => e.running).length; }

  _free() { return this.concurrency - this.inFlight.size; }

  /** Claim again once a quarter of the slots are free (or at once if nothing is running), so batches stay large under load. */
  _refillAt() { return this.inFlight.size === 0 ? 1 : Math.max(1, Math.floor(this.concurrency / 4)); }

  _wakeSlots() { this._slotWaiters.splice(0).forEach(r => r()); }

  async _run() {
    let failures = 0;
    while (!this._stopping) {
      if (this._free() < this._refillAt()) {               // wait until a worthwhile number of slots is free: one claim per freed slot would cost a Raft commit each
        await new Promise(r => this._slotWaiters.push(r));
        continue;
      }
      let messages;
      try {
        messages = await this.ember.claim(this.queue, { max: Math.min(this._free(), this.batchSize), leaseMs: this.leaseMs, waitMs: this.waitMs, worker: this.name, signal: this._abort.signal });
        failures = 0;
      } catch (e) {
        if (this._stopping) break;
        this.onError(e, { phase: 'claim' });
        if (e instanceof EmberError && e.status >= 400 && e.status < 500 && e.status !== 429) {
          await sleep(Math.min(30000, 1000 * 2 ** Math.min(failures++, 5)), this._abort.signal);   // misconfigured (bad key, no such queue): keep trying slowly
        } else {
          await sleep(Math.min(10000, 200 * 2 ** Math.min(failures++, 6)) * (0.5 + Math.random()), this._abort.signal);
        }
        continue;
      }
      if (this._stopping) {                                // claimed just as we were told to stop: give them straight back
        await this._release(messages);
        break;
      }
      this.stats.claimed += messages.length;
      const skip = this.latestWins ? this._supersededInBatch(messages) : new Set();
      for (const m of messages) this._process(m, skip.has(m));
    }
  }

  /** In one claimed batch only the newest message of a key matters (the same key is always in the same partition, so a bigger offset is newer). */
  _supersededInBatch(messages) {
    const newest = new Map();
    for (const m of messages) {
      if (m.key == null) continue;
      const cur = newest.get(m.key);
      if (!cur || m.offset > cur.offset) newest.set(m.key, m);
    }
    const skip = new Set();
    for (const m of messages) if (m.key != null && newest.get(m.key) !== m) skip.add(m);
    return skip;
  }

  _isSuperseded(m) {
    if (!this.latestWins || m.key == null) return false;
    const seen = this._newest.get(m.key);
    return seen !== undefined && seen > m.offset;          // a newer message of this key was already handled: an older one (a redelivery, a backlog) must not undo it
  }

  _remember(m) {
    if (!this.latestWins || m.key == null) return;
    const seen = this._newest.get(m.key);
    this._newest.delete(m.key);                            // re-insert: Map order is the recency order
    this._newest.set(m.key, seen === undefined ? m.offset : Math.max(seen, m.offset));
    if (this._newest.size > this.latestWinsKeys) this._newest.delete(this._newest.keys().next().value);
  }

  /** Runs fn after everything queued before it for the same key has finished (whether it succeeded or not). */
  _serial(k, fn) {
    const tail = this._tails.get(k) || Promise.resolve();
    const run = tail.then(fn);
    const end = run.then(() => {}, () => {});
    this._tails.set(k, end);
    end.then(() => { if (this._tails.get(k) === end) this._tails.delete(k); });
    return run;
  }

  _process(m, skip = false) {
    const entry = { message: m, running: true };
    this.inFlight.set(key(m), entry);
    const work = () => {
      if (skip || this._isSuperseded(m)) { this.stats.superseded++; return undefined; }
      return Promise.resolve().then(() => this.handler(m)).then(r => { if (r !== RELEASE) this._remember(m); return r; });
    };
    const exec = this.order === 'key' && m.key != null ? this._serial(m.key, work) : Promise.resolve().then(work);
    exec.then(
      result => {
        entry.running = false;
        if (result === RELEASE) return this._release([m]);
        this._queueAck(m);
      },
      err => {
        entry.running = false;
        this.onError(err, { phase: 'handler', message: m });
        return this._settle('nack', [m], String(err && err.message || err)).then(r => { this.stats.nacked += r.settled; });
      },
    ).finally(() => this._wakeSlots());
  }

  _queueAck(m) {
    if (this._crashed) return;
    this._acks.push(m);
    if (this._acks.length >= 200) return void this._flushAcks();
    if (!this._ackTimer) this._ackTimer = setTimeout(() => this._flushAcks(), this.ackDelayMs);
  }

  async _flushAcks() {
    clearTimeout(this._ackTimer);
    this._ackTimer = null;
    const batch = this._acks.splice(0);
    if (!batch.length) return;
    const r = await this._settle('ack', batch);
    this.stats.acked += r.settled;
  }

  async _release(messages) {
    if (this._crashed) return;
    const r = await this._settle('release', messages);
    this.stats.released += r.settled;
  }

  /** Settles messages (serialised so a late ack cannot overtake a nack of the same message) and forgets them. */
  _settle(op, messages, reason) {
    if (this._crashed) return Promise.resolve({ settled: 0, rejected: 0 });
    const run = async () => {
      try {
        const r = op === 'nack' ? await this.ember.nack(this.queue, messages, reason, this.name)
          : op === 'ack' ? await this.ember.ack(this.queue, messages, this.name) : await this.ember.release(this.queue, messages, this.name);
        this.stats.rejected += r.rejected;
        return r;
      } catch (e) {
        this.onError(e, { phase: op });                    // not settled: the lease will run out and the message is redelivered (at-least-once)
        return { settled: 0, rejected: 0 };
      } finally {
        messages.forEach(m => this.inFlight.delete(key(m)));
        this._wakeSlots();
      }
    };
    this._settleChain = this._settleChain.then(run, run);
    return this._settleChain;
  }

  async _extendAll() {
    const held = [...this.inFlight.values()].filter(e => e.running).map(e => e.message);
    if (!held.length) return;
    try {
      const r = await this.ember.extend(this.queue, held, this.leaseMs, this.name);
      if (r.rejected) this.onError(new EmberError(`${r.rejected} message(s) were taken over by another worker`, 0), { phase: 'extend' });
    } catch (e) {
      this.onError(e, { phase: 'extend' });
    }
  }
}

const key = m => `${m.partition}:${m.offset}:${m.attempt}`;
const encode = v => (typeof v === 'string' ? v : JSON.stringify(v));

Ember.RELEASE = RELEASE;
module.exports = { Ember, EmberError, Message, Worker, RELEASE };
