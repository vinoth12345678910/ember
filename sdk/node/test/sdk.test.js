'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { Ember, EmberError } = require('../ember-sdk');

/** A tiny fake of the console's app API: one topic, one queue, in memory. */
function fakeServer() {
  const state = { published: [], queue: [], acked: [], nacked: [], released: [], extended: [], calls: [], overloadNext: 0, claimDelayMs: 0, nextOffset: 0 };
  const server = http.createServer((req, res) => {
    let body = '';
    req.on('data', d => body += d);
    req.on('end', async () => {
      const json = body ? JSON.parse(body) : {};
      state.calls.push({ method: req.method, url: req.url, body: json, auth: req.headers.authorization });
      const send = (status, obj, headers = {}) => { res.writeHead(status, { 'Content-Type': 'application/json', ...headers }); res.end(JSON.stringify(obj)); };
      if (req.headers.authorization !== 'Bearer emb_test') return send(401, { error: 'unknown or revoked API key' });
      if (req.url.startsWith('/api/app/topics/')) {
        if (state.overloadNext > 0) { state.overloadNext--; return send(503, { error: 'overloaded' }, { 'Retry-After': '0' }); }
        if (json.records) { state.published.push(...json.records); return send(200, { results: json.records.map((r, i) => ({ partition: 0, offset: state.published.length - json.records.length + i })) }); }
        state.published.push(json);
        return send(200, { topic: 't', partition: 0, offset: state.published.length - 1 });
      }
      const m = req.url.match(/^\/api\/app\/queues\/([^/]+)(?:\/(\w+))?$/);
      if (!m) return send(404, { error: 'no such endpoint' });
      if (m[1] === 'missing') return send(404, { error: 'no such queue' });
      if (!m[2]) return send(200, { queue: m[1], topic: 'orders', waiting: state.queue.length, inFlight: 0, retrying: 0, dead: 0, partitions: [] });
      if (m[2] === 'claim') {
        if (state.claimDelayMs) await new Promise(r => setTimeout(r, state.claimDelayMs));
        const taken = state.queue.splice(0, json.max);
        return send(200, { messages: taken });
      }
      const key = h => `${h.partition}:${h.offset}:${h.attempt}`;
      const list = { ack: state.acked, nack: state.nacked, release: state.released, extend: state.extended }[m[2]];
      json.handles.forEach(h => list.push({ ...h, reason: json.reason, worker: json.worker, leaseMs: json.leaseMs }));
      return send(200, { settled: json.handles.length, rejected: 0 });
    });
  });
  return new Promise(resolve => server.listen(0, '127.0.0.1', () => {
    const base = `http://127.0.0.1:${server.address().port}`;
    const add = (n, value = i => `{"n":${i}}`) => { for (let i = 0; i < n; i++) state.queue.push({ partition: i % 2, offset: state.nextOffset++, attempt: 1, timestamp: 1, key: 'k' + i, value: value(i) }); };
    resolve({ state, base, add, close: () => new Promise(r => { server.closeAllConnections?.(); server.close(r); }) });
  }));
}

const until = async (cond, ms = 5000) => { const t = Date.now(); while (!cond()) { if (Date.now() - t > ms) throw new Error('timed out waiting'); await new Promise(r => setTimeout(r, 5)); } };

test('publish sends strings as-is, objects as JSON, and the key as text', async () => {
  const f = await fakeServer();
  const ember = new Ember({ baseUrl: f.base, apiKey: 'emb_test' });
  await ember.publish('orders', { key: 7, value: { total: 12 } });
  await ember.publish('orders', { value: 'plain' });
  assert.deepEqual(f.state.published, [{ key: '7', value: '{"total":12}' }, { key: '', value: 'plain' }]);
  await f.close();
});

test('publishBatch splits into chunks of 500 and keeps the order of the results', async () => {
  const f = await fakeServer();
  const ember = new Ember({ baseUrl: f.base, apiKey: 'emb_test' });
  const results = await ember.publishBatch('orders', Array.from({ length: 1200 }, (_, i) => ({ key: i, value: 'v' + i })));
  assert.equal(results.length, 1200);
  assert.deepEqual(results.map(r => r.offset), Array.from({ length: 1200 }, (_, i) => i));
  assert.deepEqual(f.state.calls.filter(c => c.url.startsWith('/api/app/topics')).map(c => c.body.records.length), [500, 500, 200]);
  await f.close();
});

test('an overloaded gateway (503) is retried, other errors are not', async () => {
  const f = await fakeServer();
  const ember = new Ember({ baseUrl: f.base, apiKey: 'emb_test' });
  f.state.overloadNext = 2;
  await ember.publish('orders', { value: 'x' });
  assert.equal(f.state.calls.filter(c => c.url.startsWith('/api/app/topics')).length, 3);
  await assert.rejects(new Ember({ baseUrl: f.base, apiKey: 'emb_wrong' }).publish('orders', { value: 'x' }), e => e instanceof EmberError && e.status === 401);
  await assert.rejects(ember.queue('missing'), e => e.status === 404 && /no such queue/.test(e.message));
  await f.close();
});

test('a transport failure is retried and then reported as an EmberError', async () => {
  const ember = new Ember({ baseUrl: 'http://127.0.0.1:1', apiKey: 'emb_test', maxRetries: 2 });
  await assert.rejects(ember.publish('t', { value: 'x' }), e => e instanceof EmberError && e.status === 0 && /cannot reach Ember/.test(e.message));
});

test('worker acks what the handler finishes and nacks, with the reason, what it throws', async () => {
  const f = await fakeServer();
  f.add(6);
  const ember = new Ember({ baseUrl: f.base, apiKey: 'emb_test' });
  const seen = [];
  const w = ember.worker('fulfil', async m => { seen.push(m.json().n); if (m.json().n === 3) throw new Error('bank said no'); }, { name: 'w1', concurrency: 4, waitMs: 50 }).start();
  await until(() => f.state.acked.length === 5 && f.state.nacked.length === 1);
  await w.stop();
  assert.deepEqual(seen.sort(), [0, 1, 2, 3, 4, 5]);
  assert.equal(f.state.nacked[0].reason, 'bank said no');
  assert.equal(f.state.nacked[0].offset, 3);
  assert.ok(f.state.acked.every(a => a.worker === 'w1'));
  assert.deepEqual({ acked: w.stats.acked, nacked: w.stats.nacked }, { acked: 5, nacked: 1 });
  await f.close();
});

test('returning Ember.RELEASE gives the message back without a failure', async () => {
  const f = await fakeServer();
  f.add(2);
  const ember = new Ember({ baseUrl: f.base, apiKey: 'emb_test' });
  const w = ember.worker('q', async m => (m.offset === 0 ? Ember.RELEASE : undefined), { waitMs: 50 }).start();
  await until(() => f.state.released.length === 1 && f.state.acked.length === 1);
  await w.stop();
  assert.equal(f.state.nacked.length, 0);
  await f.close();
});

test('the worker never runs more than `concurrency` handlers at once and acks are batched', async () => {
  const f = await fakeServer();
  f.add(40);
  const ember = new Ember({ baseUrl: f.base, apiKey: 'emb_test' });
  let running = 0, peak = 0;
  const w = ember.worker('q', async () => { running++; peak = Math.max(peak, running); await new Promise(r => setTimeout(r, 15)); running--; }, { concurrency: 5, waitMs: 50, ackDelayMs: 20 }).start();
  await until(() => f.state.acked.length === 40);
  await w.stop();
  assert.ok(peak <= 5 && peak >= 3, 'peak concurrency ' + peak);
  const ackCalls = f.state.calls.filter(c => c.url.endsWith('/ack')).length;
  assert.ok(ackCalls < 40, 'acks were grouped: ' + ackCalls + ' calls for 40 messages');
  await f.close();
});

test('a slow handler keeps its lease with extend calls', async () => {
  const f = await fakeServer();
  f.add(1);
  const ember = new Ember({ baseUrl: f.base, apiKey: 'emb_test' });
  const w = ember.worker('q', async () => { await new Promise(r => setTimeout(r, 250)); }, { extendEveryMs: 50, leaseMs: 1000, waitMs: 50 }).start();
  await until(() => f.state.acked.length === 1);
  await w.stop();
  assert.ok(f.state.extended.length >= 2, 'extended ' + f.state.extended.length + ' times');
  assert.equal(f.state.extended[0].leaseMs, 1000);
  await f.close();
});

test('stop() waits for running handlers, then gives back what is still running after the drain window', async () => {
  const f = await fakeServer();
  f.add(3);
  const ember = new Ember({ baseUrl: f.base, apiKey: 'emb_test' });
  const started = [];
  const w = ember.worker('q', async m => { started.push(m.offset); await new Promise(r => setTimeout(r, m.offset === 2 ? 5000 : 60)); }, { concurrency: 3, waitMs: 50, drainMs: 300 }).start();
  await until(() => started.length === 3);
  await w.stop();
  assert.equal(f.state.acked.length, 2, 'the two quick handlers finished and were acked');
  assert.deepEqual(f.state.released.map(h => h.offset), [2], 'the slow one was handed back instead of waiting for its lease');
  await f.close();
});

test('a failing claim is reported and retried; the worker keeps going', async () => {
  const f = await fakeServer();
  const errors = [];
  const ember = new Ember({ baseUrl: f.base, apiKey: 'emb_test', maxRetries: 0 });
  const bad = ember.worker('missing', async () => {}, { waitMs: 10, onError: e => errors.push(e) }).start();
  await until(() => errors.length >= 1);
  await bad.stop();
  assert.ok(errors[0] instanceof EmberError && errors[0].status === 404);
  await f.close();
});

test('under load the worker claims in batches, not one message per freed slot', async () => {
  const f = await fakeServer();
  f.add(400);
  const ember = new Ember({ baseUrl: f.base, apiKey: 'emb_test' });
  const w = ember.worker('q', async () => { await new Promise(r => setTimeout(r, 10)); }, { concurrency: 40, waitMs: 20 }).start();
  await until(() => f.state.acked.length === 400, 10000);
  await w.stop();
  const claims = f.state.calls.filter(c => c.url.endsWith('/claim')).map(c => c.body.max);
  const small = claims.filter(m => m < 10).length;
  assert.ok(claims.length < 120, `400 messages needed ${claims.length} claim calls`);
  assert.ok(small <= 6, `only the start and the end may claim fewer than 10 at a time, got ${small} of ${claims.length}: ${claims.slice(0, 20)}`);
  await f.close();
});

test('crash() abandons running messages without settling them, like a dead process', async () => {
  const f = await fakeServer();
  f.add(3);
  const ember = new Ember({ baseUrl: f.base, apiKey: 'emb_test' });
  const started = [];
  const w = ember.worker('q', async m => { started.push(m.offset); await new Promise(r => setTimeout(r, 150)); }, { concurrency: 3, waitMs: 50, ackDelayMs: 5 }).start();
  await until(() => started.length === 3);
  w.crash();
  await new Promise(r => setTimeout(r, 400));                  // the handlers finish, but a crashed worker settles nothing
  assert.equal(f.state.acked.length + f.state.nacked.length + f.state.released.length, 0);
  const claimsBefore = f.state.calls.filter(c => c.url.endsWith('/claim')).length;
  await new Promise(r => setTimeout(r, 200));
  assert.equal(f.state.calls.filter(c => c.url.endsWith('/claim')).length, claimsBefore, 'and claims nothing more');
  await f.close();
});
