'use strict';

/* Ember console: landing page, sign-in/sign-up, consumer workspace, admin console. Hash-routed, no build step. */

const $ = (sel, root = document) => root.querySelector(sel);
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const num = n => Number(n || 0).toLocaleString();
const app = $('#app');

const store = {
  get(k) { try { return localStorage.getItem(k); } catch { return null; } },
  set(k, v) { try { v == null ? localStorage.removeItem(k) : localStorage.setItem(k, v); } catch { /* storage blocked: the session just lasts until reload */ } },
};
const session = { token: store.get('ember.token'), user: null };
let timer = null;      // auto-refresh of the current page
let navSeq = 0;        // guards against a slow page overwriting a newer one

const LOGO = `<svg viewBox="0 0 32 32" aria-hidden="true"><rect width="32" height="32" rx="8" fill="#1b1410"/><path d="M16 4c1 5 7 8 7 15a7 7 0 0 1-14 0c0-3 1.500-5 3-6.500 0 2 1 3 2 3.500C13.500 12 14 8 16 4z" fill="#ff7a29"/><path d="M16 15c.5 3 3 4 3 7a3 3 0 0 1-6 0c0-2 2-3.500 3-7z" fill="#ffc861"/></svg>`;

// ---------------------------------------------------------------- api

async function api(path, { method = 'GET', body } = {}) {
  const res = await fetch('/api' + path, {
    method,
    headers: { ...(body ? { 'Content-Type': 'application/json' } : {}), ...(session.token ? { Authorization: 'Bearer ' + session.token } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  let data = {};
  try { data = await res.json(); } catch { /* empty body */ }
  if (res.status === 401 && session.token && !path.startsWith('/auth/')) {
    signOut(false);
    toast('Your session ended. Please sign in again.', true);
    location.hash = '#/login';
    throw new Error('signed out');
  }
  if (!res.ok) throw new Error(data.error || 'request failed (' + res.status + ')');
  return data;
}

function toast(msg, bad = false) {
  const t = document.createElement('div');
  t.className = 'toast' + (bad ? ' bad' : '');
  t.textContent = msg;
  $('#toasts').append(t);
  setTimeout(() => t.remove(), 4200);
}

function signIn(token, user) {
  session.token = token; session.user = user;
  store.set('ember.token', token);
}
function signOut(navigate = true) {
  session.token = null; session.user = null;
  store.set('ember.token', null);
  if (navigate) location.hash = '#/';
}
const isAdmin = () => session.user?.role === 'ADMIN';
const home = () => (isAdmin() ? '#/admin' : '#/app');

// ---------------------------------------------------------------- routing

const routes = [
  [/^\/$/, publicPage(landing)],
  [/^\/login$/, publicPage(() => authPage('login'))],
  [/^\/register$/, publicPage(() => authPage('register'))],
  [/^\/app$/, shellPage('app', 'dashboard', dashboard)],
  [/^\/app\/topics$/, shellPage('app', 'topics', topicsPage)],
  [/^\/app\/topics\/(.+)$/, shellPage('app', 'topics', topicDetail)],
  [/^\/app\/messages$/, shellPage('app', 'messages', messagesPage)],
  [/^\/app\/publish$/, shellPage('app', 'publish', publishPage)],
  [/^\/app\/groups$/, shellPage('app', 'groups', groupsPage)],
  [/^\/admin$/, shellPage('admin', 'overview', adminOverview, true)],
  [/^\/admin\/topics$/, shellPage('admin', 'admintopics', adminTopics, true)],
  [/^\/admin\/users$/, shellPage('admin', 'users', adminUsers, true)],
  [/^\/admin\/apps$/, shellPage('admin', 'apps', adminApps, true)],
  [/^\/admin\/fleet$/, shellPage('admin', 'fleet', adminFleet, true)],
  [/^\/admin\/queues$/, shellPage('admin', 'queues', adminQueues, true)],
  [/^\/admin\/queues\/(.+)$/, shellPage('admin', 'queues', adminDead, true)],
];

async function navigate() {
  clearInterval(timer);
  const seq = ++navSeq;
  const [path, query] = (location.hash.slice(1) || '/').split('?');
  const params = new URLSearchParams(query || '');
  if (session.token && !session.user) {
    try { session.user = (await api('/me')).user; } catch { session.token = null; }
  }
  if (seq !== navSeq) return;
  for (const [re, handler] of routes) {
    const m = path.match(re);
    if (m) return handler(m.slice(1).map(decodeURIComponent), params, seq);
  }
  location.hash = '#/';
}
window.addEventListener('hashchange', navigate);

function publicPage(render) {
  return () => {
    if (session.user && /^#\/(login|register)/.test(location.hash)) { location.hash = home(); return; }
    app.innerHTML = '';
    render();
    window.scrollTo(0, 0);
  };
}

function shellPage(area, key, render, adminOnly = false) {
  return async (args, params, seq) => {
    if (!session.user) { location.hash = '#/login'; return; }
    if (adminOnly && !isAdmin()) { location.hash = '#/app'; toast('That area is for admins only.', true); return; }
    const u = session.user;
    const nav = (href, k, icon, label) => `<a class="item ${k === key ? 'on' : ''}" href="${href}"><span aria-hidden="true">${icon}</span>${label}</a>`;
    app.innerHTML = `<div class="shell">
      <aside class="side" id="side">
        <a class="brand" href="#/">${LOGO}Ember</a>
        <div class="group">Workspace</div>
        ${nav('#/app', 'dashboard', '◧', 'Dashboard')}
        ${nav('#/app/topics', 'topics', '☰', 'Topics')}
        ${nav('#/app/messages', 'messages', '✉', 'Browse messages')}
        ${nav('#/app/publish', 'publish', '➤', 'Publish')}
        ${nav('#/app/groups', 'groups', '⚇', 'Consumer groups')}
        ${isAdmin() ? `<div class="group">Administration</div>
          ${nav('#/admin', 'overview', '♥', 'Cluster health')}
          ${nav('#/admin/topics', 'admintopics', '＋', 'Manage topics')}
          ${nav('#/admin/fleet', 'fleet', '◎', 'Live Fleet')}
          ${nav('#/admin/queues', 'queues', '⇄', 'Queues')}
          ${nav('#/admin/apps', 'apps', '⬡', 'Apps')}
          ${nav('#/admin/users', 'users', '☺', 'Users')}` : ''}
        <div class="me">
          <div class="avatar">${esc(u.displayName[0].toUpperCase())}</div>
          <div class="who"><b>${esc(u.displayName)}</b><span>${isAdmin() ? 'Administrator' : 'Consumer'}</span></div>
          <button class="btn sm" id="logout" title="Sign out">Sign out</button>
        </div>
      </aside>
      <main class="main"><button class="btn sm menu-btn" id="menu" aria-label="Menu" style="margin-bottom:14px">☰ Menu</button><div id="view"><span class="spin"></span></div></main>
    </div>`;
    $('#logout').onclick = () => signOut();
    $('#menu').onclick = () => $('#side').classList.toggle('open');
    $('#side').addEventListener('click', e => { if (e.target.closest('a')) $('#side').classList.remove('open'); });
    try {
      await render($('#view'), args, params, () => seq === navSeq);
    } catch (e) {
      if (seq === navSeq && e.message !== 'signed out') $('#view').innerHTML = `<div class="alert">${esc(e.message)}</div><button class="btn" onclick="location.reload()">Retry</button>`;
    }
  };
}

/** Runs fn now and then every ms while this page is still the current one. */
function refreshEvery(ms, alive, fn) {
  timer = setInterval(() => { if (alive()) fn().catch(() => {}); }, ms);
}

// ---------------------------------------------------------------- landing

function landing() {
  const cta = session.user
    ? `<a class="btn primary" href="${home()}">Open console</a>`
    : `<a class="btn" href="#/login">Sign in</a><a class="btn primary" href="#/register">Get started</a>`;
  app.innerHTML = `
  <header class="pub-head"><div class="wrap">
    <a class="brand" href="#/">${LOGO}Ember</a>
    <nav class="nav"><a class="link" href="#features" data-s="features">Features</a><a class="link" href="#roles" data-s="roles">Who it's for</a><a class="link" href="#how" data-s="how">How it works</a>${cta}</nav>
  </div></header>
  <section class="hero"><div class="wrap">
    <div>
      <span class="eyebrow">Raft-replicated · Kafka-style · built from scratch</span>
      <h1>Messaging that <em>survives</em> the loss of a broker.</h1>
      <p class="lead">Ember is a distributed message broker. Publish and read events from topics, share work in consumer groups, and watch the cluster heal itself, all from one console.</p>
      <div class="cta">${session.user ? `<a class="btn primary lg" href="${home()}">Open console</a>` : `<a class="btn primary lg" href="#/register">Create a free account</a><a class="btn lg" href="#/login">Sign in</a>`}</div>
    </div>
    <div class="terminal" aria-hidden="true"><div class="bar"><i></i><i></i><i></i></div>
<pre><span class="c"># publish an order event</span>
<span class="k">POST</span> /api/topics/orders/messages
{ "key": "order-1042", "value": "paid" }
<span class="g">→ partition 2, offset 8,311</span>

<span class="c"># 3 brokers, 1 goes down…</span>
<span class="k">broker-2</span>  <span style="color:#ff7b7b">● down</span>
<span class="k">orders-1</span>  new leader elected in 214 ms
<span class="g">✓ no acknowledged message lost</span></pre></div>
  </div></section>
  <div class="wrap stats">
    <div class="stat-tile"><b>3×</b><span>replication per partition</span></div>
    <div class="stat-tile"><b>Raft</b><span>consensus for every partition</span></div>
    <div class="stat-tile"><b>0</b><span>acknowledged messages lost in chaos tests</span></div>
    <div class="stat-tile"><b>876</b><span>automated tests</span></div>
  </div>
  <section class="block alt" id="features"><div class="wrap">
    <div class="sec-head"><h2>Everything you need to move events</h2><p>A focused console on top of a broker whose storage engine, protocol and consensus are all written in-house.</p></div>
    <div class="grid3">
      ${[['☰', 'Topics & partitions', 'Split streams into partitions for throughput. See leaders, offsets and message counts at a glance.'],
         ['✉', 'Browse any message', 'Pick a topic, partition and offset, then read records exactly as the broker stored them. Tail a topic live.'],
         ['➤', 'Publish in a click', 'Send keyed messages from the browser. Keys keep related events on the same partition, in order.'],
         ['⚇', 'Consumer groups', 'Watch groups, generations and member assignments so you always know who is working on what.'],
         ['♥', 'Live cluster health', 'Per-broker status, Raft roles, terms and commit indexes, refreshed every few seconds.'],
         ['🔒', 'Role-based access', 'Consumers work with data; administrators also manage topics, users and the cluster.']]
        .map(([i, t, d]) => `<div class="card"><div class="ico">${i}</div><h3>${t}</h3><p>${d}</p></div>`).join('')}
    </div></div></section>
  <section class="block roles" id="roles"><div class="wrap">
    <div class="sec-head"><h2>One console, two ways in</h2><p>Sign in as a consumer to work with data, or as an administrator to run the platform.</p></div>
    <div class="grid2">
      <div class="card"><div class="ico">☺</div><h3>Consumer</h3><p>For developers and services teams who read and write events.</p>
        <ul><li>Browse topics and partitions</li><li>Read messages from any offset</li><li>Publish keyed messages</li><li>Inspect consumer groups</li></ul>
        <a class="btn primary" href="#/register">Create consumer account</a></div>
      <div class="card"><div class="ico">⚙</div><h3>Administrator</h3><p>For the people who keep the cluster healthy.</p>
        <ul><li>Everything a consumer can do</li><li>Live broker and Raft health</li><li>Create topics with custom partitions</li><li>Manage users, roles and access</li></ul>
        <a class="btn" href="#/login?as=admin">Administrator sign in</a></div>
    </div></div></section>
  <section class="block alt" id="how"><div class="wrap">
    <div class="sec-head"><h2>How a message travels</h2><p>The guarantees come from the design, not from luck.</p></div>
    <div class="steps">
      ${[['Publish', 'Your message is routed to its partition leader by key.'], ['Replicate', 'The leader appends it to the Raft log and sends it to two followers.'],
         ['Commit', 'Once a majority has it on disk the message is acknowledged.'], ['Consume', 'Readers fetch committed messages by offset, in order.']]
        .map(([t, d]) => `<div class="card"><h3>${t}</h3><p>${d}</p></div>`).join('')}
    </div></div></section>
  <section class="final"><div class="wrap"><h2>Ready to light it up?</h2><p>Create an account and publish your first message in under a minute.</p>
    <div class="cta" style="justify-content:center">${session.user ? `<a class="btn primary lg" href="${home()}">Open console</a>` : `<a class="btn primary lg" href="#/register">Get started</a><a class="btn lg" href="#/login">Sign in</a>`}</div></div></section>
  <footer class="pub"><div class="wrap"><span>© Ember. Built from scratch in plain Java 21.</span><span>Distributed message broker console</span></div></footer>`;
  app.querySelectorAll('[data-s]').forEach(a => a.addEventListener('click', e => {
    e.preventDefault();
    document.getElementById(a.dataset.s)?.scrollIntoView({ behavior: 'smooth' });
  }));
}

// ---------------------------------------------------------------- auth

function authPage(mode) {
  const params = new URLSearchParams((location.hash.split('?')[1]) || '');
  let as = params.get('as') === 'admin' ? 'ADMIN' : 'CONSUMER';
  const login = mode === 'login';
  app.innerHTML = `<div class="auth-page">
    <aside class="auth-side">
      <a class="brand" href="#/">${LOGO}Ember</a>
      <div><h2>${login ? 'Welcome back.' : 'Start moving events.'}</h2>
      <p>${login ? 'Sign in to publish, browse and operate your topics.' : 'Create a consumer account to publish and read messages across the cluster.'}</p>
      <ul><li>Replicated across three brokers</li><li>Ordered per partition, durable on disk</li><li>Observable down to the Raft term</li></ul></div>
      <span class="hint">Built from scratch in plain Java 21</span>
    </aside>
    <main class="auth-main"><div class="auth-box">
      <h1>${login ? 'Sign in' : 'Create your account'}</h1>
      <p class="sub">${login ? 'Choose how you are signing in.' : 'Consumer accounts are free. Administrators are appointed by an existing admin.'}</p>
      ${login ? `<div class="seg" role="group" aria-label="Sign in as"><button type="button" data-as="CONSUMER">Consumer</button><button type="button" data-as="ADMIN">Administrator</button></div>` : ''}
      <div id="err"></div>
      <form id="form" novalidate>
        ${login ? '' : `<div class="field"><label for="dn">Display name</label><input class="input" id="dn" autocomplete="name" maxlength="60" placeholder="Ada Lovelace"></div>`}
        <div class="field"><label for="un">Username</label><input class="input" id="un" autocomplete="username" required placeholder="ada" maxlength="32"></div>
        <div class="field"><label for="pw">Password</label><input class="input" id="pw" type="password" autocomplete="${login ? 'current-password' : 'new-password'}" required placeholder="${login ? 'Your password' : 'At least 8 characters'}">${login ? '' : '<span class="hint">8 characters or more.</span>'}</div>
        ${login ? '' : `<div class="field"><label for="pw2">Confirm password</label><input class="input" id="pw2" type="password" autocomplete="new-password" required></div>`}
        <button class="btn primary lg" style="width:100%" id="go">${login ? 'Sign in' : 'Create account'}</button>
      </form>
      <p class="switch">${login ? `New here? <a href="#/register">Create an account</a>` : `Already registered? <a href="#/login">Sign in</a>`} · <a href="#/">Back to home</a></p>
    </div></main></div>`;
  const paint = () => app.querySelectorAll('.seg button').forEach(b => b.setAttribute('aria-pressed', String(b.dataset.as === as)));
  app.querySelectorAll('.seg button').forEach(b => b.onclick = () => { as = b.dataset.as; paint(); });
  paint();
  const fail = msg => { $('#err').innerHTML = `<div class="alert" role="alert">${esc(msg)}</div>`; };
  $('#form').onsubmit = async e => {
    e.preventDefault();
    const username = $('#un').value.trim(), password = $('#pw').value;
    if (!username || !password) return fail('Enter your username and password.');
    if (!login) {
      if (password.length < 8) return fail('Password must be at least 8 characters.');
      if (password !== $('#pw2').value) return fail('The two passwords do not match.');
    }
    const btn = $('#go'); btn.disabled = true;
    try {
      const body = login ? { username, password } : { username, password, displayName: $('#dn').value.trim() };
      const r = await api(login ? '/auth/login' : '/auth/register', { method: 'POST', body });
      if (login && as === 'ADMIN' && r.user.role !== 'ADMIN') return fail('This account is not an administrator. Use the Consumer option instead.');
      signIn(r.token, r.user);
      toast('Welcome, ' + r.user.displayName + '!');
      location.hash = home();
    } catch (err) { fail(err.message); } finally { btn.disabled = false; }
  };
  $('#un').focus();
}

// ---------------------------------------------------------------- shared widgets

const head = (title, sub, actions = '') => `<div class="page-head"><div><h1>${title}</h1>${sub ? `<p>${sub}</p>` : ''}</div><div>${actions}</div></div>`;
const empty = (t, d) => `<div class="empty"><b>${t}</b>${d}</div>`;

function modal(title, inner, onSubmit, submitLabel = 'Save') {
  const bg = document.createElement('div');
  bg.className = 'modal-bg';
  bg.innerHTML = `<form class="modal" role="dialog" aria-modal="true" aria-label="${esc(title)}"><h2>${esc(title)}</h2><div id="merr"></div>${inner}
    <div class="actions"><button type="button" class="btn" id="mc">Cancel</button><button class="btn primary" id="ms">${esc(submitLabel)}</button></div></form>`;
  document.body.append(bg);
  const close = () => bg.remove();
  $('#mc', bg).onclick = close;
  bg.addEventListener('mousedown', e => { if (e.target === bg) close(); });
  bg.addEventListener('keydown', e => { if (e.key === 'Escape') close(); });
  $('form', bg).onsubmit = async e => {
    e.preventDefault();
    $('#ms', bg).disabled = true;
    try { await onSubmit(bg); close(); } catch (err) { $('#merr', bg).innerHTML = `<div class="alert">${esc(err.message)}</div>`; $('#ms', bg).disabled = false; }
  };
  bg.querySelector('input,select,textarea')?.focus();
}

const when = ts => new Date(ts).toLocaleString();

// ---------------------------------------------------------------- consumer pages

async function dashboard(view, _a, _p, alive) {
  const [{ topics }, { groups }] = await Promise.all([api('/topics'), api('/groups')]);
  if (!alive()) return;
  const parts = topics.reduce((a, t) => a + t.partitionCount, 0), msgs = topics.reduce((a, t) => a + t.messages, 0);
  view.innerHTML = head(`Hello, ${esc(session.user.displayName)}`, 'Here is what is happening in your cluster.',
    `<a class="btn primary" href="#/app/publish">Publish a message</a>`) + `
    <div class="kpis">
      <div class="kpi"><span>Topics</span><b>${num(topics.length)}</b><small>${num(parts)} partitions</small></div>
      <div class="kpi"><span>Messages stored</span><b>${num(msgs)}</b><small>across all partitions</small></div>
      <div class="kpi"><span>Consumer groups</span><b>${num(groups.length)}</b><small>${num(groups.reduce((a, g) => a + g.members.length, 0))} active members</small></div>
      <div class="kpi"><span>Your role</span><b style="font-size:22px;margin-top:8px">${isAdmin() ? 'Administrator' : 'Consumer'}</b></div>
    </div>
    <div class="panel"><header><h2>Topics</h2><a href="#/app/topics">View all</a></header>
    ${topics.length ? topicTable(topics.slice(0, 6)) : empty('No topics yet', isAdmin() ? 'Create one from <a href="#/admin/topics">Manage topics</a>.' : 'Ask an administrator to create a topic.')}</div>`;
}

function topicTable(topics) {
  return `<div class="table-wrap"><table><thead><tr><th>Topic</th><th class="num">Partitions</th><th class="num">Messages</th><th></th></tr></thead><tbody>
    ${topics.map(t => `<tr><td><a href="#/app/topics/${encodeURIComponent(t.name)}"><b class="mono">${esc(t.name)}</b></a></td><td class="num">${t.partitionCount}</td><td class="num">${num(t.messages)}</td>
      <td><div class="row-actions"><a class="btn sm" href="#/app/messages?topic=${encodeURIComponent(t.name)}">Browse</a><a class="btn sm" href="#/app/publish?topic=${encodeURIComponent(t.name)}">Publish</a></div></td></tr>`).join('')}
    </tbody></table></div>`;
}

async function topicsPage(view, _a, _p, alive) {
  const { topics } = await api('/topics');
  if (!alive()) return;
  view.innerHTML = head('Topics', 'Streams of messages, split into partitions.', isAdmin() ? `<a class="btn primary" href="#/admin/topics">New topic</a>` : '') +
    `<div class="panel">${topics.length ? topicTable(topics) : empty('No topics yet', isAdmin() ? 'Create the first one from Manage topics.' : 'Ask an administrator to create one.')}</div>`;
}

async function topicDetail(view, [name], _p, alive) {
  const { topics } = await api('/topics');
  if (!alive()) return;
  const t = topics.find(x => x.name === name);
  if (!t) { view.innerHTML = head('Topic not found', '') + `<a class="btn" href="#/app/topics">Back to topics</a>`; return; }
  view.innerHTML = head(`<span class="mono">${esc(t.name)}</span>`, `${t.partitionCount} partitions · ${num(t.messages)} messages`,
    `<a class="btn" href="#/app/messages?topic=${encodeURIComponent(name)}">Browse</a> <a class="btn primary" href="#/app/publish?topic=${encodeURIComponent(name)}">Publish</a>`) + `
    <div class="panel"><header><h2>Partitions</h2></header><div class="table-wrap"><table><thead><tr><th>Partition</th><th>Leader</th><th>Replicas</th><th class="num">First offset</th><th class="num">Next offset</th><th class="num">Messages</th><th></th></tr></thead><tbody>
    ${t.partitions.map(p => `<tr><td class="mono">${p.partition}</td><td>${p.leader >= 0 ? `<span class="badge ok"><span class="dot"></span>broker ${p.leader}</span>` : '<span class="badge bad">no leader</span>'}</td>
      <td class="mono">${esc((p.replicas || []).join(', '))}</td><td class="num">${num(p.start)}</td><td class="num">${num(p.end)}</td><td class="num">${num(p.end - p.start)}</td>
      <td><div class="row-actions"><a class="btn sm" href="#/app/messages?topic=${encodeURIComponent(name)}&partition=${p.partition}">Browse</a></div></td></tr>`).join('')}
    </tbody></table></div></div>`;
}

async function messagesPage(view, _a, params, alive) {
  const { topics } = await api('/topics');
  if (!alive()) return;
  if (!topics.length) { view.innerHTML = head('Browse messages', '') + `<div class="panel">${empty('No topics yet', 'There is nothing to read until a topic exists.')}</div>`; return; }
  const want = params.get('topic');
  view.innerHTML = head('Browse messages', 'Read records straight from a partition.') + `
    <div class="panel"><div class="body"><form class="toolbar" id="q">
      <div class="field"><label for="t">Topic</label><select class="input" id="t">${topics.map(t => `<option ${t.name === want ? 'selected' : ''}>${esc(t.name)}</option>`).join('')}</select></div>
      <div class="field"><label for="p">Partition</label><select class="input" id="p"></select></div>
      <div class="field"><label for="o">From offset</label><input class="input" id="o" type="number" min="0" value="0"></div>
      <div class="field"><label for="l">Page size</label><select class="input" id="l"><option>25</option><option selected>50</option><option>100</option><option>200</option></select></div>
      <button class="btn primary" id="load">Load</button><button type="button" class="btn" id="latest">Latest</button>
      <label class="btn" style="font-weight:500"><input type="checkbox" id="tail"> Live tail</label>
    </form></div></div>
    <div class="panel"><header><h2 id="rh">Records</h2><span class="hint" id="meta"></span></header><div id="out"></div><div class="body" id="more" hidden><button class="btn" id="next">Load more</button></div></div>`;
  const sel = id => $('#' + id, view);
  let next = 0;
  const topic = () => topics.find(t => t.name === sel('t').value);
  const fillParts = () => {
    const t = topic();
    const wantP = params.get('partition') ?? String((t.partitions.find(p => p.end > p.start) || t.partitions[0]).partition);   // default: first partition with data
    sel('p').innerHTML = t.partitions.map(p => `<option value="${p.partition}" ${String(p.partition) === wantP ? 'selected' : ''}>${p.partition} (${num(p.end - p.start)} msgs)</option>`).join('');
  };
  const row = r => `<tr><td class="mono">${r.offset}</td><td class="hint" style="white-space:nowrap">${esc(when(r.timestamp))}</td><td class="mono">${r.key == null ? '<span class="hint">null</span>' : esc(r.key)}</td><td><div class="val" title="Click to expand">${r.value == null ? '<span class="hint">null</span>' : esc(r.value)}</div></td></tr>`;
  const fetchPage = async (append) => {
    const offset = append ? next : Number(sel('o').value || 0);
    const r = await api(`/topics/${encodeURIComponent(sel('t').value)}/messages?partition=${sel('p').value}&offset=${offset}&limit=${sel('l').value}`);
    if (!alive()) return r;
    next = r.nextOffset;
    const body = r.records.map(row).join('');
    if (!append) {
      sel('out').innerHTML = r.records.length ? `<div class="table-wrap"><table><thead><tr><th>Offset</th><th>Time</th><th>Key</th><th>Value</th></tr></thead><tbody>${body}</tbody></table></div>`
        : empty('No records here', `Offsets available: ${num(r.logStartOffset)} to ${num(Math.max(0, r.highWatermark - 1))}.`);
    } else if (r.records.length) {
      const tb = $('tbody', sel('out'));
      if (tb) tb.insertAdjacentHTML('beforeend', body);
      else sel('out').innerHTML = `<div class="table-wrap"><table><thead><tr><th>Offset</th><th>Time</th><th>Key</th><th>Value</th></tr></thead><tbody>${body}</tbody></table></div>`;
    }
    sel('meta').textContent = `next offset ${num(next)} · high watermark ${num(r.highWatermark)}`;
    sel('more').hidden = !r.records.length;
    return r;
  };
  const guard = fn => async e => { e?.preventDefault?.(); try { await fn(); } catch (err) { toast(err.message, true); } };
  sel('t').onchange = () => { params.delete('partition'); fillParts(); sel('o').value = 0; };
  fillParts();
  sel('q').onsubmit = guard(() => fetchPage(false));
  sel('next').onclick = guard(() => fetchPage(true));
  sel('latest').onclick = guard(async () => {
    const fresh = (await api('/topics')).topics.find(t => t.name === sel('t').value);
    const p = fresh.partitions.find(x => String(x.partition) === sel('p').value);
    sel('o').value = Math.max(p.start, p.end - Number(sel('l').value));
    await fetchPage(false);
  });
  sel('out').addEventListener('click', e => e.target.closest('.val')?.classList.toggle('open'));
  sel('tail').onchange = () => {
    clearInterval(timer);
    if (!sel('tail').checked) return;
    refreshEvery(2000, alive, async () => { if (sel('out').querySelector('tbody')) await fetchPage(true); else await fetchPage(false); });
  };
  if (want) fetchPage(false).catch(err => toast(err.message, true));
}

async function publishPage(view, _a, params, alive) {
  const { topics } = await api('/topics');
  if (!alive()) return;
  if (!topics.length) { view.innerHTML = head('Publish', '') + `<div class="panel">${empty('No topics yet', 'A topic is needed before you can publish.')}</div>`; return; }
  const want = params.get('topic');
  view.innerHTML = head('Publish a message', 'Messages with the same key always land on the same partition, in order.') + `
    <div class="grid2" style="align-items:start">
    <div class="panel"><div class="body"><form id="f">
      <div class="field"><label for="t">Topic</label><select class="input" id="t">${topics.map(t => `<option ${t.name === want ? 'selected' : ''}>${esc(t.name)}</option>`).join('')}</select></div>
      <div class="field"><label for="k">Key <span class="hint">(optional)</span></label><input class="input" id="k" placeholder="order-1042" maxlength="256"></div>
      <div class="field"><label for="v">Value</label><textarea class="input" id="v" required placeholder='{"status":"paid"}'></textarea></div>
      <button class="btn primary" id="send">Send message</button>
    </form></div></div>
    <div class="panel"><header><h2>Sent this session</h2></header><div id="sent">${empty('Nothing sent yet', 'Acknowledged messages appear here.')}</div></div></div>`;
  const sent = [];
  $('#f', view).onsubmit = async e => {
    e.preventDefault();
    const value = $('#v', view).value;
    if (!value.trim()) return toast('Write a message value first.', true);
    $('#send', view).disabled = true;
    try {
      const r = await api(`/topics/${encodeURIComponent($('#t', view).value)}/messages`, { method: 'POST', body: { key: $('#k', view).value, value } });
      sent.unshift(r);
      $('#sent', view).innerHTML = `<div class="table-wrap"><table><thead><tr><th>Topic</th><th class="num">Partition</th><th class="num">Offset</th></tr></thead><tbody>${sent.slice(0, 15).map(s =>
        `<tr><td class="mono">${esc(s.topic)}</td><td class="num">${s.partition}</td><td class="num">${num(s.offset)}</td></tr>`).join('')}</tbody></table></div>`;
      toast(`Acknowledged: partition ${r.partition}, offset ${r.offset}`);
      $('#v', view).value = '';
    } catch (err) { toast(err.message, true); } finally { $('#send', view).disabled = false; }
  };
}

async function groupsPage(view, _a, _p, alive) {
  const { groups } = await api('/groups');
  if (!alive()) return;
  view.innerHTML = head('Consumer groups', 'Consumers that share the work of reading topics.') + `<div class="panel">${groups.length ? `<div class="table-wrap"><table>
    <thead><tr><th>Group</th><th class="num">Generation</th><th class="num">Members</th><th class="num">Committed partitions</th><th>Clients</th></tr></thead><tbody>
    ${groups.map(g => `<tr><td class="mono"><b>${esc(g.group)}</b></td><td class="num">${g.generation}</td><td class="num">${g.members.length}</td><td class="num">${g.committedPartitions}</td>
      <td>${g.members.map(m => `<span class="badge">${esc(m.clientId)} · ${m.assignedPartitions}p</span>`).join(' ') || '<span class="hint">no active members</span>'}</td></tr>`).join('')}
    </tbody></table></div>` : empty('No consumer groups', 'Groups appear once a consumer joins with a group id.')}</div>`;
}

// ---------------------------------------------------------------- admin pages

async function adminOverview(view, _a, _p, alive) {
  const paint = async () => {
    const { nodes } = await api('/admin/cluster');
    if (!alive()) return;
    const up = nodes.filter(n => n.up), ready = up.filter(n => n.status.ready);
    const ref = up[0]?.status;
    const groups = new Map();            // raft group name -> {leader, term, roles per node}
    up.forEach(n => (n.status.raftGroups || []).forEach(g => {
      const e = groups.get(g.group) || { term: 0, leader: -1, commit: 0, members: [] };
      e.term = Math.max(e.term, g.term); e.commit = Math.max(e.commit, g.commitIndex);
      if (g.role === 'LEADER') e.leader = n.status.nodeId;
      e.members.push(n.status.nodeId);
      groups.set(g.group, e);
    }));
    const quorum = Math.floor(nodes.length / 2) + 1;
    const state = up.length === nodes.length && ready.length === nodes.length ? ['ok', 'Healthy'] : up.length >= quorum ? ['warn', 'Degraded (quorum holds)'] : ['bad', 'No quorum'];
    view.innerHTML = head('Cluster health', 'Live status from every broker. Refreshes every 5 seconds.', `<span class="badge ${state[0]}"><span class="dot"></span>${state[1]}</span>`) + `
      <div class="kpis">
        <div class="kpi"><span>Brokers up</span><b>${up.length} / ${nodes.length}</b><small>quorum needs ${quorum}</small></div>
        <div class="kpi"><span>Ready</span><b>${ready.length} / ${nodes.length}</b><small>joined and caught up</small></div>
        <div class="kpi"><span>Controller</span><b>${ref && ref.controllerId >= 0 ? 'broker ' + ref.controllerId : '—'}</b><small>creates topics</small></div>
        <div class="kpi"><span>Group coordinator</span><b>${ref && ref.coordinatorId >= 0 ? 'broker ' + ref.coordinatorId : '—'}</b><small>consumer groups</small></div>
      </div>
      <div class="nodes">${nodes.map(n => {
        const s = n.status;
        const leads = s ? (s.raftGroups || []).filter(g => g.role === 'LEADER').length : 0;
        return `<div class="node ${n.up ? '' : 'down'}"><h3><span>${s ? 'Broker ' + s.nodeId : esc(n.admin)}</span>
          <span class="badge ${!n.up ? 'bad' : s.ready ? 'ok' : 'warn'}"><span class="dot"></span>${!n.up ? 'down' : s.ready ? 'ready' : 'starting'}</span></h3>
          ${n.up ? `<dl><dt>Admin</dt><dd class="mono">${esc(n.admin)}</dd><dt>Client port</dt><dd>${s.port}</dd><dt>Uptime</dt><dd>${uptime(s.uptimeSeconds)}</dd>
          <dt>Raft groups led</dt><dd>${leads} of ${(s.raftGroups || []).length}</dd><dt>Peers connected</dt><dd>${(s.peers || []).filter(p => p.connected).length} of ${(s.peers || []).length}</dd></dl>`
          : `<dl><dt>Admin</dt><dd class="mono">${esc(n.admin)}</dd><dt>Error</dt><dd>${esc(n.error || 'unreachable')}</dd></dl>`}</div>`;
      }).join('')}</div>
      <div class="panel"><header><h2>Raft groups</h2><span class="hint">${groups.size} groups</span></header>
      ${groups.size ? `<div class="table-wrap"><table><thead><tr><th>Group</th><th>Leader</th><th class="num">Term</th><th class="num">Commit index</th><th>Seen on</th></tr></thead><tbody>
        ${[...groups].sort((a, b) => a[0].localeCompare(b[0])).map(([name, g]) => `<tr><td class="mono">${esc(name)}</td>
        <td>${g.leader >= 0 ? `<span class="badge ok">broker ${g.leader}</span>` : '<span class="badge bad">none</span>'}</td><td class="num">${g.term}</td><td class="num">${num(g.commit)}</td>
        <td class="mono">${g.members.join(', ')}</td></tr>`).join('')}</tbody></table></div>` : empty('No data', 'No broker is reachable.')}</div>`;
  };
  await paint();
  refreshEvery(5000, alive, paint);
}
const uptime = s => { s = Math.floor(s); const d = Math.floor(s / 86400), h = Math.floor(s % 86400 / 3600), m = Math.floor(s % 3600 / 60); return d ? `${d}d ${h}h` : h ? `${h}h ${m}m` : m ? `${m}m ${s % 60}s` : `${s}s`; };

async function adminTopics(view, _a, _p, alive) {
  const paint = async () => {
    const { topics } = await api('/topics');
    if (!alive()) return;
    view.innerHTML = head('Manage topics', 'Create topics and check how their partitions are led.', `<button class="btn primary" id="new">New topic</button>`) +
      `<div class="panel">${topics.length ? topicTable(topics) : empty('No topics yet', 'Click “New topic” to create the first one.')}</div>`;
    $('#new', view).onclick = () => modal('New topic', `
      <div class="field"><label for="n">Name</label><input class="input" id="n" required maxlength="128" placeholder="orders" pattern="[a-zA-Z0-9._\-]+"><span class="hint">Letters, digits, dot, underscore, dash.</span></div>
      <div class="field"><label for="pc">Partitions</label><input class="input" id="pc" type="number" min="1" max="64" value="3"><span class="hint">More partitions allow more parallel consumers.</span></div>`,
      async bg => {
        const name = $('#n', bg).value.trim();
        await api('/admin/topics', { method: 'POST', body: { name, partitions: Number($('#pc', bg).value) } });
        toast(`Topic “${name}” created`);
        await paint();
      }, 'Create topic');
  };
  await paint();
}

async function adminUsers(view, _a, _p, alive) {
  const paint = async () => {
    const { users } = await api('/admin/users');
    if (!alive()) return;
    const me = session.user.username.toLowerCase();
    view.innerHTML = head('Users', 'Promote consumers to administrators, or suspend access.') + `<div class="panel"><div class="table-wrap"><table>
      <thead><tr><th>User</th><th>Role</th><th>Status</th><th>Joined</th><th></th></tr></thead><tbody>
      ${users.map(u => { const self = u.username.toLowerCase() === me; return `<tr><td><b>${esc(u.displayName)}</b><div class="hint mono">${esc(u.username)}${self ? ' (you)' : ''}</div></td>
        <td><span class="badge ${u.role === 'ADMIN' ? 'acc' : ''}">${u.role === 'ADMIN' ? 'Administrator' : 'Consumer'}</span></td>
        <td><span class="badge ${u.disabled ? 'bad' : 'ok'}"><span class="dot"></span>${u.disabled ? 'Disabled' : 'Active'}</span></td><td class="hint">${esc(new Date(u.createdAt).toLocaleDateString())}</td>
        <td><div class="row-actions">${self ? '<span class="hint">—</span>' : `
          <button class="btn sm" data-a="role" data-u="${esc(u.username)}" data-v="${u.role === 'ADMIN' ? 'CONSUMER' : 'ADMIN'}">${u.role === 'ADMIN' ? 'Make consumer' : 'Make admin'}</button>
          <button class="btn sm" data-a="toggle" data-u="${esc(u.username)}" data-v="${!u.disabled}">${u.disabled ? 'Enable' : 'Disable'}</button>
          <button class="btn sm danger" data-a="delete" data-u="${esc(u.username)}">Delete</button>`}</div></td></tr>`; }).join('')}
      </tbody></table></div></div>`;
  };
  await paint();
  view.addEventListener('click', async e => {
    const b = e.target.closest('button[data-a]');
    if (!b) return;
    const user = b.dataset.u, url = '/admin/users/' + encodeURIComponent(user);
    try {
      if (b.dataset.a === 'delete') {
        if (!confirm(`Delete ${user}? This cannot be undone.`)) return;
        await api(url, { method: 'DELETE' });
      } else if (b.dataset.a === 'role') {
        await api(url, { method: 'PATCH', body: { role: b.dataset.v } });
      } else {
        await api(url, { method: 'PATCH', body: { disabled: b.dataset.v === 'true' } });
      }
      toast('Updated ' + user);
      await paint();
    } catch (err) { toast(err.message, true); }
  });
}

const splitList = s => s.split(/[\s,]+/).filter(Boolean);

function showKey(app, key, title) {
  const bg = document.createElement('div');
  bg.className = 'modal-bg';
  const base = location.origin;
  const sample = `curl -X POST ${base}/api/app/topics/TOPIC/messages \\\n  -H "Authorization: Bearer ${key}" \\\n  -d '{"key":"k1","value":"hello"}'`;
  bg.innerHTML = `<div class="modal" role="dialog" aria-modal="true" style="width:min(560px,100%)"><h2>${esc(title)}</h2>
    <p class="hint" style="margin:0 0 12px">Copy this key now. It is stored only as a hash, so it cannot be shown again. <b>${esc(app.name)}</b> uses it to sign in.</p>
    <div class="field"><label>API key</label><div style="display:flex;gap:8px"><input class="input mono" id="k" readonly value="${esc(key)}"><button class="btn" id="cp">Copy</button></div></div>
    <div class="field"><label>Try it</label><pre class="input mono" style="margin:0;overflow-x:auto;font-size:12px">${esc(sample)}</pre></div>
    <div class="actions"><button class="btn primary" id="done">I have saved the key</button></div></div>`;
  document.body.append(bg);
  $('#k', bg).select();
  $('#cp', bg).onclick = async () => { try { await navigator.clipboard.writeText(key); toast('Key copied'); } catch { $('#k', bg).select(); toast('Press Ctrl/Cmd+C to copy'); } };
  $('#done', bg).onclick = () => bg.remove();
}

async function adminApps(view, _a, _p, alive) {
  const paint = async () => {
    const { apps } = await api('/admin/apps');
    if (!alive()) return;
    const pills = l => l.length ? l.map(t => `<span class="badge mono">${esc(t)}</span>`).join(' ') : '<span class="hint">none</span>';
    view.innerHTML = head('Apps', 'Services that connect to Ember with an API key. Each one can only touch the topics you allow.', `<button class="btn primary" id="new">Register app</button>`) +
      `<div class="panel">${apps.length ? `<div class="table-wrap"><table><thead><tr><th>App</th><th>Can produce to</th><th>Can consume from</th><th>Key</th><th>Activity</th><th></th></tr></thead><tbody>
      ${apps.map(a => `<tr><td><b>${esc(a.name)}</b> ${a.disabled ? '<span class="badge bad">disabled</span>' : ''}<div class="hint">${esc(a.description)}</div><div class="hint mono">${esc(a.id)}</div></td>
        <td>${pills(a.produceTopics)}</td><td>${pills(a.consumeTopics)}</td><td class="mono">${esc(a.keyPrefix)}…</td>
        <td class="hint">${num(a.produced)} sent<br>${num(a.fetched)} read · ${num(a.acked)} acked<br>${a.lastSeen ? 'seen ' + esc(new Date(a.lastSeen).toLocaleTimeString()) : 'never connected'}</td>
        <td><div class="row-actions"><button class="btn sm" data-a="edit" data-i="${esc(a.id)}">Edit</button><button class="btn sm" data-a="rotate" data-i="${esc(a.id)}">New key</button>
        <button class="btn sm" data-a="toggle" data-i="${esc(a.id)}" data-v="${!a.disabled}">${a.disabled ? 'Enable' : 'Disable'}</button><button class="btn sm danger" data-a="delete" data-i="${esc(a.id)}">Delete</button></div></td></tr>`).join('')}
      </tbody></table></div>` : empty('No apps registered', 'Register your first service to get an API key.')}</div>`;
    view._apps = apps;
    $('#new', view).onclick = () => modal('Register app', `
      <div class="field"><label for="an">Name</label><input class="input" id="an" required maxlength="48" placeholder="matcher"></div>
      <div class="field"><label for="ad">Description <span class="hint">(optional)</span></label><input class="input" id="ad" maxlength="200" placeholder="Pairs riders with nearby drivers"></div>
      <div class="field"><label for="ap">Can produce to</label><input class="input mono" id="ap" placeholder="ride-events, payments.*"><span class="hint">Topic names, a prefix like <code>orders.*</code>, or <code>*</code> for all. Separate with commas.</span></div>
      <div class="field"><label for="ac">Can consume from</label><input class="input mono" id="ac" placeholder="ride-requests"></div>`,
      async bg => {
        const r = await api('/admin/apps', { method: 'POST', body: { name: $('#an', bg).value, description: $('#ad', bg).value,
          produceTopics: splitList($('#ap', bg).value), consumeTopics: splitList($('#ac', bg).value) } });
        await paint();
        showKey(r.app, r.apiKey, 'App registered');
      }, 'Register');
  };
  await paint();
  view.addEventListener('click', async e => {
    const b = e.target.closest('button[data-a]');
    if (!b) return;
    const app = (view._apps || []).find(x => x.id === b.dataset.i);
    if (!app) return;
    const url = '/admin/apps/' + encodeURIComponent(app.id);
    try {
      if (b.dataset.a === 'delete') {
        if (!confirm(`Delete ${app.name}? Its key stops working immediately.`)) return;
        await api(url, { method: 'DELETE' });
      } else if (b.dataset.a === 'toggle') {
        await api(url, { method: 'PATCH', body: { disabled: b.dataset.v === 'true' } });
      } else if (b.dataset.a === 'rotate') {
        if (!confirm(`Issue a new key for ${app.name}? The old key stops working immediately.`)) return;
        const r = await api(url + '/rotate-key', { method: 'POST' });
        await paint();
        return showKey(r.app, r.apiKey, 'New key issued');
      } else {
        return modal('Edit ' + app.name, `
          <div class="field"><label for="ad">Description</label><input class="input" id="ad" maxlength="200" value="${esc(app.description)}"></div>
          <div class="field"><label for="ap">Can produce to</label><input class="input mono" id="ap" value="${esc(app.produceTopics.join(', '))}"></div>
          <div class="field"><label for="ac">Can consume from</label><input class="input mono" id="ac" value="${esc(app.consumeTopics.join(', '))}"></div>`,
          async bg => {
            await api(url, { method: 'PATCH', body: { description: $('#ad', bg).value, produceTopics: splitList($('#ap', bg).value), consumeTopics: splitList($('#ac', bg).value) } });
            toast('Saved ' + app.name);
            await paint();
          });
      }
      toast('Updated ' + app.name);
      await paint();
    } catch (err) { toast(err.message, true); }
  });
}

function queueTotals(q) {
  const t = { inFlight: 0, redeliver: 0, dead: 0, waiting: 0, finished: 0 };
  q.partitions.forEach(p => { t.inFlight += p.inFlight; t.redeliver += p.redeliver; t.dead += p.dead; t.waiting += Math.max(0, p.logEnd - p.next); t.finished += p.floor + p.doneAbove; });
  return t;
}

async function adminQueues(view, _a, _p, alive) {
  const paint = async () => {
    const { queues } = await api('/admin/queues');
    if (!alive()) return;
    view.innerHTML = head('Queues', 'Per-message acknowledgement. A failing message is retried with a growing delay, then set aside as a dead letter so it never blocks the others.',
      `<button class="btn primary" id="new">New queue</button>`) +
      `<div class="panel">${queues.length ? `<div class="table-wrap"><table><thead><tr><th>Queue</th><th>Topic</th><th class="num">Waiting</th><th class="num">In flight</th><th class="num">Retrying</th><th class="num">Dead</th><th>Policy</th><th></th></tr></thead><tbody>
      ${queues.map(q => { const t = queueTotals(q); return `<tr><td><b class="mono">${esc(q.subscription)}</b></td><td class="mono">${esc(q.topic)}</td>
        <td class="num">${num(t.waiting)}</td><td class="num">${num(t.inFlight)}</td><td class="num">${num(t.redeliver)}</td>
        <td class="num">${t.dead ? `<span class="badge bad">${num(t.dead)}</span>` : '<span class="hint">0</span>'}</td>
        <td class="hint">${q.maxAttempts} attempts · wait ${esc(secs(q.backoffBaseMs))}–${esc(secs(q.backoffMaxMs))} · lease ${esc(secs(q.leaseMs))}</td>
        <td><div class="row-actions"><a class="btn sm" href="#/admin/queues/${encodeURIComponent(q.subscription)}">Dead letters</a></div></td></tr>`; }).join('')}
      </tbody></table></div>` : empty('No queues yet', 'Create one on a topic to start tracking each message separately.')}</div>`;
    $('#new', view).onclick = async () => {
      const { topics } = await api('/topics');
      if (!topics.length) return toast('Create a topic first.', true);
      modal('New queue', `
        <div class="field"><label for="qn">Name</label><input class="input" id="qn" required maxlength="128" placeholder="matcher" pattern="[a-zA-Z0-9._\\-]+"></div>
        <div class="field"><label for="qt">Topic</label><select class="input" id="qt">${topics.map(t => `<option>${esc(t.name)}</option>`).join('')}</select></div>
        <div class="field"><label for="qs">Start from</label><select class="input" id="qs"><option value="1">The beginning of the topic</option><option value="0">Only new messages</option></select></div>
        <div class="toolbar"><div class="field"><label for="qa">Attempts before dead</label><input class="input" id="qa" type="number" min="1" max="100" value="5"></div>
        <div class="field"><label for="qb">First retry wait (s)</label><input class="input" id="qb" type="number" min="0" step="0.1" value="1"></div>
        <div class="field"><label for="qm">Longest wait (s)</label><input class="input" id="qm" type="number" min="0" step="1" value="60"></div></div>
        <div class="field"><label for="ql">Lease (s)</label><input class="input" id="ql" type="number" min="1" value="30"><span class="hint">How long a worker may hold a message before it is given to someone else.</span></div>`,
        async bg => {
          const name = $('#qn', bg).value.trim();
          await api('/admin/queues', { method: 'POST', body: { name, topic: $('#qt', bg).value, fromStart: $('#qs', bg).value === '1',
            maxAttempts: Number($('#qa', bg).value), backoffMs: Math.round(Number($('#qb', bg).value) * 1000), backoffMaxMs: Math.round(Number($('#qm', bg).value) * 1000),
            leaseMs: Math.round(Number($('#ql', bg).value) * 1000) } });
          toast(`Queue “${name}” created`);
          await paint();
        }, 'Create queue');
    };
  };
  await paint();
  refreshEvery(5000, alive, paint);
}
const secs = ms => ms >= 1000 ? (ms / 1000).toFixed(ms % 1000 ? 1 : 0) + ' s' : ms + ' ms';

async function adminDead(view, [queue], _p, alive) {
  const paint = async () => {
    const [{ total, items }, { queues }] = await Promise.all([api('/admin/queues/' + encodeURIComponent(queue) + '/dead?max=200'), api('/admin/queues')]);
    if (!alive()) return;
    const q = queues.find(x => x.subscription === queue);
    view.innerHTML = head(`Dead letters · <span class="mono">${esc(queue)}</span>`,
      q ? `Messages from <b class="mono">${esc(q.topic)}</b> that failed ${q.maxAttempts} times. Fix the cause, then retry them; or discard what is not worth keeping.` : '',
      `<a class="btn" href="#/admin/queues">All queues</a>`) +
      `<div class="panel"><header><h2>${num(total)} dead message${total === 1 ? '' : 's'}${total > items.length ? ` (showing ${items.length})` : ''}</h2>
      <div class="row-actions">${items.length ? `<button class="btn sm" data-a="retry-all">Retry all shown</button><button class="btn sm danger" data-a="discard-all">Discard all shown</button>` : ''}</div></header>
      ${items.length ? `<div class="table-wrap"><table><thead><tr><th>Where</th><th>Why it failed</th><th class="num">Failures</th><th>When</th><th>Message</th><th></th></tr></thead><tbody>
      ${items.map((m, i) => `<tr><td class="mono">p${m.partition} · #${num(m.offset)}</td><td>${esc(m.reason || '—')}</td><td class="num">${m.failures}</td><td class="hint">${esc(new Date(m.deadAt).toLocaleString())}</td>
        <td>${m.key != null ? `<div class="hint mono">key ${esc(m.key)}</div>` : ''}<div class="val" title="Click to expand">${m.value ? esc(m.value) : '<span class="hint">content no longer in the log</span>'}</div></td>
        <td><div class="row-actions"><button class="btn sm" data-a="retry" data-i="${i}">Retry</button><button class="btn sm danger" data-a="discard" data-i="${i}">Discard</button></div></td></tr>`).join('')}
      </tbody></table></div>` : empty('Nothing here', 'No message has used up its attempts. 🎉')}</div>`;
    view._items = items;
  };
  await paint();
  refreshEvery(5000, alive, paint);
  view.addEventListener('click', async e => {
    const val = e.target.closest('.val');
    if (val) return val.classList.toggle('open');
    const b = e.target.closest('button[data-a]');
    if (!b) return;
    const all = view._items || [];
    const chosen = b.dataset.a.endsWith('-all') ? all : [all[Number(b.dataset.i)]].filter(Boolean);
    const action = b.dataset.a.startsWith('retry') ? 'retry' : 'discard';
    if (!chosen.length) return;
    if (action === 'discard' && !confirm(`Discard ${chosen.length} message${chosen.length > 1 ? 's' : ''} for good? They will never be delivered.`)) return;
    try {
      const r = await api('/admin/queues/' + encodeURIComponent(queue) + '/dead/' + action, { method: 'POST', body: { items: chosen.map(m => ({ partition: m.partition, offset: m.offset })) } });
      toast(`${action === 'retry' ? 'Re-queued' : 'Discarded'} ${r.settled} message${r.settled === 1 ? '' : 's'}`);
      await paint();
    } catch (err) { toast(err.message, true); }
  });
}

// ---------------------------------------------------------------- Live Fleet

const fmt = n => Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 1 });

async function adminFleet(view, _a, _p, alive) {
  let state = null, cmp = null, cmpTimer = 0;
  const q = id => $('#' + id, view);

  const notRunning = msg => {
    view.innerHTML = head('Live Fleet', 'A ride-dispatch system running on Ember, with thousands of simulated drivers.') +
      `<div class="panel"><div class="body"><div class="empty"><b>${esc(msg || 'The Fleet demo is not running')}</b>
      Start it next to the console, then this page fills in on its own:<br><br>
      <code>cd demo/fleet &amp;&amp; node bin/setup.js &amp;&amp; node bin/live.js --drivers 2000</code></div></div></div>`;
    state = null;
  };

  const skeleton = () => {
    view.innerHTML = head('Live Fleet', 'Simulated drivers and riders, a dispatcher, billing against a flaky bank. Break things and watch Ember cope.',
      `<span class="badge ok" id="f-status"><span class="dot"></span>running</span>`) + `
      <div class="kpis" id="f-kpis"></div>
      <div class="fleet-grid">
        <div class="panel"><header><h2>Live map</h2><span class="hint"><span class="legend idle"></span> idle <span class="legend busy"></span> on a trip · <span id="f-dots"></span></span></header>
          <div class="body" style="padding:10px"><canvas id="f-map" class="fleet-map" aria-label="Map of simulated drivers"></canvas></div></div>
        <div>
          <div class="panel"><header><h2>Pipeline</h2><a href="#/admin/queues">All queues</a></header><div class="table-wrap"><table id="f-queues"></table></div></div>
          <div class="panel"><header><h2>Break things</h2><span class="badge" id="f-bank"></span></header><div class="body">
            <div class="fleet-actions">
              <button class="btn" id="f-bankbtn"></button>
              <button class="btn" data-crash="billing">Crash billing (10 s)</button>
              <button class="btn" data-crash="matcher">Crash matcher (10 s)</button>
              <button class="btn" data-crash="notifier">Crash notifier (10 s)</button>
            </div>
            <div class="field" style="margin:14px 0 0"><label for="f-riders">Ride requests per second: <b id="f-riders-v"></b></label><input type="range" id="f-riders" min="0" max="60" step="1"></div>
            <p class="hint" style="margin:12px 0 0">Try it: take the bank down, watch billing's <i>retrying</i> climb, then dead letters appear. Bring the bank back and use <a href="#/admin/queues/billing">Dead letters</a> to retry them all: no ride is charged twice.</p>
          </div></div>
        </div>
      </div>
      <div class="panel"><header><h2>Why per-message tracking matters</h2><div class="row-actions"><span class="hint" id="f-cmp-note"></span><button class="btn sm primary" id="f-cmp-run">Run it now (about a minute)</button></div></header>
        <div class="body" id="f-cmp"></div></div>`;
    q('f-bankbtn').onclick = () => control({ action: 'bank', down: !(state && state.bank.down) });
    view.querySelectorAll('[data-crash]').forEach(b => b.onclick = () => control({ action: 'crash', service: b.dataset.crash, seconds: 10 }));
    q('f-riders').onchange = e => control({ action: 'riders', perSec: Number(e.target.value) });
    q('f-riders').oninput = e => { q('f-riders-v').textContent = e.target.value; };
    q('f-cmp-run').onclick = async () => {
      try { await api('/admin/fleet/compare/run', { method: 'POST', body: {} }); toast('Comparison started: about a minute'); pollCompare(); } catch (e) { toast(e.message, true); }
    };
  };

  const control = async body => {
    try { await api('/admin/fleet/control', { method: 'POST', body }); await tick(); } catch (e) { toast(e.message, true); }
  };

  const kpi = (label, value, sub) => `<div class="kpi"><span>${label}</span><b>${value}</b><small>${sub || '&nbsp;'}</small></div>`;

  const paintMap = () => {
    const c = q('f-map');
    if (!c || !state) return;
    const w = c.clientWidth, h = Math.round(w * 0.72), dpr = window.devicePixelRatio || 1;
    c.width = w * dpr; c.height = h * dpr; c.style.height = h + 'px';
    const g = c.getContext('2d');
    g.scale(dpr, dpr);
    const css = getComputedStyle(document.documentElement);
    g.fillStyle = css.getPropertyValue('--bg-2').trim() || '#eee'; g.fillRect(0, 0, w, h);
    g.strokeStyle = css.getPropertyValue('--line').trim() || '#ddd'; g.lineWidth = 1;
    for (let i = 1; i < 8; i++) { g.beginPath(); g.moveTo(w * i / 8, 0); g.lineTo(w * i / 8, h); g.moveTo(0, h * i / 8); g.lineTo(w, h * i / 8); g.stroke(); }
    const b = state.city;
    const idle = '#7d8aa3', busy = css.getPropertyValue('--accent').trim() || '#e8590c';
    for (const pass of [0, 1]) {                                   // idle dots first, trips on top
      g.fillStyle = pass ? busy : idle;
      for (const [lat, lng, isBusy] of state.drivers) {
        if (isBusy !== pass) continue;
        const x = (lng - b.minLng) / (b.maxLng - b.minLng) * w, y = (1 - (lat - b.minLat) / (b.maxLat - b.minLat)) * h;
        g.globalAlpha = pass ? 1 : 0.75;
        g.beginPath(); g.arc(x, y, pass ? 3.2 : 2, 0, 6.2832); g.fill();
      }
    }
    g.globalAlpha = 1;
  };

  const paint = () => {
    const s = state;
    const down = n => !s.services[n];
    q('f-status').className = 'badge ' + (s.bank.down ? 'warn' : 'ok');
    q('f-status').innerHTML = `<span class="dot"></span>${s.bank.down ? 'bank outage' : 'all healthy'} · up ${esc(uptime(s.uptimeSec))}`;
    q('f-kpis').innerHTML = kpi('Drivers', fmt(s.config.drivers), `${fmt(s.trackedDrivers)} tracked`) + kpi('GPS pings / s', fmt(s.rates.pings), `${fmt(s.totals.pings)} total`) +
      kpi('Rides matched / s', fmt(s.rates.matched), `${fmt(s.totals.matched)} matched · ${fmt(s.totals.unmatched)} unmatched`) +
      kpi('Request → match', s.latency.count ? `${fmt(s.latency.p50)} ms` : '—', s.latency.count ? `p99 ${fmt(s.latency.p99)} ms` : 'waiting for rides') +
      kpi('Rides charged', fmt(s.totals.charged), `${fmt(s.totals.completed)} completed`);
    q('f-dots').textContent = `${fmt(s.drivers.length)} of ${fmt(s.config.drivers)} drivers shown`;
    const rows = ['tracker', 'matcher', 'sim-events', 'billing', 'notifier'].map(n => {
      const x = s.queues[n], svc = s.services[n];
      return `<tr><td class="mono">${n}${svc === false ? ' <span class="badge bad">crashed</span>' : ''}</td>${x ? `<td class="num">${fmt(x.waiting)}</td><td class="num">${fmt(x.inFlight)}</td>
        <td class="num">${x.retrying ? `<span class="badge warn">${fmt(x.retrying)}</span>` : '0'}</td><td class="num">${x.dead ? `<span class="badge bad">${fmt(x.dead)}</span>` : '0'}</td>` : '<td colspan="4" class="hint">no data</td>'}</tr>`;
    }).join('');
    q('f-queues').innerHTML = `<thead><tr><th>Queue</th><th class="num">Waiting</th><th class="num">In flight</th><th class="num">Retrying</th><th class="num">Dead</th></tr></thead><tbody>${rows}</tbody>`;
    q('f-bank').className = 'badge ' + (s.bank.down ? 'bad' : 'ok');
    q('f-bank').textContent = s.bank.down ? 'bank DOWN' : 'bank up';
    q('f-bankbtn').textContent = s.bank.down ? 'Bring the bank back' : 'Take the bank down';
    q('f-bankbtn').className = 'btn ' + (s.bank.down ? 'primary' : 'danger');
    view.querySelectorAll('[data-crash]').forEach(b => { b.disabled = down(b.dataset.crash); });
    if (document.activeElement !== q('f-riders')) { q('f-riders').value = s.config.ridersPerSec; q('f-riders-v').textContent = s.config.ridersPerSec; }
    paintMap();
  };

  const paintCompare = () => {
    const el = q('f-cmp');
    if (!el) return;
    const r = cmp && cmp.result;
    q('f-cmp-run').disabled = !cmp || !cmp.available || cmp.running;
    q('f-cmp-run').textContent = cmp && cmp.running ? 'Running… (about a minute)' : 'Run it now (about a minute)';
    q('f-cmp-note').textContent = cmp && !cmp.available ? 'to run it here, start live.js with EMBER_CLIENT_JAR set' : (r && r.sample ? 'showing a saved result' : r ? 'result from this machine' : '');
    if (!r) { el.innerHTML = `<div class="empty"><b>No comparison result yet</b>Run it to see the same poisoned workload consumed four ways.</div>`; return; }
    const poison = r.rows[0].poisonTotal;
    el.innerHTML = `<p style="margin:0 0 14px;color:var(--muted)">The same topic: ${fmt(r.records)} messages on ${r.partitions} partitions, every ${r.poisonEvery}th one is <b>poison</b> (${fmt(poison)} messages that always fail), ${r.workMs} ms of work per message.
      A consumer group tracks one position per partition; an ack queue tracks every message.</p>` +
      r.rows.map(x => {
        const share = Math.round(100 * x.healthyProcessed / x.healthyTotal), good = share >= 100;
        return `<div class="cmp-row"><div class="cmp-label"><b>${esc(x.label)}</b><span class="hint">${x.workers} workers</span></div>
          <div class="cmp-bar"><div class="cmp-fill ${good ? 'ok' : 'bad'}" style="width:${share}%"></div><span>${fmt(x.healthyProcessed)} / ${fmt(x.healthyTotal)} healthy messages done</span></div>
          <div class="cmp-time">${x.finished ? (x.elapsedMs / 1000).toFixed(1) + ' s' : `<span class="badge bad">stuck after ${(x.elapsedMs / 1000).toFixed(0)} s</span>`}</div>
          <div class="cmp-out hint">${esc(x.poisonOutcome)}</div></div>`;
      }).join('') +
      `<p class="hint" style="margin:12px 0 0">Teams that need retries and a dead-letter queue on a consumer group build them by hand (extra topics and consumers). Here they are part of the queue.</p>`;
  };

  const tick = async () => {
    try {
      const s = await api('/admin/fleet/state');
      if (!alive()) return;
      if (!state) skeleton();
      state = s;
      paint();
    } catch (e) {
      if (!alive() || e.message === 'signed out') return;
      notRunning(e.message);
    }
  };
  const pollCompare = async () => {
    try { cmp = await api('/admin/fleet/compare'); } catch { cmp = null; }
    if (!alive() || !q('f-cmp')) return;
    paintCompare();
    clearTimeout(cmpTimer);
    if (cmp && cmp.running) cmpTimer = setTimeout(pollCompare, 3000);
  };

  await tick();
  if (state) await pollCompare();
  refreshEvery(1000, alive, async () => { const wasDown = !state; await tick(); if (wasDown && state) pollCompare(); });
}

navigate();
