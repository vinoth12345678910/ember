'use strict';
const { baseUrl } = require('./config');

/** The console's admin API, for setup and for the scenario's "operator" actions. */
class Admin {
  constructor(user = process.env.EMBER_ADMIN_USER, password = process.env.EMBER_ADMIN_PASSWORD) {
    if (!user || !password) throw new Error('set EMBER_ADMIN_USER and EMBER_ADMIN_PASSWORD (an Ember console administrator)');
    this.user = user;
    this.password = password;
    this.token = null;
  }

  async call(method, path, body, okStatuses = []) {
    if (!this.token && !path.startsWith('/api/auth/')) {
      this.token = (await this.call('POST', '/api/auth/login', { username: this.user, password: this.password })).token;
    }
    const res = await fetch(baseUrl() + path, {
      method,
      headers: { 'Content-Type': 'application/json', ...(this.token ? { Authorization: 'Bearer ' + this.token } : {}) },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok && !okStatuses.includes(res.status)) throw new Error(`${method} ${path} -> ${res.status} ${data.error || ''}`);
    return { ...data, _status: res.status };
  }
}

module.exports = { Admin };
