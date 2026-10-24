'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');

const RESULT = path.join(__dirname, '..', 'results', 'compare.json');
const SAMPLE = path.join(__dirname, '..', 'results', 'compare-sample.json');

/**
 * Runs the Java `compare-bench` (a consumer group versus an ack queue on the same poisoned workload) on demand and serves the latest result.
 * Needs EMBER_CLIENT_JAR (the shaded ember-client jar); without it the dashboard still shows the last saved result.
 */
class Compare {
  constructor({ jar = process.env.EMBER_CLIENT_JAR, bootstrap = process.env.EMBER_BOOTSTRAP || 'localhost:19092,localhost:19093,localhost:19094' } = {}) {
    this.jar = jar;
    this.bootstrap = bootstrap;
    this.running = false;
    this.startedAt = 0;
    this.error = null;
  }

  result() {
    for (const f of [RESULT, SAMPLE]) {
      try { return { ...JSON.parse(fs.readFileSync(f, 'utf8')), sample: f === SAMPLE }; } catch { /* try the next */ }
    }
    return null;
  }

  status() {
    return { available: !!this.jar, running: this.running, startedAt: this.startedAt, error: this.error, result: this.result() };
  }

  run() {
    if (!this.jar) throw new Error('set EMBER_CLIENT_JAR to the ember-client jar to run the comparison from here');
    if (this.running) throw new Error('a comparison is already running');
    this.running = true;
    this.startedAt = Date.now();
    this.error = null;
    fs.mkdirSync(path.dirname(RESULT), { recursive: true });
    const tmp = RESULT + '.tmp';
    const child = spawn('java', ['-jar', this.jar, 'compare-bench', '--bootstrap=' + this.bootstrap, '--records=3000', '--partitions=6', '--poison-every=100',
      '--work-ms=20', '--seconds=30', '--queue-workers=24', '--json=' + tmp], { stdio: 'ignore' });
    child.on('exit', code => {
      this.running = false;
      if (code === 0) { try { fs.renameSync(tmp, RESULT); } catch (e) { this.error = e.message; } } else this.error = 'compare-bench exited with code ' + code;
    });
    child.on('error', e => { this.running = false; this.error = e.message; });
  }
}

module.exports = { Compare };
