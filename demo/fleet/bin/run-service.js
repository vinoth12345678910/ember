'use strict';
const { Bank } = require('../lib/bank');
const { Control } = require('../lib/control');
const { emberFor } = require('../lib/config');
const { Simulator } = require('../lib/simulator');
const { startDispatch, startBilling, startNotifier } = require('../lib/services');

const arg = (name, fallback) => { const i = process.argv.indexOf('--' + name); return i > 0 ? Number(process.argv[i + 1]) : fallback; };

/** Runs one service until Ctrl+C, printing a line of counters every 5 seconds. */
async function run(service) {
  const control = new Control();
  let handle, snapshot;
  if (service === 'simulator') {
    const sim = new Simulator({ ember: emberFor('simulator'), drivers: arg('drivers', 1000), pingMs: arg('ping-ms', 4000), ridersPerSec: arg('riders', 10), timeScale: arg('time-scale', 1) }).start();
    handle = () => sim.stop(); snapshot = () => sim.snapshot();
  } else if (service === 'dispatch') {
    const d = startDispatch({ ember: emberFor('dispatch') });
    handle = d.stop; snapshot = () => ({ drivers: d.grid.size, ...d.stats });
  } else if (service === 'billing') {
    const bank = new Bank({ control });
    const b = startBilling({ ember: emberFor('billing'), bank });
    handle = b.stop; snapshot = () => ({ ...b.stats, bank: bank.stats });
  } else if (service === 'notifier') {
    const n = startNotifier({ ember: emberFor('notifier') });
    handle = n.stop; snapshot = () => n.stats;
  } else {
    throw new Error('unknown service ' + service);
  }
  console.log(`${service} running (Ctrl+C to stop)`);
  const timer = setInterval(() => console.log(new Date().toISOString().slice(11, 19), service, JSON.stringify(snapshot())), 5000);
  process.on('SIGINT', async () => { clearInterval(timer); await handle(); console.log('stopped', JSON.stringify(snapshot())); process.exit(0); });
}

module.exports = { run };
