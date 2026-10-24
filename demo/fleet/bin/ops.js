#!/usr/bin/env node
'use strict';
// Operator actions on the pretend world.   node bin/ops.js bank down | bank up | status
const { Control } = require('../lib/control');
const c = new Control();
const [what, state] = process.argv.slice(2);
if (what === 'bank' && (state === 'down' || state === 'up')) { c.set({ bankDown: state === 'down' }); console.log('bank is now', state); }
else if (what === 'status') console.log(JSON.stringify(c.get()));
else { console.error('usage: ops.js bank down|up | status'); process.exit(2); }
