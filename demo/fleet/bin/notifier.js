#!/usr/bin/env node
'use strict';
require('./run-service').run('notifier').catch(e => { console.error(e.message); process.exit(1); });
