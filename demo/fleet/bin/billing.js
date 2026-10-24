#!/usr/bin/env node
'use strict';
require('./run-service').run('billing').catch(e => { console.error(e.message); process.exit(1); });
