#!/usr/bin/env node
'use strict';
require('./run-service').run('dispatch').catch(e => { console.error(e.message); process.exit(1); });
