'use strict';
// The Ember SDK from this repository (no install step needed for the demo); falls back to the published name if it is installed.
try {
  module.exports = require('../../../sdk/node/ember-sdk');
} catch (e) {
  module.exports = require('@ember/sdk');
}
