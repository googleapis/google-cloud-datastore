#! /usr/bin/env node
//
// Copyright 2013 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

var util = require('util');
var events = require('events');
var readline = require('readline');

var googleapis = require('googleapis');



/**
 * Adams is a simple command line application using the Datastore API.
 *
 * It writes an entity to the datastore that represents a Trivia
 * question with its answer to your dataset. Does a lookup by key and
 * presents the question to the user. If the user gets the right
 * answer, it will greet him with a quote from a famous book.
 *
 * @param {string} datasetId The ID of the dataset.
 * @constructor
 * @extends {events.EventEmitter}
 */
function Adams(datasetId) {
  this.datasetId = datasetId;
  this.readline = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });
  // Retrieve credentials from Compute Engine metadata server.
  this.compute = new googleapis.auth.Compute();
  this.compute.authorize(this.onAuthorized.bind(this));
}
util.inherits(Adams, events.EventEmitter);


/**
 * Connect to the Datastore API.
 * @param {?Object} err compute authorization error or null if no error.
 */
Adams.prototype.onAuthorized = function(err) {
  if (err) {
    this.emit('error', err);
    return;
  }
  // Build the API bindings for the current version.
  googleapis.discover('datastore', 'v1beta1')
      .withAuthClient(this.compute)
      .execute(this.onConnected.bind(this));
};


/**
 * Run the datastore demo.
 * @param {?Object} err Datastore API discovery error or null if no error.
 * @param {Object} client Datastore API client.
 */
Adams.prototype.onConnected = function(err, client) {
  if (err) {
    this.emit('error', err);
    return;
  }
  this.datastore = client.datastore.datasets;

  // Create a new transaction.
  this.datastore.beginTransaction({
    datasetId: this.datasetId
    // Execute the RPC asynchronously, and call back with either an
    // error or the RPC result.
  }).execute(this.onBeginTransactionDone.bind(this));
};


/**
 * Callback when the BeginTransaction RPC has succeeded.
 * @param {?Object} err RPC error or null if no error.
 * @param {Object} result BlindWrite RPC response.
 */
Adams.prototype.onBeginTransactionDone = function(err, result) {
  if (err) {
    this.emit('error', err);
    return;
  }
  this.transaction = result.transaction;

  // Get entities by key.
  this.datastore.lookup({
    datasetId: this.datasetId,
    readOptions: {
      // Set the transaction, so we get a consistent snapshot of the
      // value at the time the transaction started.
      transaction: this.transaction
    },
    // Add one entity key to the lookup request, with only one
    // `path` element (i.e. no parent).
    keys: [{ path: [{ kind: 'Trivia', name: 'hgtg' }] }]
  }).execute(this.onLookupDone.bind(this));
};


/**
 * Callback when the Lookup RPC has succeeded.
 * @param {?Object} err RPC error or null if no error.
 * @param {Object} result Lookup RPC response.
 */
Adams.prototype.onLookupDone = function(err, result) {
  if (err) {
    this.emit('error', err);
    return;
  }
  if (result.found) {
    // Get the entity from the response if found.
    this.entity = result.found[0].entity;
    // No mutation.
    mutation = null;
  } else {
    // If the entity is not found create it.
    this.entity = {
        // Set the entity key with only one `path` element (i.e. no parent).
        key: { path: [{ kind: 'Trivia', name: 'hgtg' }] },
        // Set the entity properties:
        // - a utf-8 string: `question`
        // - a 64bit integer: `answer`
        properties: {
          question: { values: [{ stringValue: 'Meaning of life?' }] },
          answer: { values: [{ integerValue: 42 }] }
        }
      };
    // Build a mutation to insert the new entity.
    mutation = { insert: [this.entity] };
  }
  // Commit the transaction and the insert mutation if the entity was not found.
  this.datastore.commit({
    datasetId: this.datasetId,
    transaction: this.transaction,
    mutation: mutation
  }).execute(this.onCommitDone.bind(this));
};


/**
 * Callback when the Commit RPC has succeeded.
 * @param {?Object} err RPC error or null if no error.
 * @param {Object} result BlindWrite RPC response.
 */
Adams.prototype.onCommitDone = function(err, result) {
  if (err) {
    this.emit('error', err);
    return;
  }
  // Get `question` property value.
  this.question = this.entity.properties.question.values[0].stringValue;
  // Get `answer` property value.
  this.answer = this.entity.properties.answer.values[0].integerValue;
  // Print the question and read one line from stdin.
  this.readline.question(this.question + ' ', this.onAnswered.bind(this));
};


/**
 * Callback when the question has been answered.
 * @param {string} result answer to the question.
 */
Adams.prototype.onAnswered = function(result) {
  this.readline.close();
  // Validate the input against the entity answer property.
  if (parseInt(result, 10) == this.answer) {
    console.log('fascinating, extraordinary and, ',
                'when you think hard about it, completely obvious.');
  } else {
    console.log("Don't panic!");
  }
};


console.assert(process.argv.length == 3, 'usage: trivial.js <dataset-id>');
// Get the dataset ID from the command line parameters.
var demo = new Adams(process.argv[2]);
demo.once('error', function(err) {
  console.error('Adams:', err);
});
