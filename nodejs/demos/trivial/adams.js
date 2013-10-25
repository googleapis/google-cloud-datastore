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

var SCOPES = ['https://www.googleapis.com/auth/userinfo.email',
              'https://www.googleapis.com/auth/datastore'];


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
  this.authorize();
}
util.inherits(Adams, events.EventEmitter);


/**
 * Authorize with the Datastore API.
 */
Adams.prototype.authorize = function() {
  // First, try to retrieve credentials from Compute Engine metadata server.
  this.credentials = new googleapis.auth.Compute();
  this.credentials.authorize((function(computeErr) {
    if (computeErr) {
      var errors = {'compute auth error': computeErr};
      // Then, fallback on JWT credentials.
      this.credentials = new googleapis.auth.JWT(
          process.env['DATASTORE_SERVICE_ACCOUNT'],
          process.env['DATASTORE_PRIVATE_KEY_FILE'],
          SCOPES);
      this.credentials.authorize((function(jwtErr) {
        if (jwtErr) {
          errors['jwt auth error'] = jwtErr;
          this.emit('error', errors);
          return;
        }
        this.connect();
      }).bind(this));
    }
  }).bind(this));
};


/**
 * Connect to the Datastore API.
 */
Adams.prototype.connect = function() {
  // Build the API bindings for the current version.
  googleapis.discover('datastore', 'v1beta2')
      .withAuthClient(this.credentials)
      .execute((function(err, client) {
        if (err) {
          this.emit('error', {'connection error': err});
          return;
        }
        // Bind the datastore client to datasetId and get the datasets
        // resource.
        this.datastore = client.datastore.withDefaultParams({
          datasetId: this.datasetId}).datasets;
        this.beginTransaction();
      }).bind(this));
};


/**
 * Start a new transaction.
 */
Adams.prototype.beginTransaction = function() {
  this.datastore.beginTransaction({
    // Execute the RPC asynchronously, and call back with either an
    // error or the RPC result.
  }).execute((function(err, result) {
    if (err) {
      this.emit('error', {'rpc error': err});
      return;
    }
    this.transaction = result.transaction;
    this.lookup();
  }).bind(this));
};


/**
 * Lookup for the Trivia entity.
 */
Adams.prototype.lookup = function() {
  // Get entities by key.
  this.datastore.lookup({
    readOptions: {
      // Set the transaction, so we get a consistent snapshot of the
      // value at the time the transaction started.
      transaction: this.transaction
    },
    // Add one entity key to the lookup request, with only one
    // `path` element (i.e. no parent).
    keys: [{ path: [{ kind: 'Trivia', name: 'hgtg' }] }]
  }).execute((function(err, result) {
    if (err) {
      this.emit('error', {'rpc error': err});
      return;
    }
    // Get the entity from the response if found.
    if (result.found) {
      this.entity = result.found[0].entity;
    }
    this.commit();
  }).bind(this));
};


/**
 * Commit the transaction and an insert mutation if the entity was not
 * found.
 */
Adams.prototype.commit = function() {
  if (!this.entity) {
    // If the entity is not found create it.
    this.entity = {
        // Set the entity key with only one `path` element (i.e. no parent).
        key: { path: [{ kind: 'Trivia', name: 'hgtg' }] },
        // Set the entity properties:
        // - a utf-8 string: `question`
        // - a 64bit integer: `answer`
        properties: {
          question: { stringValue: 'Meaning of life?' },
          answer: { integerValue: 42 }
        }
      };
    // Build a mutation to insert the new entity.
    mutation = { insert: [this.entity] };
  } else {
    // No mutation if the entity was found.
    mutation = null;
  }
  // Commit the transaction and the insert mutation if the entity was not found.
  this.datastore.commit({
    transaction: this.transaction,
    mutation: mutation
  }).execute((function(err, result) {
    if (err) {
      this.emit('error', err);
      return;
    }
    this.ask();
  }).bind(this));
};


/**
 * Ask for the question and validate the answer.
 */
Adams.prototype.ask = function() {
  // Get `question` property value.
  var question = this.entity.properties.question.stringValue;
  // Get `answer` property value.
  var answer = this.entity.properties.answer.integerValue;
  // Print the question and read one line from stdin.
  this.readline.question(question + ' ', (function(result) {
    this.readline.close();
    // Validate the input against the entity answer property.
    if (parseInt(result, 10) == answer) {
      console.log('fascinating, extraordinary and, ',
                  'when you think hard about it, completely obvious.');
    } else {
      console.log("Don't panic!");
    }
  }).bind(this));
};


console.assert(process.argv.length == 3, 'usage: trivial.js <dataset-id>');
// Get the dataset ID from the command line parameters.
var demo = new Adams(process.argv[2]);
demo.once('error', function(err) {
  console.error('Adams:', err);
  process.exit(1);
});
