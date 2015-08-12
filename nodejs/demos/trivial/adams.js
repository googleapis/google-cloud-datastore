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

var SCOPES = [
  'https://www.googleapis.com/auth/userinfo.email',
  'https://www.googleapis.com/auth/datastore'
];


/**
 * Adams is a simple command line application using the Datastore API.
 *
 * It writes an entity to the datastore that represents a Trivia
 * question with its answer to your dataset. Does a lookup by key and
 * presents the question to the user. If the user gets the right
 * answer, it will greet him with a quote from a famous book.
 *
 * If not running from a Google Compute Engine instance, the path to
 * the Service Account's .json key must be set to the environment
 * variable: "DATASTORE_JSON_PRIVATE_KEY_FILE"
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
  var self = this;
  // First, try to retrieve credentials from Compute Engine metadata server.
  this.credentials = new googleapis.auth.Compute();
  this.credentials.getAccessToken(function(computeErr) {
    if (computeErr) {
      var errors = {'compute auth error': computeErr};

      // Fallback on JWT credential if Compute is not available
      self.key = require(process.env['DATASTORE_JSON_PRIVATE_KEY_FILE']);
      self.credentials = new googleapis.auth.JWT(
          self.key.client_email,
          null /* path to private_key.pem */,
          self.key.private_key,
          SCOPES,
          null /* user to impersonate */);

      self.credentials.authorize(function(jwtErr) {
        if (jwtErr) {
          errors['jwt auth error'] = jwtErr;
          self.emit('error', errors);
          return;
        }
        self.start();
      });

      return;
    }
    self.start();
  });

};


/**
 * Create the datastore object
 */
Adams.prototype.start = function() {
  this.datastore = googleapis.datastore({
    version: 'v1beta2',
    auth: this.credentials,
    projectId: this.datasetId,
    params: {datasetId: this.datasetId}
  });

  this.beginTransaction();
};


/**
 * Start a new transaction.
 */
Adams.prototype.beginTransaction = function() {
  var self = this;
  this.datastore.datasets.beginTransaction(
      {
          // Execute the RPC asynchronously, and call back with either an
          // error or the RPC result.
      },
      function(err, result) {
        if (err) {
          self.emit('error', {'rpc error': err});
          return;
        }
        self.transaction = result.transaction;
        self.lookup();
      });
};


/**
 * Lookup for the Trivia entity.
 */
Adams.prototype.lookup = function() {
  var self = this;
  this.datastore.datasets.lookup(
      {
        resource: {
          // Set the transaction, so we get a consistent snapshot of the
          // value at the time the transaction started.
          transaction: this.transaction,
          // Add one entity key to the lookup request, with only one
          // `path` element (i.e. no parent).
          keys: [{path: [{kind: 'Trivia', name: 'hgtg'}]}]
        }
      },
      function(err, result) {
        if (err) {
          self.emit('error', {'rpc error': err});
          return;
        }
        // Get the entity from the response if found.
        if (result.found.length > 0) {
          self.entity = result.found[0].entity;
        }
        self.commit();
      });
};


/**
 * Commit the transaction and an insert mutation if the entity was not
 * found.
 */
Adams.prototype.commit = function() {
  var self = this;
  if (!this.entity) {
    // If the entity is not found create it.
    this.entity = {
      // Set the entity key with only one `path` element (i.e. no parent).
      key: {path: [{kind: 'Trivia', name: 'hgtg'}]},
      // Set the entity properties:
      // - a utf-8 string: `question`
      // - a 64bit integer: `answer`
      properties: {
        question: {stringValue: 'Meaning of life?'},
        answer: {integerValue: 42}
      }
    };
    // Build a mutation to insert the new entity.
    mutation = {insert: [this.entity]};
  } else {
    mutation = null;
  }

  // Commit the transaction and the insert mutation if the entity was not found.
  this.datastore.datasets.commit(
      {resource: {transaction: this.transaction, mutation: mutation}},
      function(err, result) {
        if (err) {
          self.emit('error', err);
          return;
        }
        self.ask();
      });
};


/**
 * Ask for the question and validate the answer.
 */
Adams.prototype.ask = function() {
  var self = this;
  // Get `question` property value.
  var question = this.entity.properties.question.stringValue;
  // Get `answer` property value.
  var answer = this.entity.properties.answer.integerValue;
  // Print the question and read one line from stdin.
  this.readline.question(question + ' ', function(result) {
    self.readline.close();
    // Validate the input against the entity answer property.
    if (parseInt(result, 10) == answer) {
      console.log('fascinating, extraordinary and, ',
                  'when you think hard about it, completely obvious.');
    } else {
      console.log("Don't panic!");
    }
  });
};


console.assert(process.argv.length == 3, 'usage: trivial.js <dataset-id>');
// Get the dataset ID from the command line parameters.
var demo = new Adams(process.argv[2]);
demo.once('error', function(err) {
  console.error('Adams:', err);
  process.exit(1);
});
