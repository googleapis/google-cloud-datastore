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
var googleapis = require('googleapis');
var compute = new googleapis.auth.Compute();
console.assert(process.argv.length == 3, 'usage: trivial.js <dataset-id>');
// Get the dataset from the command line parameters.
var datasetId = process.argv[2];

// Setup the connection to Google Cloud Datastore.
function googledatastore(callback) {
  // Retrieve credentials from Compute Engine metadata server.
  compute.authorize(function(err, result) {
    // Build the API bindings for the current version.
    googleapis.discover('datastore', 'v1beta1')
      .withAuthClient(compute)
      .execute(function(err, client) {
        // Forward the datastore resource to the caller.
        callback(err, client.datastore.datasets);
      });
  });
}

googledatastore(function(err, datastore) {
  console.assert(!err, err);
  // Make write mutations outside of a transaction.
  datastore.blindWrite({
    datasetId: datasetId,
    // Create mutation that update or insert one entity.
    mutation: {
      upsert: [{
        // Set the entity key with only one `path` element: no parent.
        key: { path: [{ kind: 'Trivia', name: 'hgtg'}] },
        // Set the entity properties:
        // - a utf-8 string: `question`
        // - a 64bit integer: `answer`
        properties: {
          question: { values: [{ stringValue: 'Meaning of life?' }] },
          answer: { values: [{ integerValue: 42 }] }
        }
      }]
    }
  }).execute(function(err, result) {
    // Execute the RPC asynchronously, and call back with either an
    // error or the RPC result.
    console.assert(!err, err);
    // Get entities by key.
    datastore.lookup({
      datasetId: datasetId,
      // Lookup only one entity, with the same entity key.
      keys: [{ path: [{ kind: 'Trivia', name: 'hgtg' }] }]
    }).execute(function(err, result) {
      console.assert(result.found.length, 1);
      // Found one entity result.
      var entity = result.found[0].entity;
      // Get `question` property value.
      var question = entity.properties.question.values[0].stringValue;
      // Get `answer` property value.
      var answer = entity.properties.answer.values[0].integerValue;
      // Print the question and read one line from stdin.
      var rl = require('readline').createInterface({
        input: process.stdin,
        output: process.stdout
      });
      rl.question(question + ' ', function(result) {
        // Validate the input against the entity answer property.
        if (parseInt(result, 10) == answer) {
          console.log('fascinating, extraordinary and,',
                      'when you think hard about it, completely obvious.');
        } else {
          console.log("Don't panic!");
        }
        rl.close();
      });
    });
  });
});
