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
var googleapis = require('googleapis'),
    authclient = new googleapis.OAuth2Client(),
    datasetId = 'gcd-codelab',
    compute = new googleapis.auth.Compute(),
    datastore = null,
    todoListName = null;

var usage = 'usage todo.js <todolist> <add|get|del|edit|ls|archive> [todo-title|todo-id]';
compute.authorize(function(err, result) {
  console.assert(!err, err);
  googleapis.discover('datastore', 'v1beta1')
    .withAuthClient(compute)
    .execute(function(err, client) {
      datastore = client.datastore.datasets;
      todoListName = process.argv[2];
      var cmd = process.argv[3];
      console.assert(todoListName && cmd && commands[cmd], usage);
      commands[cmd].apply(commands, process.argv.slice(4))
    });
});

var commands = {
  // level 0
  add: function(title) {
    datastore.blindWrite({
      datasetId: datasetId,
      mutation: {
        insertAutoId: [{
          key: {
            path: [{ kind: 'TodoList', name: todoListName },
                   { kind: 'Todo' }]
          },
          properties: {
            title: { values: [{ stringValue: title }] },
            completed: { values: [{ booleanValue: false }] }
          }
        }]
      }
    }).execute(function(err, result) {
      console.assert(!err, err);
      var key = result.mutationResult.insertAutoIdKeys[0];
      console.log('%d: TODO %s', key.path[1].id, title);
    });    
  },
  get: function(id, callback) {
    datastore.lookup({
      datasetId: datasetId,
      keys: [{
        path: [{ kind: 'TodoList', name: todoListName},
               { kind: 'Todo', id: id }]
      }]
    }).execute(function(err, result) {
      console.assert(!err, err);
      console.assert(!result.missing, 'todo %d: not found', id);
      var entity = result.found[0].entity;
      var title = entity.properties.title.values[0].stringValue;
      var completed = entity.properties.completed.values[0].booleanValue == true;
      if (callback) {
        callback(err, id, title, completed);
      } else {
        console.log('%d: %s %s', id, completed && 'DONE' || 'TODO', title);
      }
    });
  },
  // level 1
  del: function(id) {
    datastore.blindWrite({
      datasetId: datasetId,
      mutation: {
        delete: [{
          path: [{ kind: 'TodoList', name: todoListName },
                 { kind: 'Todo', id: id }]
        }]
      }   
    }).execute(function(err, result) {
      console.assert(!err, err);
      console.log('%d: DEL', id);
    });
  },
  // level 2
  edit: function(id, title, completed) {
    completed = completed === 'true';
    datastore.blindWrite({
      datasetId: datasetId,
      mutation: {
        update: [{
          key: {
            path: [{ kind: 'TodoList', name: todoListName },
                   { kind: 'Todo', id: id } ]
          },
          properties: {
            title: { values: [{ stringValue: title }] },
            completed: { values: [{ booleanValue: completed }] }
          }
        }]
      }
    }).execute(function(err, result) {
      console.assert(!err, err);
      console.log('%d: %s %s', id, completed && 'DONE' || 'TODO', title);
    });
  },
  ls: function() {
    datastore.runQuery({
      datasetId: datasetId,
      query: {
        kinds: [{ name: 'Todo' }],
        filter: {
          propertyFilter: {
            property: { name: '__key__' },
            operator: 'hasAncestor',
            value: {
              keyValue: {
                path: [{ kind: 'TodoList', name: todoListName }]
              }
            }
          }
        }
      }
    }).execute(function(err, result) {
      var entityResults = result.batch.entityResults || [];
      entityResults.forEach(function(entityResult) {
        var entity = entityResult.entity;
        var id = entity.key.path[1].id;
        var properties = entity.properties;
        var title = properties.title.values[0].stringValue;
        var completed = properties.completed.values[0].booleanValue == true;
        console.log('%d: %s %s', id, completed && 'DONE' || 'TODO', title);
      });

    });
  },
  // level 3
  archive: function() {
    datastore.beginTransaction({
      datasetId: datasetId
    }).execute(function(err, result) {
      var tx = result.transaction;
      datastore.runQuery({
        datasetId: datasetId,
        readOptions: { transaction: tx },
        query: {
          kinds: [{ name: 'Todo' }],
          filter: {
            compositeFilter: {
              operator: 'and',
              filters: [{
                propertyFilter: {
                  property: { name: '__key__' },
                  operator: 'hasAncestor',
                  value: { keyValue: {
                    path: [{ kind: 'TodoList', name: todoListName }]
                  }}
                }
              }, {
                propertyFilter: {
                  property: { name: 'completed' },
                  operator: 'equal',
                  value: { booleanValue: true }
                }
              }]
            }
          }
        }
      }).execute(function(err, result) {
        var keys = [];
        var entityResults = result.batch.entityResults || [];
        entityResults.forEach(function(entityResult) {
          keys.push(entityResult.entity.key);
        });
        datastore.commit({
          datasetId: datasetId,
          transaction: tx,
          mutation: { delete: keys }
        }).execute(function(err, result) {
          console.assert(!err, err);
          keys.forEach(function(key) {
            console.log('%d: DEL', key.path[1].id);
          });
        });
      });
    });
  }
};
