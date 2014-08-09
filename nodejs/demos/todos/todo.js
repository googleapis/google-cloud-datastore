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

// See https://github.com/GoogleCloudPlatform/gcloud-node#elsewhere
// to configure dataset ID and key filename.
var DATASET_ID = 'gcd-codelab',
    KEY_FILENAME = '/path/to/key/file.json';

var gcloud = require('gcloud'),
    datastore = gcloud.datastore;

var usage = 'usage todo.js <todolist> <add|get|del|edit|ls|archive> [todo-title|todo-id]';
var cmd = process.argv[3];
    todoListName = process.argv[2];

console.assert(todoListName && cmd && commands[cmd], usage);
commands[cmd].apply(commands, process.argv.slice(4));

var ds = gcloud.datastore.dataset({
  projectId: DATASET_ID,
  keyFilename: KEY_FILENAME
});

var commands = {
  // level 0
  add: function(title) {
    ds.save({
      key: datastore.key('TodoList', todoListName, 'Todo', null),
      data: { title: title, completed: false }
    }, function(err, key) {
      console.assert(!err, err);
      console.log('Added TODO %s', title);
    });
  },
  get: function(id, callback) {
    var key = datastore.key('TodoList', todoListName, 'Todo', id);
    ds.get(key, function(err, obj) {
      console.assert(!err, err);
      console.log('%d: %s %s', id, obj.data.completed && 'DONE' || 'TODO', obj.data.title);
    });
  },
  // level 1
  del: function(id) {
    var key = datastore.key('TodoList', todoListName, 'Todo', id);
    ds.delete(key, function(err) {
      console.assert(!err, err);
      console.log('%d: DEL', id);
    });
  },
  // level 2
  edit: function(id, title, completed) {
    completed = completed === 'true';
    ds.save({
      key: datastore.key('TodoList', todoListName, 'Todo', id),
      data: { title: title, completed: completed }
    }, function(err, key) {
      console.assert(!err, err);
      console.log('%d: %s %s', id, completed && 'DONE' || 'TODO', title);
    });
  },
  ls: function() {
    var q = ds.createQuery('Todo')
        .hasAncestor(datastore.key('TodoList', todoListName));
    ds.runQuery(q, function(err, items) {
      console.assert(!err, err);
      items.forEach(function(item) {
        console.log('%d: %s %s', id, item.data.completed && 'DONE' || 'TODO', item.title);
      });
    });
  },
  // level 3
  archive: function() {
  // Deletes completed todos
    ds.transaction(function(t, done) {
      t.runQuery(q, function(err, items) {
        if (err) {
          t.rollback(done);
          return;
        }
        var keys = (items || []).map(function(item) {
          return item.key;
        });
        t.delete(keys, function(err) {
          if (err) {
            t.rollback(done);
            return;
          }
          // auto commits if no error.
        });
      });
    }, function(err) {
      console.assert(!err, err);
    });
  }
};
