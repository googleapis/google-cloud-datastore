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

var events = require('events');
var util = require('util');

var gcloud = require('gcloud')({
  projectId: process.env.DATASTORE_ID || 'gcd-codelab',
  keyFilename: process.env.DATASTORE_KEYFILE || '/path/to/keyfile.json'
});

var dataset = gcloud.datastore.dataset();

function Todos(list) {
  events.EventEmitter.call(this);

  this.list = list;
}

util.inherits(Todos, events.EventEmitter);

Todos.prototype.ls = function() {
  var query = dataset.createQuery('Todo')
      .hasAncestor(dataset.key(['TodoList', this.list]));

  dataset.runQuery(query, function(err, entities) {
    if (err) {
      this.emit('error', err);
      return;
    }

    this.emit('todos fetched', entities);
  }.bind(this));
};

Todos.prototype.add = function(title) {
  dataset.save({
    key: dataset.key(['TodoList', this.list, 'Todo']),
    data: {
      title: title,
      completed: false
    }
  }, function(err, entities) {
    if (err) {
      this.emit('error', err);
      return;
    }

    this.emit('todo added', entities);
  }.bind(this));
};

Todos.prototype.get = function(id) {
  var key = dataset.key(['TodoList', this.list, 'Todo', String(id)]);
  dataset.get(key, function(err, entity) {
    if (err) {
      this.emit('error', err);
      return;
    }

    this.emit('todo fetched', entity);
  }.bind(this));
};

Todos.prototype.del = function(id) {
  var key = dataset.key(['TodoList', this.list, 'Todo', String(id)]);
  dataset.delete(key, function(err) {
    if (err) {
      this.emit('error', err);
      return;
    }

    this.emit('todo deleted');
  }.bind(this));
};

Todos.prototype.edit = function(id, title, completed) {
  var key = dataset.key(['TodoList', this.list, 'Todo', String(id)]);
  var data = {
    completed: false
  };

  if (typeof title !== 'undefined') {
    data.title = title;
  }

  if (typeof completed !== 'undefined') {
    data.completed = Boolean(completed);
  }

  dataset.save({
    key: key,
    data: data
  }, function(err, entity) {
    if (err) {
      this.emit('error', err);
      return;
    }

    this.emit('todo edited', entity);
  }.bind(this));
};

Todos.prototype.archive = function() {
  var query = dataset.createQuery('Todo')
      .hasAncestor(dataset.key(['TodoList', this.list]))
      .filter('completed =', true);

  dataset.runInTransaction(function(transaction, done) {
    transaction.runQuery(query, queryHandler);

    var keys = [];

    function deleteKeys() {
      if (keys.length === 0) {
        done();
        return;
      }

      transaction.delete(keys, function(err) {
        if (err) {
          transaction.rollback(done);
          return;
        }
        done();
      });
    }

    function queryHandler(err, entities, nextQuery) {
      if (err) {
        transaction.rollback(done);
        return;
      }

      keys = keys.concat(entities.map(function(entity) {
        return entity.key;
      }));

      if (nextQuery) {
        transaction.runQuery(nextQuery, queryHandler);
      } else {
        deleteKeys();
      }
    }
  }, function(err) {
    if (err) {
      this.emit('error', err);
      return;
    }

    this.emit('todos archived');
  }.bind(this));
};

module.exports = Todos;