#! /usr/bin/env python
#
# Copyright 2013 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Todo webapp demo.

This is a sample JSON backend for a JavaScript todo app.

It support the following methods:
- Create a new todo
POST /todos
> {"text": "do this"}
< {"id": 1, "text": "do this", "created": 1356724843.0, "done": false}
- Update an existing todo
POST /todos
> {"id": 1, "text": "do this", "created": 1356724843.0, "done": true}
< {"id": 1, "text": "do this", "created": 1356724843.0, "done": true}
- List existing todos:
GET /todos
>
< [{"id": 1, "text": "do this", "created": 1356724843.0, "done": true},
   {"id": 2, "text": "do that", "created": 1356724849.0, "done": false}]
- Delete 'done' todos:
DELETE /todos
>
<

Usage:
todos.py <PROJECT_ID>
Then browse http://localhost:5000/static/index.html
"""


import datetime
import json
import sys

from flask import abort
from flask import Flask
from flask import request

import googledatastore as datastore
from googledatastore.helper import *


_EPOCH = datetime.datetime.utcfromtimestamp(0)

class TodoList(object):
  """Todo list model."""

  def __init__(self, name):
    self.name = name

  @property
  def key(self):
    return add_key_path(datastore.Key(), *self.key_path)

  @property
  def key_path(self):
    return ('TodoList', self.name)

  def save(self):
    req = datastore.CommitRequest()
    req.mode = datastore.CommitRequest.NON_TRANSACTIONAL
    entity = req.mutations.add().upsert
    add_key_path(entity.key, *self.key_path)
    datastore.commit(req)
    return self


class Todo(object):
  """Todo item model."""

  class Encoder(json.JSONEncoder):
    """Todo item JSON encoder."""

    def default(self, obj):
      if isinstance(obj, Todo):
        return {
            'id': obj.id,
            'text': obj.text,
            'done': obj.done,
            'created': int((obj.created - _EPOCH).total_seconds() * 1000000)
            }
      return json.JSONEncoder.default(self, obj)

  def __init__(self, params):
    self.id = params.get('id', None)
    self.text = params['text']
    self.done = params.get('done', False)
    created = params.get('created', None)
    if isinstance(created, (float, int)):
      self.created = _EPOCH + datetime.timedelta(microseconds=created)
    elif isinstance(created, datetime.datetime):
      self.created = created
    else:
      self.created = datetime.datetime.now()

  @classmethod
  def from_proto(cls, entity):
    d = {'id': entity.key.path[-1].id}
    d.update((name, get_value(value))
             for name, value in entity.properties.iteritems())
    return Todo(d)

  def to_proto(self):
    entity = datastore.Entity()
    entity.key.path.extend(default_todo_list.key.path)
    if self.id:
      add_key_path(entity.key, 'Todo', self.id)
    else:
      add_key_path(entity.key, 'Todo')
    add_properties(entity, {'text': self.text,
                            'done': self.done,
                            'created': self.created})
    return entity

  @classmethod
  def get_all(cls):
    """Query for all Todo items ordered by creation date.

    This method is eventually consistent to avoid the need for an extra index.
    """

    req = datastore.RunQueryRequest()
    q = req.query
    set_kind(q, kind='Todo')
    add_property_orders(q, 'created')
    resp = datastore.run_query(req)
    todos = [Todo.from_proto(r.entity) for r in resp.batch.entity_results]
    return todos

  @classmethod
  def archive(cls):
    """Delete all Todo items that are done."""
    req = datastore.BeginTransactionRequest()
    resp = datastore.begin_transaction(req)
    tx = resp.transaction
    req = datastore.RunQueryRequest()
    req.read_options.transaction = tx
    q = req.query
    set_kind(q, kind='Todo')
    add_projection(q, '__key__')
    set_composite_filter(q.filter,
                         datastore.CompositeFilter.AND,
                         set_property_filter(
                             datastore.Filter(),
                             'done', datastore.PropertyFilter.EQUAL, True),
                         set_property_filter(
                             datastore.Filter(),
                             '__key__', datastore.PropertyFilter.HAS_ANCESTOR,
                             default_todo_list.key))
    resp = datastore.run_query(req)
    req = datastore.CommitRequest()
    req.transaction = tx
    for result in resp.batch.entity_results:
      req.mutations.add().delete.CopyFrom(result.entity.key)
    resp = datastore.commit(req)
    return ''

  def save(self):
    """Update or insert a Todo item."""
    req = datastore.CommitRequest()
    req.mode = datastore.CommitRequest.NON_TRANSACTIONAL
    req.mutations.add().upsert.CopyFrom(self.to_proto())
    resp = datastore.commit(req)
    if not self.id:
      self.id = resp.mutation_results[0].key.path[-1].id
    return self


app = Flask(__name__)

@app.route('/todos', methods=['GET', 'POST', 'DELETE'])
def TodoService():
  try:
    if request.method == 'GET':
      return json.dumps(Todo.get_all(), cls=Todo.Encoder)
    elif request.method == 'POST':
      todo = Todo(json.loads(request.data))
      return json.dumps(todo.save(), cls=Todo.Encoder)
    elif request.method == 'DELETE':
      return Todo.archive()
    abort(405)
  except datastore.RPCError as e:
    app.logger.error(str(e))
    abort(-1)


if __name__ == '__main__':
  # Set project from command line argument.
  if len(sys.argv) < 2:
    print 'Usage: todos.py <PROJECT_ID>'
    sys.exit(1)
  datastore.set_options(project_id=sys.argv[1])
  default_todo_list = TodoList('default').save()
  print 'Application running, visit localhost:5000/static/index.html'
  app.run(host='0.0.0.0', debug=True)
