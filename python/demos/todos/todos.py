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
DATASTORE_SERVICE_ACCOUNT=.. DATASTORE_PRIVATE_KEY_FILE=.. \
todos.py <DATASET_ID>
Then browse http://localhost:5000/static/index.html
"""


from datetime import datetime
import json
import sys

from flask import abort
from flask import Flask
from flask import request

import googledatastore as datastore
from googledatastore.helper import *


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
    entity = req.mutation.upsert.add()
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
            'created': to_timestamp_usec(obj.created)/1e6
            }
      return json.JSONEncoder.default(self, obj)

  def __init__(self, params):
    self.id = params.get('id', None)
    self.text = params['text']
    self.done = params.get('done', False)
    created = params.get('created', None)
    if isinstance(created, float):
      self.created = from_timestamp_usec(created*1e6)
    elif isinstance(created, datetime):
      self.created = created
    else:
      self.created = datetime.now()

  @classmethod
  def from_proto(cls, entity):
    d = {'id': entity.key.path_element[-1].id}
    d.update((p.name, get_value(p.value)) for p in entity.property)
    return Todo(d)

  def to_proto(self):
    entity = datastore.Entity()
    entity.key.path_element.extend(default_todo_list.key.path_element)
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
    """Query for all Todo items ordered by creation date."""

    req = datastore.RunQueryRequest()
    q = req.query
    set_kind(q, kind='Todo')
    set_property_filter(q.filter, '__key__',
                        datastore.PropertyFilter.HAS_ANCESTOR,
                        default_todo_list.key)
    resp = datastore.run_query(req)
    todos = [Todo.from_proto(r.entity) for r in resp.batch.entity_result]
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
    keys = [r.entity.key for r in resp.batch.entity_result]
    req = datastore.CommitRequest()
    req.transaction = tx
    req.mutation.delete.extend(keys)
    resp = datastore.commit(req)
    return ''

  def save(self):
    """Update or insert a Todo item."""
    req = datastore.CommitRequest()
    req.mode = datastore.CommitRequest.NON_TRANSACTIONAL
    proto = self.to_proto()
    mutation = req.mutation.upsert if self.id else req.mutation.insert_auto_id
    mutation.extend([proto])
    resp = datastore.commit(req)
    if not self.id:
      keys = resp.mutation_result.insert_auto_id_key
      self.id = keys[0].path_element[-1].id
    return self


app = Flask(__name__)
default_todo_list = TodoList('default').save()

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
    abort(e.response.status)


if __name__ == '__main__':
  # Set dataset from command line argument.
  if len(sys.argv) < 2:
    print 'Usage: todos.py <DATASET_ID>'
    sys.exit(1)
  datastore.set_options(dataset=sys.argv[1])
  app.run(host='0.0.0.0', debug=True)
