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
"""googledatastore client."""
import os
import threading

from googledatastore import helper
from googledatastore.connection import *
from googledatastore.datastore_v1_pb2 import *


_conn_holder = {}  # thread id -> thread-local connection.
_options = {}  # Global options.
# Guards all access to _options and writes to _conn_holder.
_rlock = threading.RLock()


def set_options(**kwargs):
  """Set datastore connection options.

  Args:
    credentials: oauth2client.Credentials to authorize the
    connection.
    dataset: the dataset to send RPCs to.
    host: the host used to construct the datastore API, default to Google
    APIs production server.
  """
  with(_rlock):
    _options.update(kwargs)
    _conn_holder.clear()


def get_default_connection():
  """Return the default datastore connection.

  Defaults dataset to os.getenv('DATASTORE_DATASET'), host to
  os.getenv('DATASTORE_HOST'), and credentials to
  helper.get_credentials_from_env().

  Use set_options to override defaults.
  """
  tid = id(threading.current_thread())
  conn = _conn_holder.get(tid)
  if not conn:
    with(_rlock):
      # No other thread would insert a value in our slot, so no need
      # to recheck existence inside the lock.
      if 'dataset' not in _options:
        _options['dataset'] = os.getenv('DATASTORE_DATASET')
      if 'host' not in _options:
        _options['host'] = os.getenv('DATASTORE_HOST')
      if 'credentials' not in _options:
        _options['credentials'] = helper.get_credentials_from_env()
      # We still need the lock when caching the thread local connection so we
      # don't race with _conn_holder.clear() in set_options().
      _conn_holder[tid] = conn = Datastore(**_options)
  return conn


def lookup(request):
  """See connection.Datastore.lookup."""
  return get_default_connection().lookup(request)


def run_query(request):
  """See connection.Datastore.run_query."""
  return get_default_connection().run_query(request)


def begin_transaction(request):
  """See connection.Datastore.begin_transaction."""
  return get_default_connection().begin_transaction(request)


def commit(request):
  """See connection.Datastore.commit."""
  return get_default_connection().commit(request)


def rollback(request):
  """See connection.Datastore.rollback."""
  return get_default_connection().rollback(request)


def allocate_ids(request):
  """See connection.Datastore.allocate_ids."""
  return get_default_connection().allocate_ids(request)
