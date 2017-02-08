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

from . import helper
from . import connection
from .connection import *
# Import the Datastore protos. These are listed separately to avoid importing
# the Datastore service, which conflicts with our Datastore class.
from google.cloud.proto.datastore.v1.datastore_pb2 import (
    LookupRequest,
    LookupResponse,
    RunQueryRequest,
    RunQueryResponse,
    BeginTransactionRequest,
    BeginTransactionResponse,
    CommitRequest,
    CommitResponse,
    RollbackRequest,
    RollbackResponse,
    AllocateIdsRequest,
    AllocateIdsResponse,
    Mutation,
    MutationResult,
    ReadOptions)
from google.cloud.proto.datastore.v1.entity_pb2 import *
from google.cloud.proto.datastore.v1.query_pb2 import *

from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.struct_pb2 import NULL_VALUE
from google.rpc.status_pb2 import Status
from google.rpc import code_pb2
from google.type.latlng_pb2 import LatLng

__version__ = '7.0.0'
VERSION = (7, 0, 0, '~')

_conn_holder = {}  # thread id -> thread-local connection.
_options = {}  # Global options.
# Guards all access to _options and writes to _conn_holder.
_rlock = threading.RLock()


def set_options(**kwargs):
  """Set datastore connection options.

  Args:
    project_id: the Cloud project to connect to. Exactly one of
        project_endpoint and project_id must be set.
    credentials: oauth2client.Credentials to authorize the
        connection.
    project_endpoint: the Cloud Datastore API project endpoint to use.
        Defaults to the Google APIs production server. Must not be set if host
        is also set.
    host: the Cloud Datastore API host to use. Defaults to the Google APIs
        production server. Must not be set if project_endpoint is also set.
  """
  with(_rlock):
    _options.update(kwargs)
    _conn_holder.clear()


def get_default_connection():
  """Returns the default datastore connection.

  Defaults endpoint to helper.get_project_endpoint_from_env() and
  credentials to helper.get_credentials_from_env().

  Use set_options to override defaults.
  """
  tid = id(threading.current_thread())
  conn = _conn_holder.get(tid)
  if not conn:
    with(_rlock):
      # No other thread would insert a value in our slot, so no need
      # to recheck existence inside the lock.
      if 'project_endpoint' not in _options and 'project_id' not in _options:
        _options['project_endpoint'] = helper.get_project_endpoint_from_env()
      if 'credentials' not in _options:
        _options['credentials'] = helper.get_credentials_from_env()
      # We still need the lock when caching the thread local connection so we
      # don't race with _conn_holder.clear() in set_options().
      _conn_holder[tid] = conn = connection.Datastore(**_options)
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
