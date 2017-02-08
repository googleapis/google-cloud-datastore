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
"""googledatastore connection."""

import logging
import httplib2

from googledatastore import helper
from google.cloud.proto.datastore.v1 import datastore_pb2
from google.protobuf import timestamp_pb2
from google.rpc import code_pb2
from google.rpc import status_pb2
from google.type import latlng_pb2

__all__ = [
    'Datastore',
    'Error',
    'RPCError',
]


class Datastore(object):
  """Datastore client connection constructor."""

  def __init__(self, project_id=None, credentials=None, project_endpoint=None,
               host=None):
    """Datastore client connection constructor.

    Args:
      project_id: the Cloud project to use. Exactly one of
          project_endpoint and project_id must be set.
      credentials: oauth2client.Credentials to authorize the
          connection, default to no credentials.
      project_endpoint: the Cloud Datastore API project endpoint to use. Exactly one of
          project_endpoint and project_id must be set. Must not be set if
          host is also set.
      host: the Cloud Datastore API host to use. Must not be set if project_endpoint
         is also set.

    Usage: demos/trivial.py for example usages.

    Raises:
      TypeError: when neither or both of project_endpoint and project_id
      are set or when both project_endpoint and host are set.
    """
    self._http = httplib2.Http()
    if not project_endpoint and not project_id:
      raise TypeError('project_endpoint or project_id argument is required.')
    if project_endpoint and project_id:
      raise TypeError('only one of project_endpoint and project_id argument '
                      'is allowed.')
    if project_endpoint and host:
      raise TypeError('only one of project_endpoint and host is allowed.')

    self._url = (project_endpoint
                 or helper.get_project_endpoint_from_env(project_id=project_id,
                                                         host=host))

    if credentials:
      self._credentials = credentials
      credentials.authorize(self._http)
    else:
      logging.warning('no datastore credentials')

  def lookup(self, request):
    """Lookup entities by key.

    Args:
      request: LookupRequest proto message.

    Returns:
      LookupResponse proto message.

    Raises:
      RPCError: The underlying RPC call failed with an HTTP error.
          (See: .response attribute)
    """
    return self._call_method('lookup', request,
                             datastore_pb2.LookupResponse)

  def run_query(self, request):
    """Query for entities.

    Args:
      request: RunQueryRequest proto message.

    Returns:
      RunQueryResponse proto message.

    Raises:
      RPCError: The underlying RPC call failed with an HTTP error.
          (See: .response attribute)
    """
    return self._call_method('runQuery', request,
                             datastore_pb2.RunQueryResponse)

  def begin_transaction(self, request):
    """Begin a new transaction.

    Args:
      request: BeginTransactionRequest proto message.

    Returns:
      BeginTransactionResponse proto message.

    Raises:
      RPCError: The underlying RPC call failed with an HTTP error.
          (See: .response attribute)
    """
    return self._call_method('beginTransaction', request,
                             datastore_pb2.BeginTransactionResponse)

  def commit(self, request):
    """Commit a mutation, transaction or mutation in a transaction.

    Args:
      request: CommitRequest proto message.

    Returns:
      CommitResponse proto message.

    Raises:
      RPCError: The underlying RPC call failed with an HTTP error.
          (See: .response attribute)
    """
    return self._call_method('commit', request,
                             datastore_pb2.CommitResponse)

  def rollback(self, request):
    """Rollback a transaction.

    Args:
      request: RollbackRequest proto message.

    Returns:
      RollbackResponse proto message.

    Raises:
      RPCError: The underlying RPC call failed with an HTTP error.
          (See: .response attribute)
    """
    return self._call_method('rollback', request,
                             datastore_pb2.RollbackResponse)

  def allocate_ids(self, request):
    """Allocate ids for incomplete keys.

    Args:
      request: AllocateIdsRequest proto message.

    Returns:
      AllocateIdsResponse proto message.

    Raises:
      RPCError: The underlying RPC call failed with an HTTP error.
          (See: .response attribute)
    """
    return self._call_method('allocateIds', request,
                             datastore_pb2.AllocateIdsResponse)

  def _call_method(self, method, req, resp_class):
    """_call_method call the given RPC method over HTTP.

    It uses the given protobuf message request as the payload and
    returns the deserialized protobuf message response.

    Args:
      method: RPC method name to be called.
      req: protobuf message for the RPC request.
      resp_class: protobuf message class for the RPC response.

    Returns:
      Deserialized resp_class protobuf message instance.

    Raises:
      RPCError: The rpc method call failed.
    """
    payload = req.SerializeToString()
    headers = {
        'Content-Type': 'application/x-protobuf',
        'Content-Length': str(len(payload)),
        'X-Goog-Api-Format-Version': '2'
        }
    response, content = self._http.request(
        '%s:%s' % (self._url, method),
        method='POST', body=payload, headers=headers)
    if response.status != 200:
      raise _make_rpc_error(method, response, content)
    resp = resp_class()
    resp.ParseFromString(content)
    return resp


def _make_rpc_error(method, response, content):
  try:
    status = status_pb2.Status()
    status.ParseFromString(content)
    code_string = code_pb2.Code.Name(status.code)
    return RPCError(
        method, status.code,
        'Error code: %s. Message: %s' % (code_string, status.message))
  except Exception:
    return RPCError(
        method, code_pb2.INTERNAL,
        'HTTP status code: %s. Message: %s' % (response.status, content))


class Error(Exception):
  """A Datastore service error occured."""
  pass


class RPCError(Error):
  """The Datastore RPC failed."""

  method = None
  code = None
  message = None

  _failure_format = ('datastore call {method} failed: {message}')

  def __init__(self, method, code, message):
    self.method = method
    self.code = code
    self.message = message
    super(RPCError, self).__init__(self._failure_format.format(
        method=method,
        message=message))
