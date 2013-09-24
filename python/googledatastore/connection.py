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
import os
import httplib2

from googledatastore import datastore_v1_pb2

__all__ = [
    'Datastore',
    'Error',
    'HTTPError',
    'RPCError',
    'AuthError',
    'BadArgumentError'
]

SCOPE = ('https://www.googleapis.com/auth/datastore '
         'https://www.googleapis.com/auth/userinfo.email')
GOOGLEAPIS_URL = 'https://www.googleapis.com'
API_VERSION = 'v1beta2'


class Datastore(object):
  """Datastore client connection constructor."""

  def __init__(self, dataset, credentials=None, host=None):
    """Datastore client connection constructor.

    Args:
      dataset: dataset to send the RPC to.
      credentials: oauth2client.Credentials to authorize the
      connection, default to no credentials.
      host: the host used to construct the datastore API. Defaults to
      'https://www.googleapis.com'.

    Usage: demos/trivial.py for example usages.

    Raises:
      TypeError: when dataset is needed, but not provided.
    """
    self._http = httplib2.Http()
    if not dataset:
      raise TypeError('dataset argument is required')
    if not host:
      host = GOOGLEAPIS_URL
    url_internal_override = os.getenv('DATASTORE_URL_INTERNAL_OVERRIDE')
    if url_internal_override:
      self._url = '%s/datasets/%s/' % (url_internal_override, dataset)
    else:
      self._url = '%s/datastore/%s/datasets/%s/' % (host, API_VERSION, dataset)
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
                             datastore_v1_pb2.LookupResponse)

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
                             datastore_v1_pb2.RunQueryResponse)

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
                             datastore_v1_pb2.BeginTransactionResponse)

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
                             datastore_v1_pb2.CommitResponse)

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
                             datastore_v1_pb2.RollbackResponse)

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
                             datastore_v1_pb2.AllocateIdsResponse)

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
      BadArgumentError: No dataset has been defined.
      RPCError: The rpc method call failed.
    """
    payload = req.SerializeToString()
    headers = {
        'Content-Type': 'application/x-protobuf',
        'Content-Length': str(len(payload))
        }
    response, content = self._http.request(
        self._url + method, method='POST', body=payload, headers=headers)
    if response.status != 200:
      raise RPCError(method, response, content)
    resp = resp_class()
    resp.ParseFromString(content)
    return resp


class Error(Exception):
  """A Datastore service error occured."""
  pass


class HTTPError(Error):
  """An HTTP error occured."""

  response = None


class RPCError(HTTPError):
  """The Datastore RPC failed."""

  method = None
  reason = None
  _failure_format = ('{method} RPC {failure_type} failure '
                     'with HTTP({http_status}) {http_reason}: {failure_reason}')

  def __init__(self, method, response, content):
    self.method = method
    self.response = response
    self.reason = content
    super(RPCError, self).__init__(self._failure_format.format(
        method=method,
        failure_type=('server' if response.status >= 500
                      else 'client'),
        http_status=response.status,
        http_reason=response.reason,
        failure_reason=content
        ))


class AuthError(Error):
  """Authentication failed."""
  pass


class BadArgumentError(Error):
  """Argument validation failed."""
  pass
