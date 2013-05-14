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
"""googledatastore connection test suite."""

__author__ = 'proppy@google.com (Johan Euphrosine)'

import httplib2
import mox
import os
import unittest

import googledatastore as datastore
from googledatastore import helper


class TestResponse(object):
  def __init__(self, status, reason):
    self.status = status
    self.reason = reason


class FakeCredentialsFromEnv(object):
  def authorize(self, http):
    pass


class FakeCredentials(object):
  def authorize(self, http):
    pass


class DatastoreTest(unittest.TestCase):

  def setUp(self):
    self.mox = mox.Mox()
    self.conn = datastore.Datastore('foo', host='https://datastore.com')

  def tearDown(self):
    self.mox.UnsetStubs()
    self.mox.ResetAll()

  def makeLookupRequest(self):
    request = datastore.LookupRequest()
    key = request.key.add()
    path = key.path_element.add()
    path.kind = 'Greeting0'
    path.name = 'foo0'
    return request

  def makeLookupResponse(self):
    response = datastore.LookupResponse()
    entity_result = response.found.add()
    path = entity_result.entity.key.path_element.add()
    path.kind = 'Greeting0'
    path.name = 'foo0'
    return response

  def expectRequest(self, *args, **kwargs):
    self.mox.StubOutWithMock(self.conn._http, 'request')
    return self.conn._http.request(*args, **kwargs)

  def testDatasetRequired(self):
    self.assertRaises(TypeError, datastore.Datastore, None)
    self.assertRaises(TypeError, datastore.Datastore, None,
                      host='http://localhost:8080')

  def testLookupSuccess(self):
    request = self.makeLookupRequest()
    payload = request.SerializeToString()
    response = self.makeLookupResponse()
    self.expectRequest(
        'https://datastore.com/datastore/v1beta1/datasets/foo/lookup',
        method='POST', body=payload,
        headers={'Content-Type': 'application/x-protobuf',
                 'Content-Length': str(len(payload))}).AndReturn(
                     (TestResponse(status=200, reason='Found'),
                      response.SerializeToString()))
    self.mox.ReplayAll()

    resp = self.conn.lookup(request)
    self.assertEqual(response, resp)
    self.mox.VerifyAll()

  def testLookupFailure(self):
    request = self.makeLookupRequest()
    payload = request.SerializeToString()
    self.expectRequest(
        'https://datastore.com/datastore/v1beta1/datasets/foo/lookup',
        method='POST',
        body=payload,
        headers={'Content-Type': 'application/x-protobuf',
                 'Content-Length': str(len(payload))}).AndReturn(
                     (TestResponse(status=500, reason='Internal Error'),
                      'failure message'))
    self.mox.ReplayAll()

    self.assertRaises(datastore.RPCError, self.conn.lookup, request)
    self.mox.VerifyAll()

  def testBlindWrite(self):
    request = datastore.BlindWriteRequest()
    request.mutation.upsert.add()
    payload = request.SerializeToString()
    response = datastore.BlindWriteResponse()
    self.expectRequest(
        'https://datastore.com/datastore/v1beta1/datasets/foo/blindWrite',
        method='POST', body=payload,
        headers={'Content-Type': 'application/x-protobuf',
                 'Content-Length': str(len(payload))}).AndReturn(
                     (TestResponse(status=200, reason='Found'),
                      response.SerializeToString()))
    self.mox.ReplayAll()

    resp = self.conn.blind_write(request)
    self.assertEqual(response, resp)
    self.mox.VerifyAll()

  def testRunQuery(self):
    request = datastore.RunQueryRequest()
    request.query.kind.add().name = 'Foo'
    payload = request.SerializeToString()
    response = datastore.RunQueryResponse()
    self.expectRequest(
        'https://datastore.com/datastore/v1beta1/datasets/foo/runQuery',
        method='POST', body=payload,
        headers={'Content-Type': 'application/x-protobuf',
                 'Content-Length': str(len(payload))}).AndReturn(
                     (TestResponse(status=200, reason='Found'),
                      response.SerializeToString()))
    self.mox.ReplayAll()

    resp = self.conn.run_query(request)
    self.assertEqual(response, resp)
    self.mox.VerifyAll()

  def testBeginTransaction(self):
    request = datastore.BeginTransactionRequest()
    payload = request.SerializeToString()
    response = datastore.BeginTransactionResponse()
    self.expectRequest(
        'https://datastore.com/datastore/v1beta1/datasets/foo/'
        'beginTransaction',
        method='POST', body=payload,
        headers={'Content-Type': 'application/x-protobuf',
                 'Content-Length': str(len(payload))}).AndReturn(
                     (TestResponse(status=200, reason='Found'),
                      response.SerializeToString()))
    self.mox.ReplayAll()

    resp = self.conn.begin_transaction(request)
    self.assertEqual(response, resp)
    self.mox.VerifyAll()

  def testCommit(self):
    request = datastore.CommitRequest()
    request.transaction = 'transaction-id'
    payload = request.SerializeToString()
    response = datastore.CommitResponse()
    self.expectRequest(
        'https://datastore.com/datastore/v1beta1/datasets/foo/commit',
        method='POST', body=payload,
        headers={'Content-Type': 'application/x-protobuf',
                 'Content-Length': str(len(payload))}).AndReturn(
                     (TestResponse(status=200, reason='Found'),
                      response.SerializeToString()))
    self.mox.ReplayAll()

    resp = self.conn.commit(request)
    self.assertEqual(response, resp)
    self.mox.VerifyAll()

  def testRollback(self):
    request = datastore.RollbackRequest()
    request.transaction = 'transaction-id'
    payload = request.SerializeToString()
    response = datastore.RollbackResponse()
    self.expectRequest(
        'https://datastore.com/datastore/v1beta1/datasets/foo/rollback',
        method='POST', body=payload,
        headers={'Content-Type': 'application/x-protobuf',
                 'Content-Length': str(len(payload))}).AndReturn(
                     (TestResponse(status=200, reason='Found'),
                      response.SerializeToString()))
    self.mox.ReplayAll()

    resp = self.conn.rollback(request)
    self.assertEqual(response, resp)
    self.mox.VerifyAll()

  def testAllocateIds(self):
    request = datastore.AllocateIdsRequest()
    payload = request.SerializeToString()
    response = datastore.AllocateIdsResponse()
    self.expectRequest(
        'https://datastore.com/datastore/v1beta1/datasets/foo/allocateIds',
        method='POST', body=payload,
        headers={'Content-Type': 'application/x-protobuf',
                 'Content-Length': str(len(payload))}).AndReturn(
                     (TestResponse(status=200, reason='Found'),
                      response.SerializeToString()))
    self.mox.ReplayAll()

    resp = self.conn.allocate_ids(request)
    self.assertEqual(response, resp)
    self.mox.VerifyAll()

  def testDefaultBaseUrl(self):
    self.conn = datastore.Datastore(
        dataset='foo')
    request = self.makeLookupRequest()
    payload = request.SerializeToString()
    response = self.makeLookupResponse()
    self.expectRequest(
        'https://www.googleapis.com/datastore/v1beta1/datasets/foo/lookup',
        method='POST', body=payload,
        headers={'Content-Type': 'application/x-protobuf',
                 'Content-Length': str(len(payload))}).AndReturn(
                     (TestResponse(status=200, reason='Found'),
                      response.SerializeToString()))
    self.mox.ReplayAll()

    resp = self.conn.lookup(request)
    self.assertEqual(response, resp)
    self.mox.VerifyAll()

  def testSetOptions(self):
    datastore._conn = None
    self.mox.StubOutWithMock(os, 'getenv')
    self.mox.StubOutWithMock(helper, 'get_credentials_from_env')
    os.getenv('DATASTORE_HOST').AndReturn('http://localhost:8080')
    helper.get_credentials_from_env().AndReturn(FakeCredentialsFromEnv())
    self.mox.ReplayAll()

    datastore.set_options(dataset='bar')
    conn = datastore.get_default_connection()
    self.assertEqual('http://localhost:8080/datastore/v1beta1/datasets/bar/',
                     conn._url)
    self.assertEqual(FakeCredentialsFromEnv, type(conn._credentials))
    self.mox.VerifyAll()

  def test_functions(self):
    datastore._conn = datastore.Datastore(dataset='foo')
    def caml(s): return ''.join(p[0].upper()+p[1:] for p in s.split('_'))
    rpcs = ['lookup', 'blind_write', 'run_query', 'begin_transaction',
            'commit', 'rollback', 'allocate_ids']
    methods = [(r, getattr(datastore, caml(r)+'Request'),
                getattr(datastore, caml(r)+'Response'))
               for r in rpcs]
    for m, req_class, resp_class in methods:
      self.mox.StubOutWithMock(datastore._conn, m)
      method = getattr(datastore._conn, m)
      method(mox.IsA(req_class)).AndReturn(resp_class())
    self.mox.ReplayAll()

    for m, req_class, resp_class in methods:
      method = getattr(datastore, m)
      result = method(req_class())
      self.assertEqual(resp_class, type(result))
    self.mox.VerifyAll()

if __name__ == '__main__':
  unittest.main()
