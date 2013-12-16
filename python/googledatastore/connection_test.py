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

import os
import threading
import unittest


import mox

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
    self.conn = datastore.Datastore('foo', host='https://example.com')

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
        'https://example.com/datastore/v1beta2/datasets/foo/lookup',
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
        'https://example.com/datastore/v1beta2/datasets/foo/lookup',
        method='POST',
        body=payload,
        headers={'Content-Type': 'application/x-protobuf',
                 'Content-Length': str(len(payload))}).AndReturn(
                     (TestResponse(status=500, reason='Internal Error'),
                      'failure message'))
    self.mox.ReplayAll()

    self.assertRaises(datastore.RPCError, self.conn.lookup, request)
    self.mox.VerifyAll()

  def testRunQuery(self):
    request = datastore.RunQueryRequest()
    request.query.kind.add().name = 'Foo'
    payload = request.SerializeToString()
    response = datastore.RunQueryResponse()
    self.expectRequest(
        'https://example.com/datastore/v1beta2/datasets/foo/runQuery',
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
        'https://example.com/datastore/v1beta2/datasets/foo/'
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
        'https://example.com/datastore/v1beta2/datasets/foo/commit',
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
        'https://example.com/datastore/v1beta2/datasets/foo/rollback',
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
        'https://example.com/datastore/v1beta2/datasets/foo/allocateIds',
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
    self.conn = datastore.Datastore(dataset='foo')
    request = self.makeLookupRequest()
    payload = request.SerializeToString()
    response = self.makeLookupResponse()
    self.expectRequest(
        'https://www.googleapis.com/datastore/v1beta2/datasets/foo/lookup',
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
    other_thread_conn = []
    lock1 = threading.Lock()
    lock2 = threading.Lock()
    lock1.acquire()
    lock2.acquire()
    def target():
      # Grab two connections
      other_thread_conn.append(datastore.get_default_connection())
      other_thread_conn.append(datastore.get_default_connection())
      lock1.release()  # Notify that we have grabbed the first 2 connections.
      lock2.acquire()  # Wait for the signal to grab the 3rd.
      other_thread_conn.append(datastore.get_default_connection())
    other_thread = threading.Thread(target=target)

    # Resetting options and state.
    datastore._options = {}
    datastore.set_options(dataset='foo')

    self.mox.StubOutWithMock(os, 'getenv')
    self.mox.StubOutWithMock(helper, 'get_credentials_from_env')
    os.getenv('DATASTORE_HOST').AndReturn('http://localhost:8080')
    os.getenv('DATASTORE_URL_INTERNAL_OVERRIDE').AndReturn(None)
    os.getenv('DATASTORE_URL_INTERNAL_OVERRIDE').AndReturn(None)
    os.getenv('DATASTORE_URL_INTERNAL_OVERRIDE').AndReturn(None)
    os.getenv('DATASTORE_URL_INTERNAL_OVERRIDE').AndReturn(None)

    helper.get_credentials_from_env().AndReturn(FakeCredentialsFromEnv())
    self.mox.ReplayAll()

    # Start the thread and wait for the first lock.
    other_thread.start()
    lock1.acquire()

    t1_conn1 = datastore.get_default_connection()
    t2_conn1, t2_conn1b = other_thread_conn
    other_thread_conn = []
    # The two threads get different connections.
    self.assertIsNot(t1_conn1, t2_conn1)
    # Multiple calls on the same thread get the same connection.
    self.assertIs(t1_conn1, datastore.get_default_connection())
    self.assertIs(t2_conn1, t2_conn1b)

    # Change the global options and grab the connections again.
    datastore.set_options(dataset='bar')
    lock2.release()
    other_thread.join()
    t1_conn2 = datastore.get_default_connection()
    t2_conn2 = other_thread_conn[0]

    # Changing the options causes all threads to create new connections.
    self.assertIsNot(t1_conn1, t1_conn2)
    self.assertIsNot(t2_conn1, t2_conn2)
    # The new connections are still different for each thread.
    self.assertIsNot(t1_conn2, t2_conn2)
    # The old connections has the old settings.
    self.assertEqual('http://localhost:8080/datastore/v1beta2/datasets/foo/',
                     t1_conn1._url)
    self.assertEqual('http://localhost:8080/datastore/v1beta2/datasets/foo/',
                     t2_conn1._url)
    # The new connections has the new settings.
    self.assertEqual('http://localhost:8080/datastore/v1beta2/datasets/bar/',
                     t1_conn2._url)
    self.assertEqual('http://localhost:8080/datastore/v1beta2/datasets/bar/',
                     t2_conn2._url)
    self.assertEqual(FakeCredentialsFromEnv, type(t1_conn2._credentials))
    self.assertEqual(FakeCredentialsFromEnv, type(t2_conn2._credentials))
    self.mox.VerifyAll()

  def testSetUrlOverride(self):
    self.mox.StubOutWithMock(os, 'getenv')
    os.getenv('DATASTORE_URL_INTERNAL_OVERRIDE').AndReturn(
        'http://prom-qa/datastore/v1beta42')
    self.mox.ReplayAll()

    datastore.set_options(host='http://example.com', dataset='bar')
    conn = datastore.get_default_connection()
    self.assertEqual('http://prom-qa/datastore/v1beta42/datasets/bar/',
                     conn._url)
    self.mox.VerifyAll()

  def testFunctions(self):
    datastore.set_options(dataset='foo')
    def caml(s): return ''.join(p[0].upper()+p[1:] for p in s.split('_'))
    rpcs = ['lookup', 'run_query', 'begin_transaction',
            'commit', 'rollback', 'allocate_ids']
    methods = [(r, getattr(datastore, caml(r)+'Request'),
                getattr(datastore, caml(r)+'Response'))
               for r in rpcs]
    conn = datastore.get_default_connection()
    for m, req_class, resp_class in methods:
      self.mox.StubOutWithMock(conn, m)
      method = getattr(conn, m)
      method(mox.IsA(req_class)).AndReturn(resp_class())
    self.mox.ReplayAll()

    for m, req_class, resp_class in methods:
      method = getattr(datastore, m)
      result = method(req_class())
      self.assertEqual(resp_class, type(result))
    self.mox.VerifyAll()

if __name__ == '__main__':
  unittest.main()
