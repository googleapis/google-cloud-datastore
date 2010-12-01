"""Tests for query.py."""

import os
import re
import sys
import time
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_file_stub

from core import datastore_rpc
from core import datastore_query

from ndb import model
from ndb import query


class Foo(model.Model):
  name = model.StringProperty()


class QueryTests(unittest.TestCase):

  def setUp(self):
    os.environ['APPLICATION_ID'] = '_'
    self.set_up_stubs()
    self.create_entities()

  def set_up_stubs(self):
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    ds_stub = datastore_file_stub.DatastoreFileStub('_', None)
    apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', ds_stub)

  def create_entities(self):
    self.joe = Foo(name='joe')
    self.joe.put()
    self.jill = Foo(name='jill')
    self.jill.put()
    self.moe = Foo(name='moe')
    self.moe.put()

  def testBasicQuery(self):
    q = query.Query(kind='Foo').where(name__ge='joe').where(name__le='moe')
    res = []
    rpc = q.run_async(model.conn)
    while rpc is not None:
      batch = rpc.get_result()
      rpc = batch.next_batch_async()
      res.extend(batch.results)
    self.assertEqual(res, [self.joe, self.moe])

  def testOrderedQuery(self):
    q = query.Query(kind='Foo').order_by(('name', query.ASC))
    res = []
    rpc = q.run_async(model.conn)
    while rpc is not None:
      batch = rpc.get_result()
      rpc = batch.next_batch_async()
      res.extend(batch.results)
    self.assertEqual(res, [self.jill, self.joe, self.moe])


def main():
  unittest.main()


if __name__ == '__main__':
  main()
