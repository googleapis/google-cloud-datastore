"""Tests for query.py."""

import os
import re
import sys
import time
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_file_stub
from google.appengine.datastore import datastore_rpc
from google.appengine.datastore import datastore_query

from ndb import model
from ndb import query


class Foo(model.Model):
  name = model.StringProperty()
  rate = model.IntegerProperty()
  tags = model.StringProperty(repeated=True)


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
    self.joe = Foo(name='joe', tags=['joe', 'jill', 'hello'], rate=1)
    self.joe.put()
    self.jill = Foo(name='jill', tags=['jack', 'jill'], rate=2)
    self.jill.put()
    self.moe = Foo(name='moe', rate=1)
    self.moe.put()

  def testBasicQuery(self):
    q = query.Query(kind='Foo')
    q = q.where(name__ge='joe').where(name__le='moe').where()
    res = []
    rpc = q.run_async(model.conn)
    while rpc is not None:
      batch = rpc.get_result()
      rpc = batch.next_batch_async()
      res.extend(batch.results)
    self.assertEqual(res, [self.joe, self.moe])

  def testOrderedQuery(self):
    q = query.Query(kind='Foo')
    q = q.order_by('rate').order_by().order_by(('name', query.DESC))
    res = []
    rpc = q.run_async(model.conn)
    while rpc is not None:
      batch = rpc.get_result()
      rpc = batch.next_batch_async()
      res.extend(batch.results)
    self.assertEqual(res, [self.moe, self.joe, self.jill])

  def testQueryAttributes(self):
    q = query.Query(kind='Foo')
    self.assertEqual(q.kind, 'Foo')
    self.assertEqual(q.ancestor, None)
    self.assertEqual(q.filter, None)
    self.assertEqual(q.order, None)

    key = model.Key('Barba', 'papa')
    q = query.Query(kind='Foo', ancestor=key)
    self.assertEqual(q.kind, 'Foo')
    self.assertEqual(q.ancestor, key)
    self.assertEqual(q.filter, None)
    self.assertEqual(q.order, None)

    q = q.where(rate__eq=1)
    self.assertEqual(q.kind, 'Foo')
    self.assertEqual(q.ancestor, key)
    self.assertEqual(q.filter._to_pb(),
                     datastore_query.make_filter('rate', '=', 1)._to_pb())
    self.assertEqual(q.order, None)

    q = q.order_by(('name', query.DESC))
    self.assertEqual(q.kind, 'Foo')
    self.assertEqual(q.ancestor, key)
    self.assertEqual(q.filter._to_pb(),
                     datastore_query.make_filter('rate', '=', 1)._to_pb())
    order_pbs = q.order._to_pbs()
    expected_pbs = [datastore_query.PropertyOrder('name', query.DESC)._to_pb()]
    self.assertEqual(order_pbs, expected_pbs)

  def testMultiQuery(self):
    q1 = query.Query(kind='Foo').where(tags__eq='jill').order_by('name')
    q2 = query.Query(kind='Foo').where(tags__eq='joe').order_by('name')
    qq = query.MultiQuery([q1, q2], [('name', query.ASC)])
    res = list(qq.iterate(model.conn))
    self.assertEqual(res, [self.jill, self.joe])


def main():
  unittest.main()


if __name__ == '__main__':
  main()
