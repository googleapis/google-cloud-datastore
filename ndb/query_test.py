"""Tests for query.py."""

import os
import re
import sys
import time
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_file_stub
from google.appengine.api.memcache import memcache_stub
from google.appengine.datastore import datastore_rpc
from google.appengine.datastore import datastore_query

from ndb import context
from ndb import model
from ndb import query
from ndb import tasklets


class Foo(model.Model):
  name = model.StringProperty()
  rate = model.IntegerProperty()
  tags = model.StringProperty(repeated=True)


class QueryTests(unittest.TestCase):

  def setUp(self):
    os.environ['APPLICATION_ID'] = '_'
    self.set_up_stubs()
    context.set_default_context(context.Context())
    self.create_entities()

  def set_up_stubs(self):
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    ds_stub = datastore_file_stub.DatastoreFileStub('_', None)
    apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', ds_stub)
    mc_stub = memcache_stub.MemcacheServiceStub()
    apiproxy_stub_map.apiproxy.RegisterStub('memcache', mc_stub)

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
    res = list(q)
    self.assertEqual(res, [self.joe, self.moe])

  def testOrderedQuery(self):
    q = query.Query(kind='Foo')
    q = q.order_by('rate').order_by().order_by(('name', query.DESC))
    res = list(q)
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
    self.assertEqual(q.filter, query.FilterNode('rate', '=', 1))
    self.assertEqual(q.order, None)

    q = q.order_by_desc('name')
    self.assertEqual(q.kind, 'Foo')
    self.assertEqual(q.ancestor, key)
    self.assertEqual(q.filter, query.FilterNode('rate', '=', 1))
    expected_order = [('name', query.DESC)]
    self.assertEqual(q.order, expected_order)

  def testMultiQuery(self):
    q1 = query.Query(kind='Foo').where(tags__eq='jill').order_by('name')
    q2 = query.Query(kind='Foo').where(tags='joe').order_by('name')
    qq = query.MultiQuery([q1, q2], [('name', query.ASC)])
    res = list(qq)
    self.assertEqual(res, [self.jill, self.joe])

  def testLooper(self):
    q = query.Query(kind='Foo').where(tags__eq='jill').order_by('name')
    @context.synctasklet
    def foo():
      it = iter(q)
      res = []
      while (yield it.has_next_async()):
        val = it.next()
        res.append(val)
      self.assertEqual(res, [self.jill, self.joe])
    foo()

  def testMultiQueryIterator(self):
    q = query.Query(kind='Foo').where(tags__in=['joe', 'jill'])
    q = q.order_by('name')
    @context.synctasklet
    def foo():
      it = iter(q)
      res = []
      while (yield it.has_next_async()):
        val = it.next()
        res.append(val)
      self.assertEqual(res, [self.jill, self.joe])
    foo()

  def testNotEqualOperator(self):
    q = query.Query(kind='Foo').where(rate__ne=2)
    res = list(q)
    self.assertEqual(res, [self.joe, self.moe])

  def testInOperator(self):
    q = query.Query(kind='Foo').where(tags__in=('jill', 'hello'))
    res = list(q)
    self.assertEqual(res, [self.joe, self.jill])

  def testFullDistributiveLaw(self):
    q = query.Query(kind='Foo').where(tags__in=['jill', 'hello'])
    q = q.where(rate__in=[1, 2])
    DisjunctionNode = query.DisjunctionNode
    ConjunctionNode = query.ConjunctionNode
    FilterNode = query.FilterNode
    expected = DisjunctionNode(
      [ConjunctionNode([FilterNode('tags', '=', 'jill'),
                        FilterNode('rate', '=', 1)]),
       ConjunctionNode([FilterNode('tags', '=', 'jill'),
                        FilterNode('rate', '=', 2)]),
       ConjunctionNode([FilterNode('tags', '=', 'hello'),
                        FilterNode('rate', '=', 1)]),
       ConjunctionNode([FilterNode('tags', '=', 'hello'),
                        FilterNode('rate', '=', 2)])])
    self.assertEqual(q.filter, expected)

  def testHalfDistributiveLaw(self):
    DisjunctionNode = query.DisjunctionNode
    ConjunctionNode = query.ConjunctionNode
    FilterNode = query.FilterNode
    filter = ConjunctionNode(
      [FilterNode('tags', 'in', ['jill', 'hello']),
       ConjunctionNode([FilterNode('rate', '=', 1),
                        FilterNode('name', '=', 'moe')])])
    expected = DisjunctionNode(
      [ConjunctionNode([FilterNode('tags', '=', 'jill'),
                        FilterNode('rate', '=', 1),
                        FilterNode('name', '=', 'moe')]),
       ConjunctionNode([FilterNode('tags', '=', 'hello'),
                        FilterNode('rate', '=', 1),
                        FilterNode('name', '=', 'moe')])])
    self.assertEqual(filter, expected)


def main():
  unittest.main()


if __name__ == '__main__':
  main()
