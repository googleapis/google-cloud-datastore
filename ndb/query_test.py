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
    tasklets.set_context(context.Context())
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

  def testModernQuerySyntax(self):
    class Employee(model.Model):
      name = model.StringProperty()
      age = model.IntegerProperty('Age')
      rank = model.IntegerProperty()
      @classmethod
      def seniors(cls, min_age, min_rank):
        return cls.query().where(cls.age >= min_age, cls.rank >= min_rank)
    q = Employee.seniors(42, 5)
    self.assertEqual(q.filter,
                     query.ConjunctionNode(
                       [query.FilterNode('Age', '>=', 42),
                        query.FilterNode('rank', '>=', 5)]))

  def testMultiQuery(self):
    q1 = query.Query(kind='Foo').where(tags__eq='jill').order_by('name')
    q2 = query.Query(kind='Foo').where(tags='joe').order_by('name')
    qq = query.MultiQuery([q1, q2], [('name', query.ASC)])
    res = list(qq)
    self.assertEqual(res, [self.jill, self.joe])

  def testIterAsync(self):
    q = query.Query(kind='Foo').where(tags__eq='jill').order_by('name')
    @tasklets.synctasklet
    def foo():
      it = iter(q)
      res = []
      while (yield it.has_next_async()):
        val = it.next()
        res.append(val)
      self.assertEqual(res, [self.jill, self.joe])
    foo()

  def testMap(self):
    q = query.Query(kind='Foo').where(tags__eq='jill').order_by('name')
    callback = lambda e: e.name
    @tasklets.tasklet
    def callback_async(e):
      yield tasklets.sleep(0.01)
      raise tasklets.Return(e.name)
    self.assertEqual(q.map(callback), ['jill', 'joe'])
    self.assertEqual(q.map(callback_async), ['jill', 'joe'])

  def testMapAsync(self):
    q = query.Query(kind='Foo').where(tags__eq='jill').order_by('name')
    callback = lambda e: e.name
    @tasklets.tasklet
    def callback_async(e):
      yield tasklets.sleep(0.01)
      raise tasklets.Return(e.name)
    @tasklets.synctasklet
    def foo():
      fut = q.map_async(callback)
      res = yield fut
      self.assertEqual(res, ['jill', 'joe'])
      fut = q.map_async(callback_async)
      res = yield fut
      self.assertEqual(res, ['jill', 'joe'])
    foo()

  def testFetch(self):
    q = query.Query(kind='Foo').where(tags__eq='jill').order_by('name')
    self.assertEqual(q.fetch(10), [self.jill, self.joe])
    self.assertEqual(q.fetch(1), [self.jill])

  def testFetchAsync(self):
    q = query.Query(kind='Foo').where(tags__eq='jill').order_by('name')
    @tasklets.synctasklet
    def foo():
      res = yield q.fetch_async(10)
      self.assertEqual(res, [self.jill, self.joe])
      res = yield q.fetch_async(1)
      self.assertEqual(res, [self.jill])
    foo()

  def testFetchEmpty(self):
    q = query.Query(kind='Foo').where(tags__eq='jillian')
    self.assertEqual(q.fetch(1), [])

  def testCount(self):
    q = query.Query(kind='Foo').where(tags__eq='jill').order_by('name')
    self.assertEqual(q.count(10), 2)
    self.assertEqual(q.count(1), 1)

  def testCountAsync(self):
    q = query.Query(kind='Foo').where(tags__eq='jill').order_by('name')
    @tasklets.synctasklet
    def foo():
      res = yield q.count_async(10)
      self.assertEqual(res, 2)
      res = yield q.count_async(1)
      self.assertEqual(res, 1)
    foo()

  def testCountEmpty(self):
    q = query.Query(kind='Foo').where(tags__eq='jillian')
    self.assertEqual(q.count(1), 0)

  def testMultiQueryIterator(self):
    q = query.Query(kind='Foo').where(tags__in=['joe', 'jill'])
    q = q.order_by('name')
    @tasklets.synctasklet
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

  def testGqlMinimal(self):
    qry, options, bindings = query.parse_gql('SELECT * FROM Kind')
    self.assertEqual(qry.kind, 'Kind')
    self.assertEqual(qry.ancestor, None)
    self.assertEqual(qry.filter, None)
    self.assertEqual(qry.order, None)
    self.assertEqual(bindings, {})

  def testGqlAncestor(self):
    qry, options, bindings = query.parse_gql(
      'SELECT * FROM Kind WHERE ANCESTOR IS :1')
    self.assertEqual(qry.kind, 'Kind')
    self.assertEqual(qry.ancestor, query.Binding(None, 1))
    self.assertEqual(qry.filter, None)
    self.assertEqual(qry.order, None)
    self.assertEqual(bindings, {1: query.Binding(None, 1)})

  def testGqlAncestor(self):
    key = model.Key('Foo', 42)
    qry, options, bindings = query.parse_gql(
      "SELECT * FROM Kind WHERE ANCESTOR IS KEY('%s')" % key.urlsafe())
    self.assertEqual(qry.kind, 'Kind')
    self.assertEqual(qry.ancestor, key)
    self.assertEqual(qry.filter, None)
    self.assertEqual(qry.order, None)
    self.assertEqual(bindings, {})

  def testGqlFilter(self):
    qry, options, bindings = query.parse_gql(
      "SELECT * FROM Kind WHERE prop1 = 1 AND prop2 = 'a'")
    self.assertEqual(qry.kind, 'Kind')
    self.assertEqual(qry.ancestor, None)
    self.assertEqual(qry.filter,
                     query.ConjunctionNode(
                       [query.FilterNode('prop1', '=', 1),
                        query.FilterNode('prop2', '=', 'a')]))
    self.assertEqual(qry.order, None)
    self.assertEqual(bindings, {})

  def testGqlOrder(self):
    qry, options, bindings = query.parse_gql(
      'SELECT * FROM Kind ORDER BY prop1')
    self.assertEqual(qry.order, [('prop1', query.ASC)])

  def testGqlOffset(self):
    qry, options, bindings = query.parse_gql(
      'SELECT * FROM Kind OFFSET 2')
    self.assertEqual(options.offset, 2)

  def testGqlLimit(self):
    qry, options, bindings = query.parse_gql(
      'SELECT * FROM Kind LIMIT 2')
    self.assertEqual(options.limit, 2)

  def testGqlBindings(self):
    qry, options, bindings = query.parse_gql(
      'SELECT * FROM Kind WHERE prop1 = :1 AND prop2 = :foo')
    self.assertEqual(qry.kind, 'Kind')
    self.assertEqual(qry.ancestor, None)
    self.assertEqual(qry.filter,
                     query.ConjunctionNode(
                       [query.FilterNode('prop1', '=',
                                         query.Binding(None, 1)),
                        query.FilterNode('prop2', '=',
                                         query.Binding(None, 'foo'))]))
    self.assertEqual(qry.order, None)
    self.assertEqual(bindings, {1: query.Binding(None, 1),
                                'foo': query.Binding(None, 'foo')})

  def testResolveBindings(self):
    qry, options, bindings = query.parse_gql(
      'SELECT * FROM Foo WHERE name = :1')
    bindings[1].value = 'joe'
    self.assertEqual(list(qry), [self.joe])
    bindings[1].value = 'jill'
    self.assertEqual(list(qry), [self.jill])


def main():
  unittest.main()


if __name__ == '__main__':
  main()
