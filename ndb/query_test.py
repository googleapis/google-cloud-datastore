"""Tests for query.py."""

import os
import re
import sys
import time
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_errors
from google.appengine.api import datastore_file_stub
from google.appengine.api.memcache import memcache_stub
from google.appengine.datastore import datastore_rpc
from google.appengine.datastore import datastore_query

from ndb import context
from ndb import model
from ndb import query
from ndb import tasklets
from ndb import test_utils


class QueryTests(test_utils.DatastoreTest):

  def setUp(self):
    super(QueryTests, self).setUp()
    tasklets.set_context(context.Context())

    # Create class inside tests because kinds are cleared every test.
    global Foo
    class Foo(model.Model):
      name = model.StringProperty()
      rate = model.IntegerProperty()
      tags = model.StringProperty(repeated=True)
    self.create_entities()

  def create_entities(self):
    self.joe = Foo(name='joe', tags=['joe', 'jill', 'hello'], rate=1)
    self.joe.put()
    self.jill = Foo(name='jill', tags=['jack', 'jill'], rate=2)
    self.jill.put()
    self.moe = Foo(name='moe', rate=1)
    self.moe.put()

  def testBasicQuery(self):
    q = query.Query(kind='Foo')
    q = q.filter(Foo.name >= 'joe').filter(Foo.name <= 'moe').filter()
    res = list(q)
    self.assertEqual(res, [self.joe, self.moe])

  def testOrderedQuery(self):
    q = query.Query(kind='Foo')
    q = q.order(Foo.rate).order().order(-Foo.name)
    res = list(q)
    self.assertEqual(res, [self.moe, self.joe, self.jill])

  def testQueryAttributes(self):
    q = query.Query(kind='Foo')
    self.assertEqual(q.kind, 'Foo')
    self.assertEqual(q.ancestor, None)
    self.assertEqual(q.filters, None)
    self.assertEqual(q.orders, None)

    key = model.Key('Barba', 'papa')
    q = query.Query(kind='Foo', ancestor=key)
    self.assertEqual(q.kind, 'Foo')
    self.assertEqual(q.ancestor, key)
    self.assertEqual(q.filters, None)
    self.assertEqual(q.orders, None)

    q = q.filter(Foo.rate == 1)
    self.assertEqual(q.kind, 'Foo')
    self.assertEqual(q.ancestor, key)
    self.assertEqual(q.filters, query.FilterNode('rate', '=', 1))
    self.assertEqual(q.orders, None)

    q = q.order(-Foo.name)
    self.assertEqual(q.kind, 'Foo')
    self.assertEqual(q.ancestor, key)
    self.assertEqual(q.filters, query.FilterNode('rate', '=', 1))
    expected_order = [('name', query.DESC)]
    self.assertEqual(query.orders_to_orderings(q.orders), expected_order)

  def testQueryRepr(self):
    q = Foo.query()
    self.assertEqual(repr(q), "Query(kind='Foo')")
    q = Foo.query(ancestor=model.Key('Bar', 1))
    self.assertEqual(repr(q), "Query(kind='Foo', ancestor=Key('Bar', 1))")
    # Let's not specify what it should show for filters and orders,
    # just test that it doesn't blow up.
    q1 = q.filter(Foo.rate == 1, Foo.name == 'x')
    q2 = q1.order(-Foo.rate)

  def testModernQuerySyntax(self):
    class Employee(model.Model):
      name = model.StringProperty()
      age = model.IntegerProperty('Age')
      rank = model.IntegerProperty()
      @classmethod
      def seniors(cls, min_age, min_rank):
        q = cls.query().filter(cls.age >= min_age, cls.rank <= min_rank)
        q = q.order(cls.name, -cls.age)
        return q
    q = Employee.seniors(42, 5)
    self.assertEqual(q.filters,
                     query.ConjunctionNode(
                       [query.FilterNode('Age', '>=', 42),
                        query.FilterNode('rank', '<=', 5)]))
    self.assertEqual(query.orders_to_orderings(q.orders),
                     [('name', query.ASC), ('Age', query.DESC)])

  def testAndQuery(self):
    class Employee(model.Model):
      name = model.StringProperty()
      age = model.IntegerProperty('Age')
      rank = model.IntegerProperty()
    q = Employee.query().filter(query.AND(Employee.age >= 42))
    self.assertEqual(q.filters, query.FilterNode('Age', '>=', 42))
    q = Employee.query(query.AND(Employee.age >= 42, Employee.rank <= 5))
    self.assertEqual(q.filters,
                     query.ConjunctionNode(
                       [query.FilterNode('Age', '>=', 42),
                        query.FilterNode('rank', '<=', 5)]))

  def testOrQuery(self):
    class Employee(model.Model):
      name = model.StringProperty()
      age = model.IntegerProperty('Age')
      rank = model.IntegerProperty()
    q = Employee.query().filter(query.OR(Employee.age >= 42))
    self.assertEqual(q.filters, query.FilterNode('Age', '>=', 42))
    q = Employee.query(query.OR(Employee.age < 42, Employee.rank > 5))
    self.assertEqual(q.filters,
                     query.DisjunctionNode(
                       [query.FilterNode('Age', '<', 42),
                        query.FilterNode('rank', '>', 5)]))

  def testQueryForStructuredProperty(self):
    class Bar(model.Model):
      name = model.StringProperty()
      foo = model.StructuredProperty(Foo)
    b1 = Bar(name='b1', foo=Foo(name='nest', rate=1, tags=['tag1', 'tag2']))
    b1.put()
    b2 = Bar(name='b2', foo=Foo(name='best', rate=2, tags=['tag2', 'tag3']))
    b2.put()
    b3 = Bar(name='b3', foo=Foo(name='rest', rate=2, tags=['tag2']))
    b3.put()
    q1 = Bar.query().order(Bar.name)
    self.assertEqual(q1.fetch(10), [b1, b2, b3])
    q2 = Bar.query().filter(Bar.foo.rate >= 2)
    self.assertEqual(q2.fetch(10), [b2, b3])
    q3 = q2.order(Bar.foo.rate, -Bar.foo.name, +Bar.foo.rate)
    self.assertEqual(q3.fetch(10), [b3, b2])

  def testQueryForNestedStructuredProperty(self):
    class Bar(model.Model):
      name = model.StringProperty()
      foo = model.StructuredProperty(Foo)
    class Bak(model.Model):
      bar = model.StructuredProperty(Bar)
    class Baz(model.Model):
      bar = model.StructuredProperty(Bar)
      bak = model.StructuredProperty(Bak)
      rank = model.IntegerProperty()
    b1 = Baz(bar=Bar(foo=Foo(name='a')))
    b1.put()
    b2 = Baz(bar=Bar(foo=Foo(name='b')), bak=Bak(bar=Bar(foo=Foo(name='c'))))
    b2.put()
    q1 = Baz.query().filter(Baz.bar.foo.name >= 'a')
    self.assertEqual(q1.fetch(10), [b1, b2])
    q2 = Baz.query().filter(Baz.bak.bar.foo.name >= 'a')
    self.assertEqual(q2.fetch(10), [b2])

  def testQueryForWholeStructure(self):
    class Employee(model.Model):
      name = model.StringProperty()
      rank = model.IntegerProperty()
    class Manager(Employee):
      report = model.StructuredProperty(Employee, repeated=True)
    reports_a = []
    for i in range(3):
      e = Employee(name=str(i), rank=i)
      e.put()
      reports_a.append(e)
    reports_b = []
    for i in range(3, 6):
      e = Employee(name=str(i), rank=0)
      e.put()
      reports_b.append(e)
    mgr_a = Manager(name='a', report=reports_a)
    mgr_a.put()
    mgr_b = Manager(name='b', report=reports_b)
    mgr_b.put()
    mgr_c = Manager(name='c', report=reports_a + reports_b)
    mgr_c.put()
    res = list(Manager.query(Manager.report == Employee(name='1', rank=1)))
    self.assertEqual(res, [mgr_a, mgr_c])
    res = list(Manager.query(Manager.report == Employee(rank=0)))
    self.assertEqual(res, [mgr_a, mgr_b, mgr_c])
    res = list(Manager.query(Manager.report == Employee(rank=0, name='3')))
    self.assertEqual(res, [mgr_b, mgr_c])
    res = list(Manager.query(Manager.report == Employee(rank=0, name='1')))
    self.assertEqual(res, [])
    res = list(Manager.query(Manager.report == Employee(rank=0, name='0'),
                             Manager.report == Employee(rank=1, name='1')))
    self.assertEqual(res, [mgr_a, mgr_c])
    q = Manager.query(Manager.report == Employee(rank=2, name='2'))
    res = list(q)
    self.assertEqual(res, [mgr_a, mgr_c])
    res = list(q.iter(options=query.QueryOptions(offset=1)))
    self.assertEqual(res, [mgr_c])
    res = list(q.iter(options=query.QueryOptions(limit=1)))
    self.assertEqual(res, [mgr_a])

  def testMultiQuery(self):
    q1 = query.Query(kind='Foo').filter(Foo.tags == 'jill').order(Foo.name)
    q2 = query.Query(kind='Foo').filter(Foo.tags == 'joe').order(Foo.name)
    qq = query.MultiQuery([q1, q2],
                          query.ordering_to_order(('name', query.ASC)))
    res = list(qq)
    self.assertEqual(res, [self.jill, self.joe])

  def testIterAsync(self):
    q = query.Query(kind='Foo').filter(Foo.tags == 'jill').order(Foo.name)
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
    q = query.Query(kind='Foo').filter(Foo.tags == 'jill').order(Foo.name)
    callback = lambda e: e.name
    @tasklets.tasklet
    def callback_async(e):
      yield tasklets.sleep(0.01)
      raise tasklets.Return(e.name)
    self.assertEqual(q.map(callback), ['jill', 'joe'])
    self.assertEqual(q.map(callback_async), ['jill', 'joe'])

  def testMapAsync(self):
    q = query.Query(kind='Foo').filter(Foo.tags == 'jill').order(Foo.name)
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
    q = query.Query(kind='Foo').filter(Foo.tags == 'jill').order(Foo.name)
    self.assertEqual(q.fetch(10), [self.jill, self.joe])
    self.assertEqual(q.fetch(1), [self.jill])

  def testFetchAsync(self):
    q = query.Query(kind='Foo').filter(Foo.tags == 'jill').order(Foo.name)
    @tasklets.synctasklet
    def foo():
      res = yield q.fetch_async(10)
      self.assertEqual(res, [self.jill, self.joe])
      res = yield q.fetch_async(1)
      self.assertEqual(res, [self.jill])
    foo()

  def testFetchEmpty(self):
    q = query.Query(kind='Foo').filter(Foo.tags == 'jillian')
    self.assertEqual(q.fetch(1), [])

  def testFetchKeysOnly(self):
    qo = query.QueryOptions(keys_only=True)
    q = query.Query(kind='Foo').filter(Foo.tags == 'jill').order(Foo.name)
    self.assertEqual(q.fetch(10, options=qo), [self.jill.key, self.joe.key])

  def testGet(self):
    q = query.Query(kind='Foo').filter(Foo.tags == 'jill').order(Foo.name)
    self.assertEqual(q.get(), self.jill)

  def testGetEmpty(self):
    q = query.Query(kind='Foo').filter(Foo.tags == 'jillian')
    self.assertEqual(q.get(), None)

  def testGetKeysOnly(self):
    qo = query.QueryOptions(keys_only=True)
    q = query.Query(kind='Foo').filter(Foo.tags == 'jill').order(Foo.name)
    self.assertEqual(q.get(options=qo), self.jill.key)

  def testCursors(self):
    qo = query.QueryOptions(produce_cursors=True)
    q = query.Query(kind='Foo')
    it = q.iter(options=qo)
    expected = [self.joe, self.jill, self.moe]
    self.assertRaises(datastore_errors.BadArgumentError, it.cursor_before)
    self.assertRaises(datastore_errors.BadArgumentError, it.cursor_after)
    before = []
    after = []
    for i, ent in enumerate(it):
      self.assertEqual(ent, expected[i])
      before.append(it.cursor_before())
      after.append(it.cursor_after())
    before.append(it.cursor_before())
    after.append(it.cursor_after())
    self.assertEqual(before[1], after[0])
    self.assertEqual(before[2], after[1])
    self.assertEqual(before[3], after[2])
    self.assertEqual(before[3], after[3])  # !!!

  def testCursorsKeysOnly(self):
    qo = query.QueryOptions(produce_cursors=True, keys_only=True)
    q = query.Query(kind='Foo')
    it = q.iter(options=qo)
    expected = [self.joe.key, self.jill.key, self.moe.key]
    self.assertRaises(datastore_errors.BadArgumentError, it.cursor_before)
    self.assertRaises(datastore_errors.BadArgumentError, it.cursor_after)
    before = []
    after = []
    for i, ent in enumerate(it):
      self.assertEqual(ent, expected[i])
      before.append(it.cursor_before())
      after.append(it.cursor_after())
    before.append(it.cursor_before())
    after.append(it.cursor_after())
    self.assertEqual(before[1], after[0])
    self.assertEqual(before[2], after[1])
    self.assertEqual(before[3], after[2])
    self.assertEqual(before[3], after[3])  # !!!

  def testCursorsEfficientPaging(self):
    # We want to read a 'page' of data, get the cursor just past the
    # page, and know whether there is another page, all with a single
    # RPC.  To do this, set limit=pagesize+1, batch_size=pagesize.
    q = query.Query(kind='Foo')
    cursors = {}
    mores = {}
    for pagesize in [1, 2, 3, 4]:
      qo = query.QueryOptions(produce_cursors=True, limit=pagesize+1,
                              batch_size=pagesize)
      it = q.iter(options=qo)
      todo = pagesize
      for ent in it:
        todo -= 1
        if todo <= 0:
          break
      cursors[pagesize] = it.cursor_after()
      mores[pagesize] = it.probably_has_next()
    self.assertEqual(mores, {1: True, 2: True, 3: False, 4: False})
    self.assertEqual(cursors[3], cursors[4])
    # TODO: Assert that only one RPC call was made.

  def testCount(self):
    q = query.Query(kind='Foo').filter(Foo.tags == 'jill').order(Foo.name)
    self.assertEqual(q.count(10), 2)
    self.assertEqual(q.count(1), 1)

  def testCountAsync(self):
    q = query.Query(kind='Foo').filter(Foo.tags == 'jill').order(Foo.name)
    @tasklets.synctasklet
    def foo():
      res = yield q.count_async(10)
      self.assertEqual(res, 2)
      res = yield q.count_async(1)
      self.assertEqual(res, 1)
    foo()

  def testCountEmpty(self):
    q = query.Query(kind='Foo').filter(Foo.tags == 'jillian')
    self.assertEqual(q.count(1), 0)

  def testMultiQueryIterator(self):
    q = query.Query(kind='Foo').filter(Foo.tags.IN(['joe', 'jill']))
    q = q.order(Foo.name)
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
    q = query.Query(kind='Foo').filter(Foo.rate != 2)
    res = list(q)
    self.assertEqual(res, [self.joe, self.moe])

  def testInOperator(self):
    q = query.Query(kind='Foo').filter(Foo.tags.IN(('jill', 'hello')))
    res = list(q)
    self.assertEqual(res, [self.joe, self.jill])

  def testFullDistributiveLaw(self):
    q = query.Query(kind='Foo').filter(Foo.tags.IN(['jill', 'hello']))
    q = q.filter(Foo.rate.IN([1, 2]))
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
    self.assertEqual(q.filters, expected)

  def testHalfDistributiveLaw(self):
    DisjunctionNode = query.DisjunctionNode
    ConjunctionNode = query.ConjunctionNode
    FilterNode = query.FilterNode
    filters = ConjunctionNode(
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
    self.assertEqual(filters, expected)

  def testGqlMinimal(self):
    qry, options, bindings = query.parse_gql('SELECT * FROM Kind')
    self.assertEqual(qry.kind, 'Kind')
    self.assertEqual(qry.ancestor, None)
    self.assertEqual(qry.filters, None)
    self.assertEqual(qry.orders, None)
    self.assertEqual(bindings, {})

  def testGqlAncestor(self):
    qry, options, bindings = query.parse_gql(
      'SELECT * FROM Kind WHERE ANCESTOR IS :1')
    self.assertEqual(qry.kind, 'Kind')
    self.assertEqual(qry.ancestor, query.Binding(None, 1))
    self.assertEqual(qry.filters, None)
    self.assertEqual(qry.orders, None)
    self.assertEqual(bindings, {1: query.Binding(None, 1)})

  def testGqlAncestor(self):
    key = model.Key('Foo', 42)
    qry, options, bindings = query.parse_gql(
      "SELECT * FROM Kind WHERE ANCESTOR IS KEY('%s')" % key.urlsafe())
    self.assertEqual(qry.kind, 'Kind')
    self.assertEqual(qry.ancestor, key)
    self.assertEqual(qry.filters, None)
    self.assertEqual(qry.orders, None)
    self.assertEqual(bindings, {})

  def testGqlFilter(self):
    qry, options, bindings = query.parse_gql(
      "SELECT * FROM Kind WHERE prop1 = 1 AND prop2 = 'a'")
    self.assertEqual(qry.kind, 'Kind')
    self.assertEqual(qry.ancestor, None)
    self.assertEqual(qry.filters,
                     query.ConjunctionNode(
                       [query.FilterNode('prop1', '=', 1),
                        query.FilterNode('prop2', '=', 'a')]))
    self.assertEqual(qry.orders, None)
    self.assertEqual(bindings, {})

  def testGqlOrder(self):
    qry, options, bindings = query.parse_gql(
      'SELECT * FROM Kind ORDER BY prop1')
    self.assertEqual(query.orders_to_orderings(qry.orders),
                     [('prop1', query.ASC)])

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
    self.assertEqual(qry.filters,
                     query.ConjunctionNode(
                       [query.FilterNode('prop1', '=',
                                         query.Binding(None, 1)),
                        query.FilterNode('prop2', '=',
                                         query.Binding(None, 'foo'))]))
    self.assertEqual(qry.orders, None)
    self.assertEqual(bindings, {1: query.Binding(None, 1),
                                'foo': query.Binding(None, 'foo')})

  def testResolveBindings(self):
    qry, options, bindings = query.parse_gql(
      'SELECT * FROM Foo WHERE name = :1')
    bindings[1].value = 'joe'
    self.assertEqual(list(qry), [self.joe])
    bindings[1].value = 'jill'
    self.assertEqual(list(qry), [self.jill])

  def testKeyFilter(self):
    class MyModel(model.Model):
      number = model.IntegerProperty()

    k1 = model.Key('MyModel', 'foo-1')
    m1 = MyModel(key=k1)
    m1.put()

    k2 = model.Key('MyModel', 'foo-2')
    m2 = MyModel(key=k2)
    m2.put()

    q = MyModel.query(MyModel.key == k1)
    res = q.get()
    self.assertEqual(res, m1)

    q = MyModel.query(MyModel.key > k1)
    res = q.get()
    self.assertEqual(res, m2)

    q = MyModel.query(MyModel.key < k2)
    res = q.get()
    self.assertEqual(res, m1)

def main():
  unittest.main()


if __name__ == '__main__':
  main()
