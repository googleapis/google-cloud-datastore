#
# Copyright 2008 The ndb Authors. All Rights Reserved.
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

"""Tests for query.py."""

import datetime
import pickle

from .google_imports import datastore_errors
from .google_imports import datastore_pbs
from .google_imports import datastore_rpc
from .google_imports import namespace_manager
from .google_imports import users
from .google_test_imports import datastore_stub_util
from .google_test_imports import real_unittest
from .google_test_imports import unittest

from . import model
from . import query
from . import tasklets
from . import test_utils


class BaseQueryTestMixin(object):

  def setUp(self):
    # Create class inside tests because kinds are cleared every test.
    global Foo

    class Foo(model.Model):
      name = model.StringProperty()
      rate = model.IntegerProperty()
      tags = model.StringProperty(repeated=True)
    self.create_entities()

  the_module = query

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

  def testQueryError(self):
    self.assertRaises(TypeError, query.Query,
                      ancestor=query.ParameterizedFunction('user',
                                                           query.Parameter(1)))
    self.assertRaises(TypeError, query.Query, ancestor=42)
    self.assertRaises(ValueError, query.Query, ancestor=model.Key('X', None))
    self.assertRaises(TypeError, query.Query,
                      ancestor=model.Key('X', 1), app='another')
    self.assertRaises(TypeError, query.Query,
                      ancestor=model.Key('X', 1), namespace='another')
    self.assertRaises(TypeError, query.Query, filters=42)
    self.assertRaises(TypeError, query.Query, orders=42)
    self.assertRaises(TypeError, query.Query, default_options=42)

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
    expected_order = [('name', query._DESC)]
    self.assertEqual(query._orders_to_orderings(q.orders), expected_order)

  def testQueryRepr(self):
    q = Foo.query()
    self.assertEqual(repr(q), "Query(kind='Foo')")
    q = Foo.query(ancestor=model.Key('Bar', 1))
    self.assertEqual(repr(q), "Query(kind='Foo', ancestor=Key('Bar', 1))")
    # Let's not specify what it should show for filters and orders,
    # just test that it doesn't blow up.
    q1 = q.filter(Foo.rate == 1, Foo.name == 'x')
    repr(q1)
    q2 = q1.order(-Foo.rate)
    repr(q2)
    # App and namespace.
    q3 = Foo.query(app='a', namespace='ns')
    self.assertEqual(repr(q3), "Query(app='a', namespace='ns', kind='Foo')")
    # default_options.
    q4 = Foo.query(default_options=query.QueryOptions(limit=3))
    self.assertEqual(
        repr(q4),
        "Query(kind='Foo', default_options=QueryOptions(limit=3))")
    q5 = Foo.query(projection=[Foo.name, 'tags'], distinct=True)
    self.assertEqual(
        repr(q5),
        "Query(kind='Foo', projection=['name', 'tags'], "
        "group_by=['name', 'tags'])")

  def testRunToQueue(self):
    qry = Foo.query()
    queue = tasklets.MultiFuture()
    qry.run_to_queue(queue, self.conn).check_success()
    results = queue.get_result()
    self.assertEqual(len(results), 3)
    self.assertEqual(results[0][2], self.joe)
    self.assertEqual(results[1][2], self.jill)
    self.assertEqual(results[2][2], self.moe)

  def testRunToQueueError(self):
    self.ExpectWarnings()
    qry = Foo.query(Foo.name > '', Foo.rate > 0)
    queue = tasklets.MultiFuture()
    fut = qry.run_to_queue(queue, self.conn)
    self.assertRaises(datastore_errors.BadRequestError, fut.check_success)
    self.assertRaises(datastore_errors.BadRequestError, queue.check_success)

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
                         query.FilterNode('Age', '>=', 42),
                         query.FilterNode('rank', '<=', 5)))
    self.assertEqual(query._orders_to_orderings(q.orders),
                     [('name', query._ASC), ('Age', query._DESC)])

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
                         query.FilterNode('Age', '>=', 42),
                         query.FilterNode('rank', '<=', 5)))

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
                         query.FilterNode('Age', '<', 42),
                         query.FilterNode('rank', '>', 5)))

  def testEmptyInFilter(self):
    self.ExpectWarnings()

    class Employee(model.Model):
      name = model.StringProperty()
    for arg in [], (), set(), frozenset():
      q = Employee.query(Employee.name.IN(arg))
      self.assertEqual(q.filters, query.FalseNode())
      self.assertNotEqual(q.filters, 42)
      f = iter(q).has_next_async()
      self.assertRaises(datastore_errors.BadQueryError, f.check_success)

  def testSingletonInFilter(self):
    class Employee(model.Model):
      name = model.StringProperty()
    q = Employee.query(Employee.name.IN(['xyzzy']))
    self.assertEqual(q.filters, query.FilterNode('name', '=', 'xyzzy'))
    self.assertNotEqual(q.filters, 42)
    e = Employee(name='xyzzy')
    e.put()
    self.assertEqual(q.get(), e)

  def testInFilter(self):
    class Employee(model.Model):
      name = model.StringProperty()
    q = Employee.query(Employee.name.IN(['a', 'b']))
    self.assertEqual(q.filters,
                     query.DisjunctionNode(
                         query.FilterNode('name', '=', 'a'),
                         query.FilterNode('name', '=', 'b')))
    a = Employee(name='a')
    a.put()
    b = Employee(name='b')
    b.put()
    self.assertEqual(list(q), [a, b])

  def testInFilterArgTypes(self):
    class Employee(model.Model):
      name = model.StringProperty()
    a = Employee(name='a')
    a.put()
    b = Employee(name='b')
    b.put()
    for arg in ('a', 'b'), set(['a', 'b']), frozenset(['a', 'b']):
      q = Employee.query(Employee.name.IN(arg))
      self.assertEqual(set(x.name for x in q), set(['a', 'b']))

  def testInFilterWithNone(self):
    class Employee(model.Model):
      # Try a few different property types, to get a good mix of what
      # used to fail.
      name = model.StringProperty()
      boss = model.KeyProperty()
      age = model.IntegerProperty()
      date = model.DateProperty()
    a = Employee(name='a', age=42L)
    a.put()
    bosskey = model.Key(Employee, 'x')
    b = Employee(boss=bosskey, date=datetime.date(1996, 1, 31))
    b.put()
    keys = set([a.key, b.key])
    q1 = Employee.query(Employee.name.IN(['a', None]))
    self.assertEqual(set(e.key for e in q1), keys)
    q2 = Employee.query(Employee.boss.IN([bosskey, None]))
    self.assertEqual(set(e.key for e in q2), keys)
    q3 = Employee.query(Employee.age.IN([42, None]))
    self.assertEqual(set(e.key for e in q3), keys)
    q4 = Employee.query(Employee.date.IN([datetime.date(1996, 1, 31), None]))
    self.assertEqual(set(e.key for e in q4), keys)

  def testQueryExceptions(self):
    self.ExpectWarnings()
    q = Foo.query(Foo.name > '', Foo.rate > 0)
    f = q.fetch_async()
    self.assertRaises(datastore_errors.BadRequestError, f.check_success)

  def testQueryUnindexedFails(self):
    # Shouldn't be able to query for unindexed properties
    class SubModel(model.Model):
      booh = model.IntegerProperty(indexed=False)

    class Emp(model.Model):
      name = model.StringProperty()
      text = model.TextProperty()
      blob = model.BlobProperty()
      sub = model.StructuredProperty(SubModel)
      struct = model.StructuredProperty(Foo, indexed=False)
      local = model.LocalStructuredProperty(Foo)
    Emp.query(Emp.name == 'a').fetch()  # Should pass
    self.assertRaises(datastore_errors.BadFilterError,
                      lambda: Emp.text == 'a')
    self.assertRaises(datastore_errors.BadFilterError,
                      lambda: Emp.text.IN(['a', 'b']))
    self.assertRaises(datastore_errors.BadFilterError,
                      lambda: Emp.blob == 'a')
    self.assertRaises(datastore_errors.BadFilterError,
                      lambda: Emp.sub == SubModel(booh=42))
    self.assertRaises(datastore_errors.BadFilterError,
                      lambda: Emp.sub.booh == 42)
    self.assertRaises(datastore_errors.BadFilterError,
                      lambda: Emp.struct == Foo(name='a'))
    # TODO: Make this fail?  See issue 89.  http://goo.gl/K4gbY
    # Currently StructuredProperty(..., indexed=False) has no effect.
    # self.assertRaises(datastore_errors.BadFilterError,
    #                   lambda: Emp.struct.name == 'a')
    self.assertRaises(datastore_errors.BadFilterError,
                      lambda: Emp.local == Foo(name='a'))

  def testConstructor(self):
    self.ExpectWarnings()

    class Foo(model.Model):
      p = model.IntegerProperty('pp')  # Also check renaming.
      q = model.IntegerProperty(required=True)

    key = Foo(p=1, q=2, namespace='ns').put()

    # Check distinct validation
    self.assertRaises(TypeError, Foo.query, distinct=True)
    self.assertRaises(TypeError, Foo.query, distinct=False)
    self.assertRaises(TypeError, Foo.query,
                      distinct=True, projection=Foo.p, group_by=[])
    self.assertRaises(TypeError, Foo.query,
                      distinct=False, projection=Foo.p, group_by=[])

    # Check both projection and default_options.projection/keys_only is not
    # allowed.
    self.assertRaises(TypeError, Foo.query,
                      projection='pp',
                      default_options=query.QueryOptions(projection=['pp']))
    self.assertRaises(TypeError, Foo.query,
                      projection='pp',
                      default_options=query.QueryOptions(keys_only=False))
    # Check empty projection/group_by not allowed.
    for empty in ([], tuple()):
      self.assertRaises(TypeError, Foo.query, projection=empty)
      self.assertRaises(TypeError, Foo.query, group_by=empty)

    # Check that ancestor and namespace must match.
    self.assertRaises(TypeError, Foo.query, namespace='other', ancestor=key)

  def testIsDistinct(self):
    class Foo(model.Model):
      p = model.IntegerProperty('pp')  # Also check renaming.
      q = model.IntegerProperty(required=True)

    for qry in (Foo.query(projection=[Foo.p, 'q'], distinct=True),
                Foo.query(projection=[Foo.p, 'q'],
                          group_by=(Foo.q, 'pp', Foo.p))):
      self.assertEquals(True, qry.is_distinct)

    for qry in (Foo.query(),
                Foo.query(projection=[Foo.p, 'q'])):
      self.assertEquals(False, qry.is_distinct)

  def testIndexOnlyPropertyListNormalization(self):
    class Foo(model.Model):
      p = model.IntegerProperty('pp')  # Also check renaming.

    def assertNormalization(expected, value):
      q1 = Foo.query(group_by=value, projection=value)
      q2 = Foo.query(distinct=True, projection=value)

      # make sure it survives mutation.
      q1 = q1.order(Foo.p).filter(Foo.p > 0)
      q2 = q2.order(Foo.p).filter(Foo.p > 0)
      self.assertEquals(expected, q1.group_by)
      self.assertEquals(expected, q1.projection)
      self.assertEquals(expected, q2.group_by)
      self.assertEquals(expected, q2.projection)

    for value in (('pp',), ['pp']):
      assertNormalization(('pp',), value)

  def testIndexOnlyPropertyValidation(self):
    self.ExpectWarnings()

    class Foo(model.Model):
      p = model.IntegerProperty('pp', indexed=False)  # Also check renaming.
      q = model.IntegerProperty(required=True)

    self.assertRaises(TypeError,
                      Foo.query, group_by=[Foo.q, 42], projection=[Foo.q])
    self.assertRaises(datastore_errors.BadArgumentError,
                      Foo.query().get, projection=[42])
    self.assertRaises(TypeError,
                      Foo.query, group_by=Foo.q, projection=[Foo.q])
    self.assertRaises(TypeError,
                      Foo.query, projection=Foo.q)

    # Legacy support for single value projection
    Foo.query().get(projection=Foo.q)

    for bad in ((Foo.p,), ['wot']):
      self.assertRaises(model.InvalidPropertyError, Foo.query,
                        group_by=bad, projection=[Foo.q])
      self.assertRaises(model.BadProjectionError, Foo.query,
                        group_by=bad, projection=[Foo.q])
      self.assertRaises(model.InvalidPropertyError, Foo.query, projection=bad)
      self.assertRaises(model.BadProjectionError, Foo.query, projection=bad)
      self.assertRaises(model.InvalidPropertyError,
                        Foo.query().get, projection=bad)
      self.assertRaises(model.BadProjectionError,
                        Foo.query().get, projection=bad)

  def testGroupByQuery(self):
    self.ExpectWarnings()

    class Foo(model.Model):
      p = model.IntegerProperty('pp')  # Also check renaming
      q = model.IntegerProperty(required=True)
      r = model.IntegerProperty(repeated=True)
      d = model.IntegerProperty(default=42)

    key1 = Foo(p=1, q=5, r=[3, 4, 5]).put()
    key2 = Foo(p=1, q=4, r=[3, 4]).put()
    key3 = Foo(p=2, q=3, r=[3, 4]).put()
    key4 = Foo(p=2, q=2, r=[3]).put()

    qry = Foo.query(projection=[Foo.p], group_by=[Foo.r, Foo.p])
    qry = qry.order(Foo.p, Foo.r, Foo.q)

    expected = [(1, key2), (1, key2), (1, key1), (2, key4), (2, key3)]

    # Test fetch and iter in base case.
    self.assertEqual(expected, [(ent.p, ent.key) for ent in qry.fetch()])
    self.assertEqual(expected, [(ent.p, ent.key) for ent in qry])

    # Test projection using default options.
    qry = Foo.query(group_by=[Foo.r, Foo.p],
                    default_options=query.QueryOptions(projection=['pp']))
    qry = qry.order(Foo.p, Foo.r, Foo.q)
    self.assertEqual(expected, [(ent.p, ent.key) for ent in qry.fetch()])
    self.assertEqual(expected, [(ent.p, ent.key) for ent in qry])

    # Test projection with other default options.
    qry = Foo.query(projection=[Foo.p], group_by=[Foo.r, Foo.p],
                    default_options=query.QueryOptions(limit=4))
    qry = qry.order(Foo.p, Foo.r, Foo.q)
    self.assertEqual(expected[:4], [(ent.p, ent.key) for ent in qry.fetch()])
    self.assertEqual(expected[:4], [(ent.p, ent.key) for ent in qry])

  def testProjectionQuery(self):
    self.ExpectWarnings()

    class Foo(model.Model):
      p = model.IntegerProperty('pp')  # Also check renaming
      q = model.IntegerProperty(required=True)
      r = model.IntegerProperty(repeated=True)
      d = model.IntegerProperty(default=42)

    key = Foo(p=1, q=2, r=[3, 4]).put()
    q = Foo.query(Foo.p >= 0)
    ent = q.get(projection=[Foo.p, 'q'])
    self.assertItemsEqual(ent._projection, ('pp', 'q'))
    self.assertEqual(ent.p, 1)
    self.assertEqual(ent.q, 2)
    self.assertRaises(model.UnprojectedPropertyError, lambda: ent.r)
    self.assertRaises(model.UnprojectedPropertyError, lambda: ent.d)
    ents = q.fetch(projection=['pp', 'r'])
    ents.sort(key=lambda ent: ent.r)
    self.assertEqual(ents, [Foo(p=1, r=[3], key=key, projection=('pp', 'r')),
                            Foo(p=1, r=[4], key=key, projection=['pp', 'r'])])

  def testProjectionQuery_AllTypes(self):
    class Foo(model.Model):
      abool = model.BooleanProperty()
      aint = model.IntegerProperty()
      afloat = model.FloatProperty()
      astring = model.StringProperty()
      ablob = model.BlobProperty(indexed=True)
      akey = model.KeyProperty()
      auser = model.UserProperty()
      apoint = model.GeoPtProperty()
      adatetime = model.DateTimeProperty()
      adate = model.DateProperty()
      atime = model.TimeProperty()
    boo = Foo(abool=True,
              aint=42,
              afloat=3.14,
              astring='foo',
              ablob='bar',
              akey=model.Key(Foo, 'ref'),
              auser=users.User('test@example.com'),
              apoint=model.GeoPt(52.35, 4.9166667),
              adatetime=datetime.datetime(2012, 5, 1, 8, 19, 42),
              adate=datetime.date(2012, 5, 1),
              atime=datetime.time(8, 19, 42),
             )
    boo.put()
    qry = Foo.query()
    for prop in Foo._properties.itervalues():
      ent = qry.get(projection=[prop._name])
      pb = ent._to_pb()
      decoded_ent = Foo._from_pb(pb, set_key=False)
      self.assertEqual(ent, decoded_ent)
      self.assertEqual(getattr(ent, prop._code_name),
                       getattr(boo, prop._code_name))
      for otherprop in Foo._properties.itervalues():
        if otherprop is not prop:
          try:
            getattr(ent, otherprop._code_name)
            self.fail('Expected an UnprojectedPropertyError for property %s'
                      ' when projecting %s.' % (otherprop, prop))
          except model.UnprojectedPropertyError:
            pass

  def testProjectionQuery_ComputedProperties(self):
    class Foo(model.Model):
      a = model.StringProperty()
      b = model.StringProperty()
      c = model.ComputedProperty(lambda ent: '<%s.%s>' % (ent.a, ent.b))
      d = model.ComputedProperty(lambda ent: '<%s>' % (ent.a,))
    foo = Foo(a='a', b='b')
    foo.put()
    self.assertEqual((foo.a, foo.b, foo.c, foo.d), ('a', 'b', '<a.b>', '<a>'))
    qry = Foo.query()
    x = qry.get(projection=['a', 'b'])
    self.assertEqual((x.a, x.b, x.c, x.d), ('a', 'b', '<a.b>', '<a>'))
    y = qry.get(projection=['a'])
    self.assertEqual((y.a, y.d), ('a', '<a>'))
    self.assertRaises(model.UnprojectedPropertyError, lambda: y.b)
    self.assertRaises(model.UnprojectedPropertyError, lambda: y.c)
    z = qry.get(projection=['b'])
    self.assertEqual((z.b,), ('b',))
    p = qry.get(projection=['c', 'd'])
    self.assertEqual((p.c, p.d), ('<a.b>', '<a>'))

  def testProjectionQuery_StructuredProperties(self):
    class Inner(model.Model):
      foo = model.StringProperty()
      bar = model.StringProperty()
      beh = model.StringProperty()

    class Middle(model.Model):
      baz = model.StringProperty()
      inner = model.StructuredProperty(Inner)
      inners = model.StructuredProperty(Inner, repeated=True)

    class Outer(model.Model):
      name = model.StringProperty()
      middle = model.StructuredProperty(Middle, 'mid')
    one = Outer(name='one',
                middle=Middle(baz='one',
                              inner=Inner(foo='foo', bar='bar'),
                              inners=[Inner(foo='a', bar='b'),
                                      Inner(foo='c', bar='d')]))
    one.put()
    two = Outer(name='two',
                middle=Middle(baz='two',
                              inner=Inner(foo='x', bar='y'),
                              inners=[Inner(foo='p', bar='q')]))
    two.put()
    q = Outer.query()

    x, y = q.fetch(projection=[Outer.name, Outer.middle.baz])
    pb = x._to_pb()
    z = Outer._from_pb(pb, set_key=False)
    self.assertEqual(x, z)
    self.assertEqual(x.middle.baz, 'one')
    self.assertEqual(x.middle._projection, ('baz',))
    self.assertEqual(x,
                     Outer(key=one.key, name='one',
                           middle=Middle(baz='one', projection=['baz']),
                           projection=['mid.baz', 'name']))
    self.assertEqual(y,
                     Outer(key=two.key, name='two',
                           middle=Middle(baz='two', projection=['baz']),
                           projection=['mid.baz', 'name']))
    self.assertRaises(model.UnprojectedPropertyError, lambda: x.middle.inner)
    self.assertRaises(model.ReadonlyPropertyError,
                      setattr, x, 'middle', None)
    self.assertRaises(model.ReadonlyPropertyError,
                      setattr, x, 'middle', x.middle)
    self.assertRaises(model.ReadonlyPropertyError,
                      setattr, x.middle, 'inner', None)
    self.assertRaises(model.ReadonlyPropertyError,
                      setattr, x.middle, 'inner',
                      Inner(foo='', projection=['foo']))

    x = q.get(projection=[Outer.middle.inner.foo, 'mid.inner.bar'])
    self.assertEqual(x.middle.inner.foo, 'foo')
    self.assertItemsEqual(x.middle.inner._projection, ('bar', 'foo'))
    self.assertItemsEqual(x.middle._projection, ('inner.bar', 'inner.foo'))
    self.assertItemsEqual(x._projection, ('mid.inner.bar', 'mid.inner.foo'))
    self.assertEqual(x,
                     Outer(key=one.key,
                           projection=['mid.inner.bar', 'mid.inner.foo'],
                           middle=Middle(projection=['inner.bar', 'inner.foo'],
                                         inner=Inner(projection=['bar', 'foo'],
                                                     foo='foo', bar='bar'))))
    self.assertRaises(model.UnprojectedPropertyError,
                      lambda: x.middle.inner.beh)
    self.assertRaises(model.ReadonlyPropertyError,
                      setattr, x.middle.inner, 'foo', '')
    self.assertRaises(model.ReadonlyPropertyError,
                      setattr, x.middle.inner, 'beh', '')

    xs = q.fetch(projection=[Outer.middle.inners.foo])
    self.assertEqual(xs[0],
                     Outer(key=one.key,
                           middle=Middle(inners=[Inner(foo='a',
                                                       _projection=('foo',))],
                                         _projection=('inners.foo',)),
                           _projection=('mid.inners.foo',)))
    self.assertEqual(len(xs), 3)
    for x, foo in zip(xs, ['a', 'c', 'p']):
      self.assertEqual(len(x.middle.inners), 1)
      self.assertEqual(x.middle.inners[0].foo, foo)

  def testFilterRepr(self):
    class Employee(model.Model):
      name = model.StringProperty()
    f = (Employee.name == 'xyzzy')
    self.assertEqual(repr(f), "FilterNode('name', '=', 'xyzzy')")

  def testNodeComparisons(self):
    a = query.FilterNode('foo', '=', 1)
    b = query.FilterNode('foo', '=', 1)
    c = query.FilterNode('foo', '=', 2)
    d = query.FilterNode('foo', '<', 1)
    # Don't use assertEqual/assertNotEqual; we want to be sure that
    # __eq__ or __ne__ is really called here!
    self.assertTrue(a == b)
    self.assertTrue(a != c)
    self.assertTrue(b != d)
    self.assertRaises(TypeError, lambda: a < b)
    self.assertRaises(TypeError, lambda: a <= b)
    self.assertRaises(TypeError, lambda: a > b)
    self.assertRaises(TypeError, lambda: a >= b)
    x = query.AND(a, b, c)
    y = query.AND(a, b, c)
    z = query.AND(a, d)
    self.assertTrue(x == y)
    self.assertTrue(x != z)

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

  def testQueryForStructuredPropertyErrors(self):
    class Bar(model.Model):
      name = model.StringProperty()
      foo = model.StructuredProperty(Foo)
    # Can't use inequalities.
    self.assertRaises(datastore_errors.BadFilterError,
                      lambda: Bar.foo < Foo())
    self.assertRaises(datastore_errors.BadFilterError,
                      lambda: Bar.foo != Foo())
    # Can't use an empty value.
    self.assertRaises(datastore_errors.BadFilterError,
                      lambda: Bar.foo == Foo())

  def testQueryForStructuredPropertyIn(self):
    self.ExpectWarnings()

    class Bar(model.Model):
      name = model.StringProperty()
      foo = model.StructuredProperty(Foo)
    a = Bar(name='a', foo=Foo(name='a'))
    a.put()
    b = Bar(name='b', foo=Foo(name='b'))
    b.put()
    self.assertEqual(
        Bar.query(Bar.foo.IN((Foo(name='a'), Foo(name='b')))).fetch(),
        [a, b])
    self.assertEqual(Bar.query(Bar.foo.IN([Foo(name='a')])).fetch(), [a])
    # An IN query with empty argument can be constructed but not executed.
    q = Bar.query(Bar.foo.IN(set()))
    self.assertRaises(datastore_errors.BadQueryError, q.fetch)
    # Passing a non-sequence argument should fail.
    self.assertRaises(datastore_errors.BadArgumentError,
                      Bar.foo.IN, 42)
    self.assertRaises(datastore_errors.BadArgumentError,
                      Bar.foo.IN, None)
    self.assertRaises(datastore_errors.BadArgumentError,
                      Bar.foo.IN, 'not a sequence')

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
      e.key = None
      reports_a.append(e)
    reports_b = []
    for i in range(3, 6):
      e = Employee(name=str(i), rank=0)
      e.put()
      e.key = None
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
    res = list(q.iter(offset=1))
    self.assertEqual(res, [mgr_c])
    res = list(q.iter(limit=1))
    self.assertEqual(res, [mgr_a])

  def testQueryForWholeStructureCallsDatastoreType(self):
    # See issue 87.  http://goo.gl/Tl5Ed
    class Event(model.Model):
      what = model.StringProperty()
      when = model.DateProperty()  # Has non-trivial _datastore_type().

    class Outer(model.Model):
      who = model.StringProperty()
      events = model.StructuredProperty(Event, repeated=True)
    q = Outer.query(Outer.events == Event(what='stuff',
                                          when=datetime.date.today()))
    q.fetch()  # Failed before the fix.

  def testQueryForWholeNestedStructure(self):
    class A(model.Model):
      a1 = model.StringProperty()
      a2 = model.StringProperty()

    class B(model.Model):
      b1 = model.StructuredProperty(A)
      b2 = model.StructuredProperty(A)

    class C(model.Model):
      c = model.StructuredProperty(B)
    x = C(c=B(b1=A(a1='a1', a2='a2'), b2=A(a1='a3', a2='a4')))
    x.put()
    q = C.query(C.c == x.c)
    self.assertEqual(q.get(), x)

  def testQueryForWholeStructureNone(self):
    class X(model.Model):
      name = model.StringProperty()

    class Y(model.Model):
      x = model.StructuredProperty(X)
    y = Y(x=None)
    y.put()
    q = Y.query(Y.x == None)
    self.assertEqual(q.fetch(), [y])

  def testQueryAncestorConsistentWithAppId(self):
    class Employee(model.Model):
      pass
    a = model.Key(Employee, 1)
    self.assertEqual(a.app(), self.APP_ID)  # Just checkin'.
    Employee.query(ancestor=a, app=a.app()).fetch()  # Shouldn't fail.
    self.assertRaises(Exception, Employee.query, ancestor=a, app='notthisapp')

  def testQueryAncestorConsistentWithNamespace(self):
    class Employee(model.Model):
      pass
    a = model.Key(Employee, 1, namespace='ns')
    self.assertEqual(a.namespace(), 'ns')  # Just checkin'.
    Employee.query(ancestor=a, namespace='ns').fetch()
    Employee.query(ancestor=a, namespace=None).fetch()
    self.assertRaises(Exception,
                      Employee.query, ancestor=a, namespace='another')
    self.assertRaises(Exception,
                      Employee.query, ancestor=a, namespace='')
    # And again with the default namespace.
    b = model.Key(Employee, 1)
    self.assertEqual(b.namespace(), '')  # Just checkin'.
    Employee.query(ancestor=b, namespace='')
    Employee.query(ancestor=b, namespace=None)
    self.assertRaises(Exception,
                      Employee.query, ancestor=b, namespace='ns')
    # Finally some queries with a namespace but no ancestor.
    Employee.query(namespace='').fetch()
    Employee.query(namespace='ns').fetch()

  def testQueryWithNamespace(self):
    class Employee(model.Model):
      pass
    k = model.Key(Employee, None, namespace='ns')
    e = Employee(key=k)
    e.put()
    self.assertEqual(Employee.query().fetch(), [])
    self.assertEqual(Employee.query(namespace='ns').fetch(), [e])

  def testQueryFilterAndOrderPreserveNamespace(self):
    class Employee(model.Model):
      name = model.StringProperty()
    q1 = Employee.query(namespace='ns')
    q2 = q1.filter(Employee.name == 'Joe')
    self.assertEqual(q2.namespace, 'ns')
    # Ditto for order()
    q3 = q2.order(Employee.name)
    self.assertEqual(q3.namespace, 'ns')

  def testMultiQuery(self):
    q1 = query.Query(kind='Foo').filter(Foo.tags == 'jill').order(Foo.name)
    q2 = query.Query(kind='Foo').filter(Foo.tags == 'joe').order(Foo.name)
    qq = query._MultiQuery([q1, q2])
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

  # TODO: Test map() with esoteric argument combinations
  # e.g. keys_only, produce_cursors, and merge_future.

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
    self.assertEqual(q.fetch(2), [self.jill, self.joe])
    self.assertEqual(q.fetch(1), [self.jill])

  def testFetchAsync(self):
    q = query.Query(kind='Foo').filter(Foo.tags == 'jill').order(Foo.name)

    @tasklets.synctasklet
    def foo():
      res = yield q.fetch_async(10)
      self.assertEqual(res, [self.jill, self.joe])
      res = yield q.fetch_async(2)
      self.assertEqual(res, [self.jill, self.joe])
      res = yield q.fetch_async(1)
      self.assertEqual(res, [self.jill])
    foo()

  def testFetchEmpty(self):
    q = query.Query(kind='Foo').filter(Foo.tags == 'jillian')
    self.assertEqual(q.fetch(1), [])

  def testFetchKeysOnly(self):
    q = query.Query(kind='Foo').filter(Foo.tags == 'jill').order(Foo.name)
    self.assertEqual(q.fetch(10, keys_only=True),
                     [self.jill.key, self.joe.key])

  def testGet(self):
    q = query.Query(kind='Foo').filter(Foo.tags == 'jill').order(Foo.name)
    self.assertEqual(q.get(), self.jill)

  def testGetEmpty(self):
    q = query.Query(kind='Foo').filter(Foo.tags == 'jillian')
    self.assertEqual(q.get(), None)

  def testGetKeysOnly(self):
    q = query.Query(kind='Foo').filter(Foo.tags == 'jill').order(Foo.name)
    self.assertEqual(q.get(keys_only=True), self.jill.key)

  def testCursors(self):
    q = query.Query(kind='Foo')
    it = q.iter(produce_cursors=True)
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
    q = query.Query(kind='Foo')
    it = q.iter(produce_cursors=True, keys_only=True)
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

  def testCursorsForAugmentedQuery(self):
    class Employee(model.Model):
      name = model.StringProperty()
      rank = model.IntegerProperty()

    class Manager(Employee):
      report = model.StructuredProperty(Employee, repeated=True)
    reports_a = []
    for i in range(3):
      e = Employee(name=str(i), rank=i)
      e.put()
      e.key = None
      reports_a.append(e)
    reports_b = []
    for i in range(3, 6):
      e = Employee(name=str(i), rank=0)
      e.put()
      e.key = None
      reports_b.append(e)
    mgr_a = Manager(name='a', report=reports_a)
    mgr_a.put()
    mgr_b = Manager(name='b', report=reports_b)
    mgr_b.put()
    mgr_c = Manager(name='c', report=reports_a + reports_b)
    mgr_c.put()
    it = Manager.query(Manager.report == Employee(name='1', rank=1)).iter()

    it.next()
    self.assertRaises(NotImplementedError, it.cursor_before)
    self.assertRaises(NotImplementedError, it.cursor_after)

    it.next()
    self.assertRaises(NotImplementedError, it.cursor_before)
    self.assertRaises(NotImplementedError, it.cursor_after)

    self.assertFalse(it.has_next())

  def testCursorsEfficientPaging(self):
    # We want to read a 'page' of data, get the cursor just past the
    # page, and know whether there is another page, all with a single
    # RPC.  To do this, set limit=pagesize+1, batch_size=pagesize.
    q = query.Query(kind='Foo')
    cursors = {}
    mores = {}
    for pagesize in [1, 2, 3, 4]:
      it = q.iter(produce_cursors=True, limit=pagesize + 1, batch_size=pagesize)
      todo = pagesize
      for _ in it:
        todo -= 1
        if todo <= 0:
          break
      cursors[pagesize] = it.cursor_after()
      mores[pagesize] = it.probably_has_next()
    self.assertEqual(mores, {1: True, 2: True, 3: False, 4: False})
    self.assertEqual(cursors[3], cursors[4])
    # TODO: Assert that only one RPC call was made.

  def testProbablyHasNext(self):
    q = query.Query(kind='Foo')
    probablies = []
    it = q.iter(produce_cursors=True)
    for _ in it:
      probablies.append(it.probably_has_next())
    self.assertEqual(probablies, [True, True, False])

  def testProbablyHasNextMultipleBatches(self):
    q = query.Query(kind='Foo')
    probablies = []
    it = q.iter(produce_cursors=True, batch_size=1)
    for _ in it:
      probablies.append(it.probably_has_next())
    self.assertEqual(probablies, [True, True, False])

  def testProbablyHasNextAndHasNextInteraction(self):
    q = query.Query(kind='Foo')
    mores = []
    probablies = []
    it = q.iter(produce_cursors=True)
    for _ in it:
      mores.append(it.has_next())
      probablies.append(it.probably_has_next())
    self.assertEqual(probablies, [True, True, False])
    self.assertEqual(mores, [True, True, False])

  def testCursorsDelete(self):
    """Tests that deleting an entity doesn't affect cursor positioning."""
    class DeletedEntity(model.Model):
      name = model.StringProperty()
    entities = [DeletedEntity(name='A'),
                DeletedEntity(name='B'),
                DeletedEntity(name='C')]
    model.put_multi(entities)
    q = DeletedEntity.query().order(DeletedEntity.name)
    it = q.iter(limit=2, produce_cursors=True)
    self.assertEqual('A', it.next().name)
    entities[0].key.delete()
    # Grab cursor after deleting first entity. This should point before second.
    cursor = it.cursor_after()
    it = q.iter(start_cursor=cursor, produce_cursors=True)
    self.assertEqual('B', it.next().name)

  def testSkippedResultCursor(self):
    class SkippedEntity(model.Model):
      name = model.StringProperty()
    entities = [SkippedEntity(name='A'),
                SkippedEntity(name='B'),
                SkippedEntity(name='C')]
    model.put_multi(entities)
    q = SkippedEntity.query().order(SkippedEntity.name)
    it = q.iter(offset=2, produce_cursors=True)
    self.assertEqual('C', it.next().name)
    cursor = it.cursor_before()
    # Run the query at the iterator returned before the first result
    it = q.iter(start_cursor=cursor, produce_cursors=True)
    self.assertEqual('C', it.next().name)

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

  def testCountPostFilter(self):
    class Froo(model.Model):
      name = model.StringProperty()
      rate = model.IntegerProperty()
      age = model.IntegerProperty()

    class Bar(model.Model):
      name = model.StringProperty()
      froo = model.StructuredProperty(Froo, repeated=True)
    b1 = Bar(name='b1', froo=[Froo(name='a', rate=1)])
    b1.put()
    b2 = Bar(name='b2', froo=[Froo(name='a', rate=1)])
    b2.put()
    q = Bar.query(Bar.froo == Froo(name='a', rate=1))
    self.assertEqual(q.count(3), 2)
    self.assertEqual(q.count(2), 2)
    self.assertEqual(q.count(1), 1)

  def testCountDisjunction(self):
    q = Foo.query(Foo.name.IN(['joe', 'jill']))
    self.assertEqual(q.count(3), 2)
    self.assertEqual(q.count(2), 2)
    self.assertEqual(q.count(1), 1)

  def testLargeCount(self):
    class Bar(model.Model):
      pass
    for i in xrange(0, datastore_stub_util._MAX_QUERY_OFFSET + 10):
      Bar(id=str(i)).put()
    count = Bar.query().count(datastore_stub_util._MAX_QUERY_OFFSET + 20)
    self.assertEqual(datastore_stub_util._MAX_QUERY_OFFSET + 10, count)

    # Test count less than requested limit.
    count = Bar.query().count(datastore_stub_util._MAX_QUERY_OFFSET + 5)
    self.assertEqual(datastore_stub_util._MAX_QUERY_OFFSET + 5, count)

  def testFetchPage(self):
    # This test implicitly also tests fetch_page_async().
    q = query.Query(kind='Foo')

    page_size = 1
    res, curs, more = q.fetch_page(page_size)
    self.assertEqual(res, [self.joe])
    self.assertTrue(more)
    res, curs, more = q.fetch_page(page_size, start_cursor=curs)
    self.assertEqual(res, [self.jill])
    self.assertTrue(more)
    res, curs, more = q.fetch_page(page_size, start_cursor=curs)
    self.assertEqual(res, [self.moe])
    self.assertFalse(more)
    res, curs, more = q.fetch_page(page_size, start_cursor=curs)
    self.assertEqual(res, [])
    self.assertFalse(more)

    page_size = 2
    res, curs, more = q.fetch_page(page_size)
    self.assertEqual(res, [self.joe, self.jill])
    self.assertTrue(more)
    res, curs, more = q.fetch_page(page_size, start_cursor=curs)
    self.assertEqual(res, [self.moe])
    self.assertFalse(more)
    res, curs, more = q.fetch_page(page_size, start_cursor=curs)
    self.assertEqual(res, [])
    self.assertFalse(more)

    page_size = 3
    res, curs, more = q.fetch_page(page_size)
    self.assertEqual(res, [self.joe, self.jill, self.moe])
    self.assertFalse(more)
    res, curs, more = q.fetch_page(page_size, start_cursor=curs)
    self.assertEqual(res, [])
    self.assertFalse(more)

    page_size = 4
    res, curs, more = q.fetch_page(page_size)
    self.assertEqual(res, [self.joe, self.jill, self.moe])
    self.assertFalse(more)
    res, curs, more = q.fetch_page(page_size, start_cursor=curs)
    self.assertEqual(res, [])
    self.assertFalse(more)

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

  def testMultiQueryIteratorUnordered(self):
    q = query.Query(kind='Foo').filter(Foo.tags.IN(['joe', 'jill']))

    @tasklets.synctasklet
    def foo():
      it = iter(q)
      res = []
      while (yield it.has_next_async()):
        val = it.next()
        res.append(val)
      self.assertEqual(set(r._key for r in res),
                       set([self.jill._key, self.joe._key]))
    foo()

  def testMultiQueryFetch(self):
    q = Foo.query(Foo.tags.IN(['joe', 'jill'])).order(-Foo.name)
    expected = [self.joe, self.jill]
    self.assertEqual(q.fetch(10), expected)
    self.assertEqual(q.fetch(None), expected)
    self.assertEqual(q.fetch(), expected)
    self.assertEqual(q.fetch(2), expected)
    self.assertEqual(q.fetch(1), expected[:1])
    self.assertEqual(q.fetch(10, offset=1), expected[1:])
    self.assertEqual(q.fetch(1, offset=1), expected[1:])
    self.assertEqual(q.fetch(10, keys_only=True), [e._key for e in expected])

  def testMultiQueryFetchUnordered(self):
    q = Foo.query(Foo.tags.IN(['joe', 'jill']))
    expected = [self.joe, self.jill]
    self.assertEqual(q.fetch(10), expected)
    self.assertEqual(q.fetch(None), expected)
    self.assertEqual(q.fetch(), expected)
    self.assertEqual(q.fetch(2), expected)
    self.assertEqual(q.fetch(1), expected[:1])
    self.assertEqual(q.fetch(10, offset=1), expected[1:])
    self.assertEqual(q.fetch(1, offset=1), expected[1:])
    self.assertEqual(q.fetch(10, keys_only=True), [e._key for e in expected])

  def testMultiQueryCount(self):
    q = Foo.query(Foo.tags.IN(['joe', 'jill'])).order(Foo.name)
    self.assertEqual(q.count(10), 2)
    self.assertEqual(q.count(None), 2)
    self.assertEqual(q.count(), 2)
    self.assertEqual(q.count(2), 2)
    self.assertEqual(q.count(1), 1)
    self.assertEqual(q.count(10, keys_only=True), 2)
    self.assertEqual(q.count(keys_only=True), 2)

  def testMultiQueryCountUnordered(self):
    q = Foo.query(Foo.tags.IN(['joe', 'jill']))
    self.assertEqual(q.count(10), 2)
    self.assertEqual(q.count(None), 2)
    self.assertEqual(q.count(), 2)
    self.assertEqual(q.count(10, keys_only=True), 2)
    self.assertEqual(q.count(keys_only=True), 2)

  def testMultiQueryCursors(self):
    self.ExpectWarnings()
    q = Foo.query(Foo.tags.IN(['joe', 'jill']))
    self.assertRaises(datastore_errors.BadArgumentError, q.fetch_page, 1)
    q = q.order(Foo.tags)
    self.assertRaises(datastore_errors.BadArgumentError, q.fetch_page, 1)
    q = q.order(Foo.key)
    expected = q.fetch()
    self.assertEqual(len(expected), 2)
    res, curs, more = q.fetch_page(1, keys_only=True)
    self.assertEqual(res, [expected[0].key])
    self.assertTrue(curs is not None)
    self.assertTrue(more)
    res, curs, more = q.fetch_page(1, keys_only=False, start_cursor=curs)
    self.assertEqual(res, [expected[1]])
    self.assertTrue(curs is not None)
    self.assertFalse(more)
    res, curs, more = q.fetch_page(1, start_cursor=curs)
    self.assertEqual(res, [])
    self.assertTrue(curs is None)
    self.assertFalse(more)

  def testMultiQueryWithAndWithoutAncestor(self):
    class Benjamin(model.Model):
      name = model.StringProperty()
    ben = Benjamin(name='ben', parent=self.moe.key)
    ben.put()
    benji = Benjamin(name='benji')
    benji.put()
    bq = Benjamin.query()
    baq = Benjamin.query(ancestor=self.moe.key)
    mq = query._MultiQuery([bq, baq])
    res = list(mq)
    self.assertEqual(res, [benji, ben])

  def testNestedMultiQuery(self):
    class Bar(model.Model):
      a = model.StringProperty()
      b = model.StringProperty()

    class Rank(model.Model):
      val = model.IntegerProperty()

    class Foo(model.Model):
      bar = model.StructuredProperty(Bar, repeated=True)
      rank = model.StructuredProperty(Rank)

    f1 = Foo(bar=[Bar(a='a1', b='b')], rank=Rank(val=1))
    f2 = Foo(bar=[Bar(a='a2', b='e')], rank=Rank(val=2))
    f1.put()
    f2.put()

    q = Foo.query(query.OR(Foo.bar == Bar(a='a1', b='b'),
                           Foo.bar == Bar(a='a2', b='e')))
    q = q.order(Foo.rank.val)
    self.assertEqual([f1, f2], q.fetch())

  def testProbablyHasNextWithMultiQuery(self):
    class Foo(model.Model):
      a = model.IntegerProperty()
    keys = model.put_multi([Foo(a=i) for i in range(100)])
    q = Foo.query(Foo.key.IN(keys)).order(Foo.a)
    it = q.iter()
    for i in range(0, 99):
      it.next()
      # Probably has next is conservative so it should always return True
      # if there are in fact more results.
      self.assertTrue(it.probably_has_next())

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
        ConjunctionNode(FilterNode('tags', '=', 'jill'),
                        FilterNode('rate', '=', 1)),
        ConjunctionNode(FilterNode('tags', '=', 'jill'),
                        FilterNode('rate', '=', 2)),
        ConjunctionNode(FilterNode('tags', '=', 'hello'),
                        FilterNode('rate', '=', 1)),
        ConjunctionNode(FilterNode('tags', '=', 'hello'),
                        FilterNode('rate', '=', 2)))
    self.assertEqual(q.filters, expected)

  def testHalfDistributiveLaw(self):
    DisjunctionNode = query.DisjunctionNode
    ConjunctionNode = query.ConjunctionNode
    FilterNode = query.FilterNode
    filters = ConjunctionNode(
        FilterNode('tags', 'in', ['jill', 'hello']),
        ConjunctionNode(FilterNode('rate', '=', 1),
                        FilterNode('name', '=', 'moe')))
    expected = DisjunctionNode(
        ConjunctionNode(FilterNode('tags', '=', 'jill'),
                        FilterNode('rate', '=', 1),
                        FilterNode('name', '=', 'moe')),
        ConjunctionNode(FilterNode('tags', '=', 'hello'),
                        FilterNode('rate', '=', 1),
                        FilterNode('name', '=', 'moe')))
    self.assertEqual(filters, expected)

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

  def testUnicode(self):
    class MyModel(model.Model):
      n = model.IntegerProperty(u'\u4321')

      @classmethod
      def _get_kind(cls):
        return u'\u1234'.encode('utf-8')
    a = MyModel(n=42)
    k = a.put()
    b = k.get()
    self.assertEqual(a, b)
    self.assertFalse(a is b)
    # So far so good, now try queries
    res = MyModel.query(MyModel.n == 42).fetch()
    self.assertEqual(res, [a])

  def testBlobQuery(self):
    class MyModel(model.Model):
      b = model.BlobProperty(indexed=True)
    a = MyModel(b='\xff\x00')
    a.put()
    q = MyModel.query(MyModel.b == '\xff\x00')
    it = iter(q)
    b = it.next()
    self.assertEqual(a, b)

  def testKindlessQuery(self):
    class ParentModel(model.Model):
      a = model.StringProperty()

    class ChildModel(model.Model):
      b = model.StringProperty()
    p = ParentModel(a="Test1")
    p.put()
    c = ChildModel(parent=p.key, b="Test2")
    c.put()
    q = query.Query(ancestor=p.key)
    self.assertEqual(q.count(), 2)
    l = q.fetch()
    self.assertTrue(c in l)
    self.assertTrue(p in l)

  def testExpandoQueries(self):
    class Foo(model.Expando):
      pass
    testdata = {'int': 42,
                'float': 3.14,
                'string': 'hello',
                'bool': True,
                # Don't call this 'key'; it interferes with the built-in
                # key attribute (the entity's key).
                'akey': model.Key('Foo', 1),
                'point': model.GeoPt(52.35, 4.9166667),
                'user': users.User('test@example.com', 'example.com', '123'),
                'blobkey': model.BlobKey('blah'),
                'none': None,
               }
    for name, value in testdata.iteritems():
      foo = Foo()
      setattr(foo, name, value)
      foo.put()
      qry = Foo.query(query.FilterNode(name, '=', value))
      res = qry.get()
      self.assertTrue(res is not None, name)
      self.assertEqual(getattr(res, name), value)
      res.key.delete()

  def testQueryCacheInteraction(self):
    class Bar(model.Model):
      name = model.StringProperty()
    ctx = tasklets.get_context()
    ctx.set_cache_policy(True)
    a = Bar(name='a')
    a.put()
    b = a.key.get()
    self.assertTrue(b is a)  # Just verifying that the cache is on.
    b = Bar.query().get()
    self.assertTrue(b is a)
    a.name = 'x'  # Modify, but don't write.
    b = Bar.query().get()
    self.assertTrue(b is a)
    self.assertEqual(a.name, 'x')
    b = Bar.query().get(use_cache=False)  # Skip the cache.
    self.assertFalse(b is a)
    self.assertEqual(b.name, 'a')
    a.key = None  # Invalidate cache by resetting key.
    b = Bar.query().get()
    self.assertFalse(b is a)
    self.assertEqual(a.name, 'x')
    self.assertEqual(b.name, 'a')

  def testGqlMinimal(self):
    qry = query.gql('SELECT * FROM Foo')
    self.assertEqual(qry.kind, 'Foo')
    self.assertEqual(qry.ancestor, None)
    self.assertEqual(qry.filters, None)
    self.assertEqual(qry.orders, None)

  def testGqlAncestor(self):
    key = model.Key('Foo', 42)
    qry = query.gql("SELECT * FROM Foo WHERE ANCESTOR IS KEY('%s')" %
                    key.urlsafe())
    self.assertEqual(qry.kind, 'Foo')
    self.assertEqual(qry.ancestor, key)
    self.assertEqual(qry.filters, None)
    self.assertEqual(qry.orders, None)

  def testGqlAncestorWithParameter(self):
    qry = query.gql('SELECT * FROM Foo WHERE ANCESTOR IS :1')
    self.assertEqual(qry.kind, 'Foo')
    self.assertEqual(qry.ancestor, query.Parameter(1))
    self.assertEqual(qry.filters, None)
    self.assertEqual(qry.orders, None)

  def testGqlFilter(self):
    qry = query.gql("SELECT * FROM Foo WHERE name = 'joe' AND rate = 1")
    self.assertEqual(qry.kind, 'Foo')
    self.assertEqual(qry.ancestor, None)
    self.assertEqual(qry.filters,
                     query.ConjunctionNode(
                         query.FilterNode('name', '=', 'joe'),
                         query.FilterNode('rate', '=', 1)))
    self.assertEqual(qry.orders, None)

  def testGqlOrder(self):
    qry = query.gql('SELECT * FROM Foo ORDER BY name')
    self.assertEqual(query._orders_to_orderings(qry.orders),
                     [('name', query._ASC)])

  def testGqlOffset(self):
    qry = query.gql('SELECT * FROM Foo OFFSET 2')
    self.assertEqual(qry.default_options.offset, 2)

  def testGqlLimit(self):
    qry = query.gql('SELECT * FROM Foo LIMIT 2')
    self.assertEqual(qry.default_options.limit, 2)

  def testGqlParameters(self):
    qry = query.gql('SELECT * FROM Foo WHERE name = :1 AND rate = :foo')
    self.assertEqual(qry.kind, 'Foo')
    self.assertEqual(qry.ancestor, None)
    self.assertEqual(qry.filters,
                     query.ConjunctionNode(
                         query.ParameterNode(Foo.name, '=',
                                             query.Parameter(1)),
                         query.ParameterNode(Foo.rate, '=',
                                             query.Parameter('foo'))))
    self.assertEqual(qry.orders, None)

  def testGqlBindParameters(self):
    pqry = query.gql('SELECT * FROM Foo WHERE name = :1')
    qry = pqry.bind('joe')
    self.assertEqual(list(qry), [self.joe])
    qry = pqry.bind('jill')
    self.assertEqual(list(qry), [self.jill])

  def testGqlUnresolvedParameters(self):
    self.ExpectErrors()
    qry = query.gql(
        'SELECT * FROM Foo WHERE name = :1')
    self.assertRaises(datastore_errors.BadArgumentError, qry.fetch)
    self.assertRaises(datastore_errors.BadArgumentError, qry.count)
    self.assertRaises(datastore_errors.BadArgumentError, list, qry)
    self.assertRaises(datastore_errors.BadArgumentError, qry.iter)

  def checkGql(self, expected, gql, args=(), kwds={},
               fetch=lambda q: list(q)):
    actual = fetch(query.gql(gql).bind(*args, **kwds))
    self.assertEqual(expected, actual)

  def testGqlBasicQueries(self):
    self.checkGql([self.joe, self.jill, self.moe], "SELECT * FROM Foo")

  def testGqlKeyQueries(self):
    self.checkGql([self.joe.key, self.jill.key, self.moe.key],
                  "SELECT __key__ FROM Foo")

  def testGqlOperatorQueries(self):
    self.checkGql([self.joe], "SELECT * FROM Foo WHERE name = 'joe'")
    self.checkGql([self.moe], "SELECT * FROM Foo WHERE name > 'joe'")
    self.checkGql([self.jill], "SELECT * FROM Foo WHERE name < 'joe'")
    self.checkGql([self.joe, self.moe],
                  "SELECT * FROM Foo WHERE name >= 'joe'")
    self.checkGql([self.jill, self.joe],
                  "SELECT * FROM Foo WHERE name <= 'joe'")
    self.checkGql([self.jill, self.moe],
                  "SELECT * FROM Foo WHERE name != 'joe'")
    # NOTE: The ordering on these is questionable:
    self.checkGql([self.joe, self.jill],
                  "SELECT * FROM Foo WHERE name IN ('joe', 'jill')")
    self.checkGql([self.jill, self.joe],
                  "SELECT * FROM Foo WHERE name IN ('jill', 'joe')")

  def testGqlOrderQueries(self):
    self.checkGql([self.jill, self.joe, self.moe],
                  "SELECT * FROM Foo ORDER BY name")
    self.checkGql([self.moe, self.joe, self.jill],
                  "SELECT * FROM Foo ORDER BY name DESC")
    self.checkGql([self.joe, self.jill, self.moe],
                  "SELECT * FROM Foo ORDER BY __key__ ASC")
    self.checkGql([self.moe, self.jill, self.joe],
                  "SELECT * FROM Foo ORDER BY __key__ DESC")
    self.checkGql([self.jill, self.joe, self.moe],
                  "SELECT * FROM Foo ORDER BY rate DESC, name")

  def testGqlOffsetQuery(self):
    self.checkGql([self.jill, self.moe], "SELECT * FROM Foo OFFSET 1")

  def testGqlLimitQuery(self):
    self.checkGql([self.joe, self.jill], "SELECT * FROM Foo LIMIT 2")

  def testGqlLimitOffsetQuery(self):
    self.checkGql([self.jill], "SELECT * FROM Foo LIMIT 1 OFFSET 1")

  def testGqlLimitOffsetQueryUsingFetch(self):
    self.checkGql([self.jill], "SELECT * FROM Foo LIMIT 1 OFFSET 1",
                  fetch=lambda q: q.fetch())

# XXX TODO: Make this work:
# def testGqlLimitQueryUsingFetch(self):
#   self.checkGql([self.joe, self.jill], "SELECT * FROM Foo LIMIT 2",
#                 fetch=lambda q: q.fetch(3))

  def testGqlOffsetQueryUsingFetchPage(self):
    q = query.gql("SELECT * FROM Foo LIMIT 2")
    res1, cur1, more1 = q.fetch_page(1)
    self.assertEqual([self.joe], res1)
    self.assertEqual(True, more1)
    res2, cur2, more2 = q.fetch_page(1, start_cursor=cur1)
    self.assertEqual([self.jill], res2)
    # XXX TODO: Gotta make this work:
    # self.assertEqual(False, more2)
    # res3, cur3, more3 = q.fetch_page(1, start_cursor=cur2)
    # self.assertEqual([], res3)
    # self.assertEqual(False, more3)
    # self.assertEqual(None, cur3)

  def testGqlLimitQueryUsingFetchPage(self):
    q = query.gql("SELECT * FROM Foo OFFSET 1")
    res1, cur1, more1 = q.fetch_page(1)
    self.assertEqual([self.jill], res1)
    self.assertEqual(True, more1)
    # NOTE: Without offset=0, the following break.
    res2, cur2, more2 = q.fetch_page(1, start_cursor=cur1, offset=0)
    self.assertEqual([self.moe], res2)
    self.assertEqual(False, more2)
    res3, cur3, more3 = q.fetch_page(1, start_cursor=cur2, offset=0)
    self.assertEqual([], res3)
    self.assertEqual(False, more3)
    self.assertEqual(None, cur3)

  def testGqlParameterizedAncestor(self):
    q = query.gql("SELECT * FROM Foo WHERE ANCESTOR IS :1")
    self.assertEqual([self.moe], q.bind(self.moe.key).fetch())

  def testGqlParameterizedInClause(self):
    # NOTE: The ordering on these is questionable:
    q = query.gql("SELECT * FROM Foo WHERE name IN :1")
    self.assertEqual([self.jill, self.joe], q.bind(('jill', 'joe')).fetch())
    # Exercise the LIST function.
    q = query.gql("SELECT * FROM Foo WHERE name IN (:a, :b)")
    self.assertEqual([self.jill, self.joe], q.bind(a='jill', b='joe').fetch())
    # Generate OR/AND nodes containing parameter nodes.
    q = query.gql("SELECT * FROM Foo WHERE name = :1 AND rate in (1, 2)")
    self.assertEqual([self.jill], q.bind('jill').fetch())

  def testGqlKeyFunction(self):
    class Bar(model.Model):
      ref = model.KeyProperty(kind=Foo)
    noref = Bar()
    noref.put()
    joeref = Bar(ref=self.joe.key)
    joeref.put()
    moeref = Bar(ref=self.moe.key)
    moeref.put()
    self.assertEqual(
        [noref],
        Bar.gql("WHERE ref = NULL").fetch())
    self.assertEqual(
        [noref],
        Bar.gql("WHERE ref = :1").bind(None).fetch())
    self.assertEqual(
        [joeref],
        Bar.gql("WHERE ref = :1").bind(self.joe.key).fetch())
    self.assertEqual(
        [joeref],
        Bar.gql("WHERE ref = KEY('%s')" % self.joe.key.urlsafe()).fetch())
    self.assertEqual(
        [joeref],
        Bar.gql("WHERE ref = KEY('Foo', %s)" % self.joe.key.id()).fetch())
    self.assertEqual(
        [joeref],
        Bar.gql("WHERE ref = KEY(:1)").bind(self.joe.key.urlsafe()).fetch())
    self.assertEqual(
        [joeref],
        Bar.gql("WHERE ref = KEY('Foo', :1)").bind(self.joe.key.id()).fetch())

  def testGqlKeyFunctionAncestor(self):
    class Bar(model.Model):
      pass
    nobar = Bar()
    nobar.put()
    joebar = Bar(parent=self.joe.key)
    joebar.put()
    moebar = Bar(parent=self.moe.key)
    moebar.put()
    self.assertEqual(
        [joebar],
        Bar.gql("WHERE ANCESTOR IS KEY('%s')" % self.joe.key.urlsafe()).fetch())
    self.assertEqual(
        [joebar],
        Bar.gql("WHERE ANCESTOR IS :1").bind(self.joe.key).fetch())
    self.assertEqual(
        [joebar],
        Bar.gql("WHERE ANCESTOR IS KEY(:1)").bind(
            self.joe.key.urlsafe()).fetch())
    self.assertEqual(
        [joebar],
        Bar.gql("WHERE ANCESTOR IS KEY('Foo', :1)")
        .bind(self.joe.key.id()).fetch())

  def testGqlAncestorFunctionError(self):
    self.assertRaises(TypeError,
                      query.gql, 'SELECT * FROM Foo WHERE ANCESTOR IS USER(:1)')

  def testGqlOtherFunctions(self):
    class Bar(model.Model):
      auser = model.UserProperty()
      apoint = model.GeoPtProperty()
      adatetime = model.DateTimeProperty()
      adate = model.DateProperty()
      atime = model.TimeProperty()
    abar = Bar(
        auser=users.User('test@example.com'),
        apoint=model.GeoPt(52.35, 4.9166667),
        adatetime=datetime.datetime(2012, 2, 1, 14, 54, 0),
        adate=datetime.date(2012, 2, 2),
        atime=datetime.time(14, 54, 0),
    )
    abar.put()
    bbar = Bar()
    bbar.put()
    self.assertEqual(
        [abar.key],
        query.gql("SELECT __key__ FROM Bar WHERE auser=USER(:1)")
        .bind('test@example.com').fetch())
    self.assertEqual(
        [abar.key],
        query.gql("SELECT __key__ FROM Bar WHERE apoint=GEOPT(:1, :2)")
        .bind(52.35, 4.9166667).fetch())
    self.assertEqual(
        [abar.key],
        query.gql("SELECT __key__ FROM Bar WHERE adatetime=DATETIME(:1)")
        .bind('2012-02-01 14:54:00').fetch())
    self.assertEqual(
        [abar.key],
        query.gql("SELECT __key__ FROM Bar WHERE adate=DATE(:1, :2, :2)")
        .bind(2012, 2).fetch())
    self.assertEqual(
        [abar.key],
        query.gql("SELECT __key__ FROM Bar WHERE atime=TIME(:hour, :min, :sec)")
        .bind(hour=14, min=54, sec=0).fetch())

  def testGqlStructuredPropertyQuery(self):
    class Bar(model.Model):
      foo = model.StructuredProperty(Foo)
    barf = Bar(foo=Foo(name='one', rate=3, tags=['a', 'b']))
    barf.put()
    barg = Bar(foo=Foo(name='two', rate=4, tags=['b', 'c']))
    barg.put()
    barh = Bar()
    barh.put()
    # TODO: Once SDK 1.6.3 is released, drop quotes around foo.name.
    q = Bar.gql("WHERE \"foo.name\" = 'one'")
    self.assertEqual([barf], q.fetch())
    q = Bar.gql("WHERE foo = :1").bind(Foo(name='two', rate=4))
    self.assertEqual([barg], q.fetch())
    q = Bar.gql("WHERE foo = NULL")
    self.assertEqual([barh], q.fetch())
    q = Bar.gql("WHERE foo = :1")
    self.assertEqual([barh], q.bind(None).fetch())

  def testGqlExpandoProperty(self):
    class Bar(model.Expando):
      pass
    babar = Bar(name='Babar')
    babar.put()
    bare = Bar(nude=42)
    bare.put()
    q = Bar.gql("WHERE name = 'Babar'")
    self.assertEqual([babar], q.fetch())
    q = Bar.gql("WHERE nude = :1")
    self.assertEqual([bare], q.bind(42).fetch())

  def testGqlExpandoInStructure(self):
    class Bar(model.Expando):
      pass

    class Baz(model.Model):
      bar = model.StructuredProperty(Bar)
    bazar = Baz(bar=Bar(bow=1, wow=2))
    bazar.put()
    bazone = Baz()
    bazone.put()
    q = Baz.gql("WHERE \"bar.bow\" = 1")
    self.assertEqual([bazar], q.fetch())

  def testGqlKindlessQuery(self):
    results = query.gql('SELECT *').fetch()
    self.assertEqual([self.joe, self.jill, self.moe], results)

  def testGqlSubclass(self):
    # You can pass _gql() a subclass of Query and it'll use that.
    class MyQuery(query.Query):
      pass
    q = query._gql("SELECT * FROM Foo WHERE name = :1", query_class=MyQuery)
    self.assertTrue(isinstance(q, MyQuery))
    # And bind() preserves the class.
    qb = q.bind('joe')
    self.assertTrue(isinstance(qb, MyQuery))
    # .filter() also preserves the class, as well as default_options.
    qf = q.filter(Foo.rate == 1)
    self.assertTrue(isinstance(qf, MyQuery))
    self.assertEqual(qf.default_options, q.default_options)
    # Same for .options().
    qo = q.order(-Foo.name)
    self.assertTrue(isinstance(qo, MyQuery))
    self.assertEqual(qo.default_options, q.default_options)

  def testGqlUnusedBindings(self):
    # Only unused positional bindings raise an error.
    q = Foo.gql("WHERE ANCESTOR IS :1 AND rate >= :2")
    qb = q.bind(self.joe.key, 2, foo=42)  # Must not fail
    self.assertRaises(datastore_errors.BadArgumentError, q.bind)
    self.assertRaises(datastore_errors.BadArgumentError, q.bind, self.joe.key)
    self.assertRaises(datastore_errors.BadArgumentError, q.bind,
                      self.joe.key, 2, 42)

  def testGqlWithBind(self):
    q = Foo.gql("WHERE name = :1", 'joe')
    self.assertEqual([self.joe], q.fetch())

  def testGqlAnalyze(self):
    q = Foo.gql("WHERE name = 'joe'")
    self.assertEqual([], q.analyze())
    q = Foo.gql("WHERE name = :1 AND rate = :2")
    self.assertEqual([1, 2], q.analyze())
    q = Foo.gql("WHERE name = :foo AND rate = :bar")
    self.assertEqual(['bar', 'foo'], q.analyze())
    q = Foo.gql("WHERE tags = :1 AND name = :foo AND rate = :bar")
    self.assertEqual([1, 'bar', 'foo'], q.analyze())

  def testGqlGroupBy(self):
    q = query.gql("SELECT DISTINCT name, tags FROM Foo "
                  "WHERE name < 'joe' ORDER BY name")
    self.assertEquals(('name', 'tags'), q.projection)
    self.assertEquals(('name', 'tags'), q.group_by)
    self.assertEquals(True, q.is_distinct)
    ents = q.fetch()
    ents.sort(key=lambda ent: ent.tags)
    self.assertEqual(ents, [Foo(name='jill', tags=['jack'],
                                key=self.jill.key,
                                projection=['name', 'tags']),
                            Foo(name='jill', tags=['jill'],
                                key=self.jill.key,
                                projection=('name', 'tags'))])

  def testGqlProjection(self):
    q = query.gql("SELECT name, tags FROM Foo WHERE name < 'joe' ORDER BY name")
    self.assertEquals(('name', 'tags'), q.projection)
    self.assertEquals(None, q.group_by)
    self.assertEquals(False, q.is_distinct)
    ents = q.fetch()
    ents.sort(key=lambda ent: ent.tags)
    self.assertEqual(ents, [Foo(name='jill', tags=['jack'],
                                key=self.jill.key,
                                projection=['name', 'tags']),
                            Foo(name='jill', tags=['jill'],
                                key=self.jill.key,
                                projection=('name', 'tags'))])

  def testGqlBadProjection(self):
    self.assertRaises(model.BadProjectionError,
                      query.gql, "SELECT qqq FROM Foo")
    self.assertRaises(model.InvalidPropertyError,
                      query.gql, "SELECT qqq FROM Foo")

  def testGqlBadKind(self):
    self.assertRaises(model.KindError,
                      query.gql, "SELECT * FROM Whatever")

  def testAsyncNamespace(self):
    # Test that async queries pick up the namespace when the
    # foo_async() call is made, not later.
    # See issue 168.  http://goo.gl/aJp7i
    namespace_manager.set_namespace('mission')
    barney = Foo(name='Barney')
    barney.put()
    willy = Foo(name='Willy')
    willy.put()
    q1 = Foo.query()
    qm = Foo.query(Foo.name.IN(['Barney', 'Willy'])).order(Foo._key)

    # Test twice: once with a simple query, once with a MultiQuery.
    for q in q1, qm:
      # Test fetch_async().
      namespace_manager.set_namespace('mission')
      fut = q.fetch_async()
      namespace_manager.set_namespace('impossible')
      res = fut.get_result()
      self.assertEqual(res, [barney, willy])

      # Test map_async().
      namespace_manager.set_namespace('mission')
      fut = q.map_async(None)
      namespace_manager.set_namespace('impossible')
      res = fut.get_result()
      self.assertEqual(res, [barney, willy])

      # Test get_async().
      namespace_manager.set_namespace('mission')
      fut = q.get_async()
      namespace_manager.set_namespace('impossible')
      res = fut.get_result()
      self.assertEqual(res, barney)

      # Test count_async().
      namespace_manager.set_namespace('mission')
      fut = q.count_async()
      namespace_manager.set_namespace('impossible')
      res = fut.get_result()
      self.assertEqual(res, 2)

      # Test fetch_page_async().
      namespace_manager.set_namespace('mission')
      fut = q.fetch_page_async(2)
      namespace_manager.set_namespace('impossible')
      res, cur, more = fut.get_result()
      self.assertEqual(res, [barney, willy])
      self.assertEqual(more, False)

  def hugeOffsetTestHelper(self, fetch):
    """ Helper function to test large offsets.

    Args:
      fetch: A function that takes in (query, offset) and returns a list with
      one result.
    """
    # See issue 210.  http://goo.gl/EDfHa
    # Vastly reduce _MAX_QUERY_OFFSET since otherwise the test spends
    # several seconds creating enough entities to reproduce the problem.
    save_max_query_offset = datastore_stub_util._MAX_QUERY_OFFSET
    try:
      datastore_stub_util._MAX_QUERY_OFFSET = 10
      ndb = model

      class M(ndb.Model):
        a = ndb.IntegerProperty()
      ms = [M(a=i, id='%04d' % i) for i in range(33)]
      ks = ndb.put_multi(ms)
      q = M.query().order(M.a)
      xs = fetch(q, 9)
      self.assertEqual(xs, ms[9:10])
      xs = fetch(q, 10)
      self.assertEqual(xs, ms[10:11])
      xs = fetch(q, 11)
      self.assertEqual(xs, ms[11:12])
      xs = fetch(q, 21)
      self.assertEqual(xs, ms[21:22])
      xs = fetch(q, 31)
      self.assertEqual(xs, ms[31:32])
    finally:
      datastore_stub_util._MAX_QUERY_OFFSET = save_max_query_offset

  def testHugeOffset(self):
    """Test offset > MAX_OFFSET for fetch."""
    def fetch_one(qry, offset):
      return qry.fetch(1, offset=offset)

    self.hugeOffsetTestHelper(fetch_one)

  def testHugeOffsetRunToQueue(self):
    """Test offset > MAX_OFFSET for run_to_queue."""
    def fetch_from_queue(qry, offset):
      queue = tasklets.MultiFuture()
      options = query.QueryOptions(offset=offset, limit=1)
      qry.run_to_queue(queue, self.conn, options).check_success()
      results = queue.get_result()
      return [result[2] for result in results]

    self.hugeOffsetTestHelper(fetch_from_queue)

  def testQueryPickleFilter(self):
    for protocol in (0, pickle.HIGHEST_PROTOCOL):
      q = query.Query(kind='Foo').filter(Foo.rate == 1)
      new_q = pickle.loads(pickle.dumps(q, protocol=protocol))

      self.assertTrue(isinstance(new_q, query.Query))
      self.assertEqual(new_q.filters, q.filters)
      self.assertTrue(isinstance(new_q.filters, query.FilterNode))

  def testQueryPickleParameterAndConjunction(self):
    for protocol in (0, pickle.HIGHEST_PROTOCOL):
      q = query.gql('SELECT * FROM Foo WHERE name = :1 AND rate = :foo')
      new_q = pickle.loads(pickle.dumps(q, protocol=protocol))

      self.assertTrue(isinstance(new_q, query.Query))
      self.assertEqual(new_q.filters, q.filters)
      self.assertTrue(isinstance(new_q.filters, query.ConjunctionNode))
      self.assertTrue(isinstance(list(new_q.filters)[0], query.ParameterNode))

  def testQueryPicklePostFilter(self):
    class Struct(model.Model):
      other_prop = model.StringProperty()
      other_other_prop = model.IntegerProperty()

    class Bar(model.Model):
      prop = model.StructuredProperty(Struct, repeated=True)

    for protocol in (0, pickle.HIGHEST_PROTOCOL):
      q = query.Query(kind='Bar').filter(
          Bar.prop == Struct(other_prop='foo', other_other_prop=1))
      new_q = pickle.loads(pickle.dumps(q, protocol=protocol))

      self.assertTrue(isinstance(new_q, query.Query))
      self.assertEqual(new_q.filters, q.filters)
      self.assertTrue(isinstance(new_q.filters, query.ConjunctionNode))
      subnodes = list(new_q.filters)
      self.assertTrue(
          any(isinstance(node, query.PostFilterNode) for node in subnodes))


class IndexListTestMixin(object):
  """Tests for Index lists. Must be used with BaseQueryTestMixin."""

  def testIndexListPremature(self):
    # Before calling next() we don't have the information.
    q = Foo.query(Foo.name >= 'joe', Foo.tags == 'joe')
    qi = q.iter()
    self.assertEqual(qi.index_list(), None)

  def testIndexListEmpty(self):
    # A simple query requires no composite indexes.
    q = Foo.query(Foo.name == 'joe', Foo.tags == 'joe')
    qi = q.iter()
    qi.next()
    self.assertEqual(qi.index_list(), [])

  def testIndexListNontrivial(self):
    # Test a non-trivial query.
    q = Foo.query(Foo.name >= 'joe', Foo.tags == 'joe')
    qi = q.iter()
    qi.next()
    properties = [model.IndexProperty(name='tags', direction='asc'),
                  model.IndexProperty(name='name', direction='asc')]
    self.assertEqual(qi.index_list(),
                     [model.IndexState(
                         definition=model.Index(kind='Foo',
                                                properties=properties,
                                                ancestor=False),
                         state='serving',
                         id=0)])

  def testIndexListExhausted(self):
    # Test that the information is preserved after the iterator is
    # exhausted.
    q = Foo.query(Foo.name >= 'joe', Foo.tags == 'joe')
    qi = q.iter()
    list(qi)
    properties = [model.IndexProperty(name='tags', direction='asc'),
                  model.IndexProperty(name='name', direction='asc')]
    self.assertEqual(qi.index_list(),
                     [model.IndexState(
                         definition=model.Index(kind='Foo',
                                                properties=properties,
                                                ancestor=False),
                         state='serving',
                         id=0)])

  def testIndexListWithIndexAndOrder(self):
    # Test a non-trivial query with sort order and an actual composite
    # index present.
    q = Foo.query(Foo.name >= 'joe', Foo.tags == 'joe')
    q = q.order(-Foo.name, Foo.tags)
    qi = q.iter()
    qi.next()
    # TODO: This is a little odd, because that's not exactly the index
    # we created...?
    properties = [model.IndexProperty(name='tags', direction='asc'),
                  model.IndexProperty(name='name', direction='desc')]
    self.assertEqual(qi.index_list(),
                     [model.IndexState(
                         definition=model.Index(kind='Foo',
                                                properties=properties,
                                                ancestor=False),
                         state='serving',
                         id=0)])

  def testIndexListMultiQuery(self):
    q = Foo.query(query.OR(Foo.name == 'joe', Foo.name == 'jill'))
    qi = q.iter()
    qi.next()
    self.assertEqual(qi.index_list(), None)


class QueryV3Tests(test_utils.NDBTest, BaseQueryTestMixin, IndexListTestMixin):
  """Query tests that use a connection to a Datastore V3 stub."""

  def setUp(self):
    test_utils.NDBTest.setUp(self)
    BaseQueryTestMixin.setUp(self)

  def testConstructorOptionsInteractions(self):
    self.ExpectWarnings()
    qry = Foo.query(projection=[Foo.name, Foo.rate])
    # Keys only overrides projection.
    qry.get(keys_only=True)
    # Projection overrides original projection.
    qry.get(projection=Foo.tags)
    # Cannot override both.
    self.assertRaises(datastore_errors.BadRequestError, qry.get,
                      projection=Foo.tags, keys_only=True)

    qry = Foo.query(projection=[Foo.name, Foo.rate], distinct=True)
    # Cannot project something out side the group by.
    self.assertRaises(datastore_errors.BadRequestError, qry.get,
                      projection=Foo.tags)
    # Can project a subset of the group by.
    qry.get(projection=Foo.name)
    # Keys only overrides projection but a projection is required for group_by.
    self.assertRaises(datastore_errors.BadRequestError,
                      qry.get, keys_only=True)

  def testCursorsForMultiQuery(self):
    # Only relevant for V3 since V1 has per result cursors.
    # TODO(pcostello): This should throw a better error.
    q1 = query.Query(kind='Foo').filter(Foo.tags == 'jill').order(Foo.name)
    q2 = query.Query(kind='Foo').filter(Foo.tags == 'joe').order(Foo.name)
    qq = query._MultiQuery([q1, q2])
    it = qq.iter()

    it.next()
    it.cursor_before()  # Start cursor
    self.assertRaises(AttributeError, it.cursor_after)

    it.next()
    it.cursor_before()  # Start of second query
    it.cursor_after()  # End of batch cursor

    self.assertFalse(it.has_next())


@real_unittest.skipUnless(datastore_pbs._CLOUD_DATASTORE_ENABLED,
                          "V1 must be supported to run V1 tests.")
class QueryV1Tests(test_utils.NDBCloudDatastoreV1Test, BaseQueryTestMixin):
  """Query tests that use a connection to a Cloud Datastore V1 stub."""

  def setUp(self):
    test_utils.NDBCloudDatastoreV1Test.setUp(self)
    BaseQueryTestMixin.setUp(self)

  def testConstructorOptionsInteractions(self):
    self.ExpectWarnings()
    qry = Foo.query(projection=[Foo.name, Foo.rate])
    # Keys only overrides projection.
    qry.get(keys_only=True)
    # Projection overrides original projection.
    qry.get(projection=Foo.tags)
    # Can override both.
    qry.get(projection=Foo.tags, keys_only=True)
    qry = Foo.query(projection=[Foo.name, Foo.rate], distinct=True)
    # Cannot project something out side the group by.
    self.assertRaises(datastore_errors.BadRequestError, qry.get,
                      projection=Foo.tags)
    # Can project a subset of the group by.
    qry.get(projection=Foo.name)
    # Keys only overrides projection but a projection is required for group_by.
    self.assertRaises(datastore_errors.BadRequestError,
                      qry.get, keys_only=True)


if __name__ == '__main__':
  unittest.main()
