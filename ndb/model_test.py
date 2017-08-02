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

"""Tests for model.py."""

import datetime
import difflib
import os
import pickle
import re

from .google_imports import datastore_errors
from .google_imports import datastore_pbs
from .google_imports import datastore_types
from .google_imports import db
from .google_imports import entity_pb
from .google_imports import memcache
from .google_imports import namespace_manager
from .google_imports import users
from .google_test_imports import real_unittest
from .google_test_imports import unittest

try:
  import json
except ImportError:
  import simplejson as json

from . import context
from . import eventloop
from . import key
from . import model
from . import query
from . import tasklets
from . import test_utils

TESTUSER = users.User('test@example.com', 'example.com', '123')
AMSTERDAM = model.GeoPt(52.35, 4.9166667)

GOLDEN_PB = """\
key <
  app: "ndb-test-app-id"
  path <
    Element {
      type: "Model"
      id: 42
    }
  >
>
entity_group <
  Element {
    type: "Model"
    id: 42
  }
>
property <
  name: "b"
  value <
    booleanValue: true
  >
  multiple: false
>
property <
  name: "d"
  value <
    doubleValue: 2.5
  >
  multiple: false
>
property <
  name: "k"
  value <
    ReferenceValue {
      app: "ndb-test-app-id"
      PathElement {
        type: "Model"
        id: 42
      }
    }
  >
  multiple: false
>
property <
  name: "p"
  value <
    int64Value: 42
  >
  multiple: false
>
property <
  name: "q"
  value <
    stringValue: "hello"
  >
  multiple: false
>
property <
  name: "u"
  value <
    UserValue {
      email: "test@example.com"
      auth_domain: "example.com"
      gaiaid: 0
      obfuscated_gaiaid: "123"
    }
  >
  multiple: false
>
property <
  meaning: 9
  name: "xy"
  value <
    PointValue {
      x: 52.35
      y: 4.9166667
    }
  >
  multiple: false
>
"""

INDEXED_PB = re.sub('Model', 'MyModel', GOLDEN_PB)

UNINDEXED_PB = """\
key <
  app: "ndb-test-app-id"
  path <
    Element {
      type: "MyModel"
    }
  >
>
entity_group <
>
raw_property <
  meaning: 14
  name: "b"
  value <
    stringValue: "\\000\\377"
  >
  multiple: false
>
raw_property <
  meaning: 15
  name: "t"
  value <
    stringValue: "Hello world\\341\\210\\264"
  >
  multiple: false
>
"""

PERSON_PB = """\
key <
  app: "ndb-test-app-id"
  path <
    Element {
      type: "Person"
    }
  >
>
entity_group <
>
property <
  name: "address.city"
  value <
    stringValue: "Mountain View"
  >
  multiple: false
>
property <
  name: "address.street"
  value <
    stringValue: "1600 Amphitheatre"
  >
  multiple: false
>
property <
  name: "name"
  value <
    stringValue: "Google"
  >
  multiple: false
>
"""

NESTED_PB = """\
key <
  app: "ndb-test-app-id"
  path <
    Element {
      type: "Person"
    }
  >
>
entity_group <
>
property <
  name: "address.home.city"
  value <
    stringValue: "Mountain View"
  >
  multiple: false
>
property <
  name: "address.home.street"
  value <
    stringValue: "1600 Amphitheatre"
  >
  multiple: false
>
property <
  name: "address.work.city"
  value <
    stringValue: "San Francisco"
  >
  multiple: false
>
property <
  name: "address.work.street"
  value <
    stringValue: "345 Spear"
  >
  multiple: false
>
property <
  name: "name"
  value <
    stringValue: "Google"
  >
  multiple: false
>
"""

RECURSIVE_PB = """\
key <
  app: "ndb-test-app-id"
  path <
    Element {
      type: "Tree"
    }
  >
>
entity_group <
>
property <
  name: "root.left.left.left"
  value <
  >
  multiple: false
>
property <
  name: "root.left.left.name"
  value <
    stringValue: "a1a"
  >
  multiple: false
>
property <
  name: "root.left.left.rite"
  value <
  >
  multiple: false
>
property <
  name: "root.left.name"
  value <
    stringValue: "a1"
  >
  multiple: false
>
property <
  name: "root.left.rite.left"
  value <
  >
  multiple: false
>
property <
  name: "root.left.rite.name"
  value <
    stringValue: "a1b"
  >
  multiple: false
>
property <
  name: "root.left.rite.rite"
  value <
  >
  multiple: false
>
property <
  name: "root.name"
  value <
    stringValue: "a"
  >
  multiple: false
>
property <
  name: "root.rite.left"
  value <
  >
  multiple: false
>
property <
  name: "root.rite.name"
  value <
    stringValue: "a2"
  >
  multiple: false
>
property <
  name: "root.rite.rite.left"
  value <
  >
  multiple: false
>
property <
  name: "root.rite.rite.name"
  value <
    stringValue: "a2b"
  >
  multiple: false
>
property <
  name: "root.rite.rite.rite"
  value <
  >
  multiple: false
>
"""

MULTI_PB = """\
key <
  app: "ndb-test-app-id"
  path <
    Element {
      type: "Person"
    }
  >
>
entity_group <
>
property <
  name: "address"
  value <
    stringValue: "345 Spear"
  >
  multiple: true
>
property <
  name: "address"
  value <
    stringValue: "San Francisco"
  >
  multiple: true
>
property <
  name: "name"
  value <
    stringValue: "Google"
  >
  multiple: false
>
"""

MULTIINSTRUCT_PB = """\
key <
  app: "ndb-test-app-id"
  path <
    Element {
      type: "Person"
    }
  >
>
entity_group <
>
property <
  name: "address.label"
  value <
    stringValue: "work"
  >
  multiple: false
>
property <
  name: "address.line"
  value <
    stringValue: "345 Spear"
  >
  multiple: true
>
property <
  name: "address.line"
  value <
    stringValue: "San Francisco"
  >
  multiple: true
>
property <
  name: "name"
  value <
    stringValue: "Google"
  >
  multiple: false
>
"""

MULTISTRUCT_PB = """\
key <
  app: "ndb-test-app-id"
  path <
    Element {
      type: "Person"
    }
  >
>
entity_group <
>
property <
  name: "address.label"
  value <
    stringValue: "work"
  >
  multiple: true
>
property <
  name: "address.text"
  value <
    stringValue: "San Francisco"
  >
  multiple: true
>
property <
  name: "address.label"
  value <
    stringValue: "home"
  >
  multiple: true
>
property <
  name: "address.text"
  value <
    stringValue: "Mountain View"
  >
  multiple: true
>
property <
  name: "name"
  value <
    stringValue: "Google"
  >
  multiple: false
>
"""


class BaseModelTestMixin(object):

  def assertBetween(self, x, a, b):
    '''Asserts a <= x <= b.'''
    if not a <= x <= b:
      self.fail('%s is not between %s and %s' % (x, a, b))

  def setUp(self):
    pass

  def tearDown(self):
    self.assertTrue(model.Model._properties == {})
    self.assertTrue(model.Expando._properties == {})

  the_module = model

  def testConstruct(self):
    self.assertRaises(TypeError,
                      model.Model,
                      'Unsupported positional argument')

  def testKey(self):
    m = model.Model()
    self.assertEqual(m.key, None)
    k = model.Key(flat=['ParentModel', 42, 'Model', 'foobar'])
    m.key = k
    self.assertEqual(m.key, k)
    del m.key
    self.assertEqual(m.key, None)
    # incomplete key
    k2 = model.Key(flat=['ParentModel', 42, 'Model', None])
    m.key = k2
    self.assertEqual(m.key, k2)

  def testIncompleteKey(self):
    m = model.Model()
    k = model.Key(flat=['Model', None])
    m.key = k
    pb = m._to_pb()
    m2 = model.Model._from_pb(pb)
    self.assertEqual(m2, m)

  def testIdAndParent(self):
    p = model.Key('ParentModel', 'foo')

    # key name
    m = model.Model(id='bar')
    m2 = model.Model._from_pb(m._to_pb())
    self.assertEqual(m2.key, model.Key('Model', 'bar'))

    # key name + parent
    m = model.Model(id='bar', parent=p)
    m2 = model.Model._from_pb(m._to_pb())
    self.assertEqual(m2.key, model.Key('ParentModel', 'foo', 'Model', 'bar'))

    # key id
    m = model.Model(id=42)
    m2 = model.Model._from_pb(m._to_pb())
    self.assertEqual(m2.key, model.Key('Model', 42))

    # key id + parent
    m = model.Model(id=42, parent=p)
    m2 = model.Model._from_pb(m._to_pb())
    self.assertEqual(m2.key, model.Key('ParentModel', 'foo', 'Model', 42))

    # parent
    m = model.Model(parent=p)
    m2 = model.Model._from_pb(m._to_pb())
    self.assertEqual(m2.key, model.Key('ParentModel', 'foo', 'Model', None))

    # not key -- invalid
    self.assertRaises(datastore_errors.BadValueError, model.Model, key='foo')

    # wrong key kind -- invalid
    k = model.Key('OtherModel', 'bar')

    class MyModel(model.Model):
      pass
    self.assertRaises(model.KindError, MyModel, key=k)

    # incomplete parent -- invalid
    p2 = model.Key('ParentModel', None)
    self.assertRaises(datastore_errors.BadArgumentError, model.Model,
                      parent=p2)
    self.assertRaises(datastore_errors.BadArgumentError, model.Model,
                      id='bar', parent=p2)

    # key + id -- invalid
    k = model.Key('Model', 'bar')
    self.assertRaises(datastore_errors.BadArgumentError, model.Model, key=k,
                      id='bar')

    # key + parent -- invalid
    k = model.Key('Model', 'bar', parent=p)
    self.assertRaises(datastore_errors.BadArgumentError, model.Model, key=k,
                      parent=p)

    # key + id + parent -- invalid
    self.assertRaises(datastore_errors.BadArgumentError, model.Model, key=k,
                      id='bar', parent=p)

  def testNamespaceAndApp(self):
    m = model.Model(namespace='')
    self.assertEqual(m.key.namespace(), '')
    m = model.Model(namespace='x')
    self.assertEqual(m.key.namespace(), 'x')
    m = model.Model(app='y')
    self.assertEqual(m.key.app(), 'y')

  def testNamespaceAndAppErrors(self):
    self.assertRaises(datastore_errors.BadArgumentError,
                      model.Model, key=model.Key('X', 1), namespace='')
    self.assertRaises(datastore_errors.BadArgumentError,
                      model.Model, key=model.Key('X', 1), namespace='x')
    self.assertRaises(datastore_errors.BadArgumentError,
                      model.Model, key=model.Key('X', 1), app='y')

  def testPropsOverrideConstructorArgs(self):
    class MyModel(model.Model):
      key = model.StringProperty()
      id = model.StringProperty()
      app = model.StringProperty()
      namespace = model.StringProperty()
      parent = model.StringProperty()
    root = model.Key('Root', 1, app='app', namespace='ns')
    key = model.Key(MyModel, 42, parent=root)

    a = MyModel(_key=key)
    self.assertEqual(a._key, key)
    self.assertEqual(a.key, None)

    b = MyModel(_id=42, _app='app', _namespace='ns', _parent=root)
    self.assertEqual(b._key, key)
    self.assertEqual(b.key, None)
    self.assertEqual(b.id, None)
    self.assertEqual(b.app, None)
    self.assertEqual(b.namespace, None)
    self.assertEqual(b.parent, None)

    c = MyModel(key='key', id='id', app='app', namespace='ns', parent='root')
    self.assertEqual(c._key, None)
    self.assertEqual(c.key, 'key')
    self.assertEqual(c.id, 'id')
    self.assertEqual(c.app, 'app')
    self.assertEqual(c.namespace, 'ns')
    self.assertEqual(c.parent, 'root')

    d = MyModel(_id=42, _app='app', _namespace='ns', _parent=root,
                key='key', id='id', app='app', namespace='ns', parent='root')
    self.assertEqual(d._key, key)
    self.assertEqual(d.key, 'key')
    self.assertEqual(d.id, 'id')
    self.assertEqual(d.app, 'app')
    self.assertEqual(d.namespace, 'ns')
    self.assertEqual(d.parent, 'root')

  def testAdapter(self):
    class Foo(model.Model):
      name = model.StringProperty()
    ad = model.ModelAdapter()
    foo1 = Foo(name='abc')
    pb1 = ad.entity_to_pb(foo1)
    foo2 = ad.pb_to_entity(pb1)
    self.assertEqual(foo1, foo2)
    self.assertTrue(foo2.key is None)
    pb2 = foo2._to_pb(set_key=False)
    self.assertRaises(model.KindError, ad.pb_to_entity, pb2)
    ad = model.ModelAdapter(Foo)
    foo3 = ad.pb_to_entity(pb2)
    self.assertEqual(foo3, foo2)

    key1 = model.Key(Foo, 1)
    pbk1 = ad.key_to_pb(key1)
    key2 = ad.pb_to_key(pbk1)
    self.assertEqual(key1, key2)

  def testPropertyVerboseNameAttribute(self):
    class Foo(model.Model):
      name = model.StringProperty(verbose_name='Full name')
    np = Foo._properties['name']
    self.assertEqual('Full name', np._verbose_name)

  def testProjectedEntities(self):
    class Foo(model.Expando):
      a = model.StringProperty()
      b = model.StringProperty()

    ent0 = Foo()
    self.assertEqual(ent0._projection, ())

    ent1 = Foo(projection=('a',))
    self.assertEqual(ent1._projection, ('a',))
    self.assertNotEqual(ent0, ent1)
    self.assertEqual(ent1.a, None)
    ent2 = Foo(projection=['a'])
    self.assertEqual(ent2._projection, ('a',))
    self.assertEqual(ent1, ent2)
    self.assertRaises(TypeError, Foo, projection=42)
    self.assertRaises(model.UnprojectedPropertyError, lambda: ent1.b)
    self.assertRaises(model.ReadonlyPropertyError, setattr, ent1, 'a', 'a')
    self.assertRaises(model.ReadonlyPropertyError, setattr, ent1, 'b', 'b')
    # Dynamic property creation should fail also:
    self.assertRaises(model.ReadonlyPropertyError, setattr, ent1, 'c', 'c')

    ent2 = Foo(_projection=['a'])
    self.assertEqual(ent2._projection, ('a',))
    self.assertEqual(repr(ent2), "Foo(_projection=('a',))")
    self.assertRaises(datastore_errors.BadRequestError, ent2.put)

    ent3 = Foo(_projection=('a',), id=42)  # Sets the key
    self.assertEqual(ent3._projection, ('a',))
    self.assertEqual(repr(ent3),
                     "Foo(key=Key('Foo', 42), _projection=('a',))")
    ent3.key.delete()  # No failure

    # Another one that differs only in projection.
    ent4 = Foo(_projection=('a', 'b'), id=42)
    self.assertEqual(ent4._projection, ('a', 'b'))
    self.assertEqual(repr(ent4),
                     "Foo(key=Key('Foo', 42), _projection=('a', 'b'))")
    self.assertNotEqual(ent3, ent4)

  def testProjectedEntities_StructuredProperties(self):
    class Bar(model.Model):
      a = model.StringProperty()
      b = model.StringProperty()

    class Foo(model.Model):
      bar = model.StructuredProperty(Bar)

    foo = Foo(bar=Bar(a='a', b='b'), projection=(('bar.a',)))
    self.assertRaises(model.UnprojectedPropertyError, lambda: foo.bar.b)
    self.assertRaises(AssertionError, Foo, projection=(('bar.a',)))

  def testProjectionToDict(self):
    class Foo(model.Model):
      bar = model.StringProperty()
      baz = model.StringProperty('bletch', repeated=True)
    foo1 = Foo(bar='bar1', baz=['baz1'])
    foo1.put()
    foo2 = Foo.query().get(projection=['bar'])
    self.assertEqual(foo2._to_dict(), {'bar': 'bar1'})
    foo3 = Foo.query().get(projection=[Foo.baz])
    self.assertEqual(foo3._to_dict(), {'baz': ['baz1']})

    class Bar(model.Model):
      foo = model.StructuredProperty(Foo)
      baz = model.StringProperty()
    bar1 = Bar(foo=foo1, baz='baz2')
    bar1.put()
    bar2 = Bar.query().get(projection=['foo.bar'])
    self.assertEqual(bar2.to_dict(), {'foo': {'bar': 'bar1'}})
    bar3 = Bar.query().get(projection=['baz'])
    self.assertEqual(bar3.to_dict(), {'baz': 'baz2'})

  def testBadProjectionErrorRaised(self):
    # Note: Projections are not checked on entity construction;
    # only on queries.
    class Inner(model.Model):
      name = model.StringProperty()
      rank = model.IntegerProperty()
    q = Inner.query()
    q.fetch(projection=['name', 'rank'])
    self.assertRaises(model.InvalidPropertyError,
                      q.fetch, projection=['name.foo'])
    self.assertRaises(model.InvalidPropertyError,
                      q.fetch, projection=['booh'])

    class Outer(model.Expando):
      inner = model.StructuredProperty(Inner)
      blob = model.BlobProperty()
      tag = model.StringProperty()
    q = Outer.query()
    q.fetch(projection=['tag', 'inner.name', 'inner.rank', 'whatever'])
    self.assertRaises(model.InvalidPropertyError,
                      q.fetch, projection=['inner'])
    self.assertRaises(model.InvalidPropertyError,
                      q.fetch, projection=['inner.name.foo'])
    self.assertRaises(model.InvalidPropertyError,
                      q.fetch, projection=['inner.booh'])
    self.assertRaises(model.InvalidPropertyError,
                      q.fetch, projection=['blob'])

  def testQuery(self):
    class MyModel(model.Model):
      p = model.IntegerProperty()

    q = MyModel.query()
    self.assertTrue(isinstance(q, query.Query))
    self.assertEqual(q.kind, 'MyModel')
    self.assertEqual(q.ancestor, None)

    k = model.Key(flat=['Model', 1])
    q = MyModel.query(ancestor=k)
    self.assertEqual(q.kind, 'MyModel')
    self.assertEqual(q.ancestor, k)

    k0 = model.Key(flat=['Model', None])
    self.assertRaises(Exception, MyModel.query, ancestor=k0)

  def testQueryWithFilter(self):
    class MyModel(model.Model):
      p = model.IntegerProperty()

    q = MyModel.query(MyModel.p >= 0)
    self.assertTrue(isinstance(q, query.Query))
    self.assertEqual(q.kind, 'MyModel')
    self.assertEqual(q.ancestor, None)
    self.assertTrue(q.filters is not None)

    q2 = MyModel.query().filter(MyModel.p >= 0)
    self.assertEqual(q.filters, q2.filters)

  def testQueryForNone(self):
    class MyModel(model.Model):
      b = model.BooleanProperty()
      bb = model.BlobProperty(indexed=True)
      d = model.DateProperty()
      f = model.FloatProperty()
      i = model.IntegerProperty()
      k = model.KeyProperty()
      s = model.StringProperty()
      t = model.TimeProperty()
      u = model.UserProperty()
      xy = model.GeoPtProperty()
    m1 = MyModel()
    m1.put()
    m2 = MyModel(
        b=True,
        bb='z',
        d=datetime.date.today(),
        f=3.14,
        i=1,
        k=m1.key,
        s='a',
        t=datetime.time(),
        u=TESTUSER,
        xy=AMSTERDAM,
    )
    m2.put()
    q = MyModel.query(
        MyModel.b == None,
        MyModel.bb == None,
        MyModel.d == None,
        MyModel.f == None,
        MyModel.i == None,
        MyModel.k == None,
        MyModel.s == None,
        MyModel.t == None,
        MyModel.u == None,
        MyModel.xy == None,
    )
    r = q.fetch()
    self.assertEqual(r, [m1])
    qq = [
        MyModel.query(MyModel.b != None),
        MyModel.query(MyModel.bb != None),
        MyModel.query(MyModel.d != None),
        MyModel.query(MyModel.f != None),
        MyModel.query(MyModel.i != None),
        MyModel.query(MyModel.k != None),
        MyModel.query(MyModel.s != None),
        MyModel.query(MyModel.t != None),
        MyModel.query(MyModel.u != None),
        MyModel.query(MyModel.xy != None),
    ]
    for q in qq:
      r = q.fetch()
      self.assertEqual(r, [m2], str(q))

  def testBottom(self):
    a = model._BaseValue(42)
    b = model._BaseValue(42)
    c = model._BaseValue('hello')
    self.assertEqual("_BaseValue(42)", repr(a))
    self.assertEqual("_BaseValue('hello')", repr(c))
    self.assertTrue(a == b)
    self.assertFalse(a != b)
    self.assertTrue(b != c)
    self.assertFalse(b == c)
    self.assertFalse(a == 42)
    self.assertTrue(a != 42)

  def testCompressedValue(self):
    a = model._CompressedValue('xyz')
    b = model._CompressedValue('xyz')
    c = model._CompressedValue('abc')
    self.assertEqual("_CompressedValue('abc')", repr(c))
    self.assertTrue(a == b)
    self.assertFalse(a != b)
    self.assertTrue(b != c)
    self.assertFalse(b == c)
    self.assertFalse(a == 'xyz')
    self.assertTrue(a != 'xyz')

  def testCompressedNestedFakeValue(self):
    class InternalModel(model.Model):
      json_property = model.JsonProperty(compressed=True, indexed=False)
      normal_property = model.StringProperty()

    class Model(model.Model):
      internal = model.StructuredProperty(InternalModel)

    key = Model(internal=InternalModel(json_property={'some': 'object'},
                                       normal_property='string')).put()
    # Redefine models with missing compressed property.

    class InternalModel(model.Model):
      normal_property = model.StringProperty()

    class Model(model.Model):
      internal = model.StructuredProperty(InternalModel)

    ent = key.get()
    self.assertTrue('json_property' in ent.internal._values)

  def testUnindexibleNestedFakeValue(self):
    class InternalModel(model.Model):
      normal_property = model.StringProperty(indexed=False)

    class Model(model.Model):
      internal = model.StructuredProperty(InternalModel)

    a_long_string = 'a' * 2000
    key = Model(internal=InternalModel(normal_property=a_long_string)).put()
    # Redefine models with missing compressed property.

    class InternalModel(model.Model):
      pass

    class Model(model.Model):
      internal = model.StructuredProperty(InternalModel)

    ent = key.get()
    self.assertTrue('normal_property' in ent.internal._values)

    # TODO(pcostello): This should not fail because the value should maintain
    # its indexing properties. However, this requires a major version change.
    self.assertRaises(datastore_errors.BadRequestError, ent.put)

  def testUnindexibleFakeValue(self):
    class Model(model.Model):
      prop = model.StringProperty(indexed=False)

    a_long_string = 'a' * 2000
    key = Model(prop=a_long_string).put()
    # Redefine models with missing compressed property.

    class Model(model.Model):
      pass

    ent = key.get()
    self.assertTrue('prop' in ent._values)
    # This will succeed because deserialization maintains the indexed settings
    # of properties.
    ent.put()

  def testProperty(self):
    class MyModel(model.Model):
      b = model.BooleanProperty()
      p = model.IntegerProperty()
      q = model.StringProperty()
      d = model.FloatProperty()
      k = model.KeyProperty()
      u = model.UserProperty()
      xy = model.GeoPtProperty()

    ent = MyModel()
    k = model.Key(flat=['MyModel', 42])
    ent.key = k
    MyModel.b._set_value(ent, True)
    MyModel.p._set_value(ent, 42)
    MyModel.q._set_value(ent, 'hello')
    MyModel.d._set_value(ent, 2.5)
    MyModel.k._set_value(ent, k)
    MyModel.u._set_value(ent, TESTUSER)
    MyModel.xy._set_value(ent, AMSTERDAM)
    self.assertEqual(MyModel.b._get_value(ent), True)
    self.assertEqual(MyModel.p._get_value(ent), 42)
    self.assertEqual(MyModel.q._get_value(ent), 'hello')
    self.assertEqual(MyModel.d._get_value(ent), 2.5)
    self.assertEqual(MyModel.k._get_value(ent), k)
    self.assertEqual(MyModel.u._get_value(ent), TESTUSER)
    self.assertEqual(MyModel.xy._get_value(ent), AMSTERDAM)
    pb = self.conn.adapter.entity_to_pb(ent)
    self.assertEqual(str(pb), INDEXED_PB)

    ent = MyModel._from_pb(pb)
    self.assertEqual(ent._get_kind(), 'MyModel')
    k = model.Key(flat=['MyModel', 42])
    self.assertEqual(ent.key, k)
    self.assertEqual(MyModel.p._get_value(ent), 42)
    self.assertEqual(MyModel.q._get_value(ent), 'hello')
    self.assertEqual(MyModel.d._get_value(ent), 2.5)
    self.assertEqual(MyModel.k._get_value(ent), k)

  def testDeletingPropertyValue(self):
    class MyModel(model.Model):
      a = model.StringProperty()
    m = MyModel()

    # Initially it isn't there (but the value defaults to None).
    self.assertEqual(m.a, None)
    self.assertFalse(MyModel.a._has_value(m))

    # Explicit None assignment makes it present.
    m.a = None
    self.assertEqual(m.a, None)
    self.assertTrue(MyModel.a._has_value(m))

    # Deletion restores the initial state.
    del m.a
    self.assertEqual(m.a, None)
    self.assertFalse(MyModel.a._has_value(m))

    # Redundant deletions are okay.
    del m.a
    self.assertEqual(m.a, None)
    self.assertFalse(MyModel.a._has_value(m))

    # Deleted/missing values are serialized and considered present
    # when deserialized.
    pb = m._to_pb()
    m = MyModel._from_pb(pb)
    self.assertEqual(m.a, None)
    self.assertTrue(MyModel.a._has_value(m))

  def testDefaultPropertyValue(self):
    class MyModel(model.Model):
      a = model.StringProperty(default='a')
      b = model.StringProperty(default='')
    m = MyModel()

    # Initial values equal the defaults.
    self.assertEqual(m.a, 'a')
    self.assertEqual(m.b, '')
    self.assertFalse(MyModel.a._has_value(m))
    self.assertFalse(MyModel.b._has_value(m))

    # Setting values erases the defaults.
    m.a = ''
    m.b = 'b'
    self.assertEqual(m.a, '')
    self.assertEqual(m.b, 'b')
    self.assertTrue(MyModel.a._has_value(m))
    self.assertTrue(MyModel.b._has_value(m))

    # Deleting values restores the defaults.
    del m.a
    del m.b
    self.assertEqual(m.a, 'a')
    self.assertEqual(m.b, '')
    self.assertFalse(MyModel.a._has_value(m))
    self.assertFalse(MyModel.b._has_value(m))

    # Serialization makes the default values explicit.
    pb = m._to_pb()
    m = MyModel._from_pb(pb)
    self.assertEqual(m.a, 'a')
    self.assertEqual(m.b, '')
    self.assertTrue(MyModel.a._has_value(m))
    self.assertTrue(MyModel.b._has_value(m))

  def testComparingExplicitAndImplicitValue(self):
    class MyModel(model.Model):
      a = model.StringProperty(default='a')
      b = model.StringProperty()
    m1 = MyModel(b=None)
    m2 = MyModel()
    self.assertEqual(m1, m2)
    m1.a = 'a'
    self.assertEqual(m1, m2)

  def testRequiredProperty(self):
    class MyModel(model.Model):
      a = model.StringProperty(required=True)
      b = model.StringProperty()  # Never counts as uninitialized
    self.assertEqual(repr(MyModel.a), "StringProperty('a', required=True)")
    m = MyModel()

    # Never-assigned values are considered uninitialized.
    self.assertEqual(m._find_uninitialized(), set(['a']))
    self.assertRaises(datastore_errors.BadValueError, m._check_initialized)
    self.assertRaises(datastore_errors.BadValueError, m._to_pb)

    # Empty string is fine.
    m.a = ''
    self.assertFalse(m._find_uninitialized())
    m._check_initialized()
    m._to_pb()

    # Non-empty string is fine (of course).
    m.a = 'foo'
    self.assertFalse(m._find_uninitialized())
    m._check_initialized()
    m._to_pb()

    # Deleted value is not fine.
    del m.a
    self.assertEqual(m._find_uninitialized(), set(['a']))
    self.assertRaises(datastore_errors.BadValueError, m._check_initialized)
    self.assertRaises(datastore_errors.BadValueError, m._to_pb)

    # Explicitly assigned None is *not* fine.
    m.a = None
    self.assertEqual(m._find_uninitialized(), set(['a']))
    self.assertRaises(datastore_errors.BadValueError, m._check_initialized)
    self.assertRaises(datastore_errors.BadValueError, m._to_pb)

    # Check that b is still unset.
    self.assertFalse(MyModel.b._has_value(m))

  def testRequiredDefault(self):
    # Test combining required and default.
    class MyModel(model.Model):
      a = model.StringProperty(required=True, default='a')
      b = model.StringProperty(required=True, default='')
    x = MyModel()
    self.assertEqual(x.a, 'a')
    self.assertEqual(x.b, '')
    x._to_pb()  # Okay.
    x.a = ''
    self.assertEqual(x.a, '')
    x._to_pb()  # Okay.
    x.a = None
    self.assertEqual(x.a, None)
    self.assertRaises(datastore_errors.BadValueError, x._to_pb)  # Fail.
    x.a = ''
    x.b = None
    self.assertEqual(x.a, '')
    self.assertEqual(x.b, None)
    self.assertRaises(datastore_errors.BadValueError, x._to_pb)  # Fail.

  def testRepeatedRequiredDefaultConflict(self):
    # Don't combine repeated=True with required=True or default=<non-None>
    class MyModel(model.Model):
      self.assertRaises(Exception,
                        model.StringProperty, repeated=True, default='')
      self.assertRaises(Exception,
                        model.StringProperty, repeated=True, required=True)
      self.assertRaises(Exception,
                        model.StringProperty,
                        repeated=True, required=True, default='')
    self.assertEqual('MyModel()', repr(MyModel()))

  def testKeyProperty(self):
    class RefModel(model.Model):
      pass

    class FancyModel(model.Model):

      @classmethod
      def _get_kind(cls):
        return 'Fancy'

    class FancierModel(model.Model):

      @classmethod
      def _get_kind(cls):
        return u'Fancier'

    class MyModel(model.Model):
      basic = model.KeyProperty(kind=None)
      ref = model.KeyProperty(kind=RefModel)
      refs = model.KeyProperty(kind=RefModel, repeated=True)
      fancy = model.KeyProperty(kind=FancyModel)
      fancee = model.KeyProperty(kind='Fancy')
      fancier = model.KeyProperty(kind=FancierModel)

    a = MyModel(basic=model.Key('Foo', 1),
                ref=model.Key(RefModel, 1),
                refs=[model.Key(RefModel, 2), model.Key(RefModel, 3)],
                fancy=model.Key(FancyModel, 1),
                fancee=model.Key(FancyModel, 2),
                fancier=model.Key('Fancier', 1))
    a.put()
    b = a.key.get()
    self.assertEqual(a, b)
    # Try some assignments.
    b.basic = model.Key('Bar', 1)
    b.ref = model.Key(RefModel, 2)
    b.refs = [model.Key(RefModel, 4)]
    # Try the repr().
    self.assertEqual(repr(MyModel.basic), "KeyProperty('basic')")
    self.assertEqual(repr(MyModel.ref), "KeyProperty('ref', kind='RefModel')")
    # Try some errors declaring properties.
    self.assertRaises(TypeError, model.KeyProperty, kind=42)  # Non-class.
    self.assertRaises(TypeError, model.KeyProperty, kind=int)  # Non-Model.
    # Try some errors assigning property values.
    self.assertRaises(datastore_errors.BadValueError,
                      setattr, a, 'ref', model.Key('Bar', 1))
    self.assertRaises(datastore_errors.BadValueError,
                      setattr, a, 'refs', [model.Key('Bar', 1)])

  def testKeyPropertyNonUtf8Kind(self):

    class FanciestModel(model.Model):

      @classmethod
      def _get_kind(cls):
        return '\xff'

    class MyModel(model.Model):
      fanciest = model.KeyProperty(kind=FanciestModel)
      faanceest = model.KeyProperty(kind=u'\xff')

    a = MyModel(fanciest=model.Key(FanciestModel, 1))
    a.put()
    b = a.key.get()
    self.assertEqual(a, b)

  def testKeyPropertyPositionalKind(self):
    class RefModel(model.Model):
      pass

    class MyModel(model.Model):
      ref0 = model.KeyProperty('REF0')
      ref1 = model.KeyProperty(RefModel)
      ref2 = model.KeyProperty(RefModel, 'REF2')
      ref3 = model.KeyProperty('REF3', RefModel)
      ref4 = model.KeyProperty(None)
      ref5 = model.KeyProperty(None, None)
      ref6 = model.KeyProperty(RefModel, None)
      ref7 = model.KeyProperty(None, RefModel)
      ref8 = model.KeyProperty('REF8', None)
      ref9 = model.KeyProperty(None, 'REF9')

    self.assertEqual(MyModel.ref0._kind, None)
    self.assertEqual(MyModel.ref1._kind, 'RefModel')
    self.assertEqual(MyModel.ref2._kind, 'RefModel')
    self.assertEqual(MyModel.ref3._kind, 'RefModel')
    self.assertEqual(MyModel.ref4._kind, None)
    self.assertEqual(MyModel.ref5._kind, None)
    self.assertEqual(MyModel.ref6._kind, 'RefModel')
    self.assertEqual(MyModel.ref7._kind, 'RefModel')
    self.assertEqual(MyModel.ref8._kind, None)
    self.assertEqual(MyModel.ref9._kind, None)

    self.assertEqual(MyModel.ref0._name, 'REF0')
    self.assertEqual(MyModel.ref1._name, 'ref1')
    self.assertEqual(MyModel.ref2._name, 'REF2')
    self.assertEqual(MyModel.ref3._name, 'REF3')
    self.assertEqual(MyModel.ref4._name, 'ref4')
    self.assertEqual(MyModel.ref5._name, 'ref5')
    self.assertEqual(MyModel.ref6._name, 'ref6')
    self.assertEqual(MyModel.ref7._name, 'ref7')
    self.assertEqual(MyModel.ref8._name, 'REF8')
    self.assertEqual(MyModel.ref9._name, 'REF9')

    for args in [(1,), (int,), (1, int), (int, 1),
                 ('x', 'y'), (RefModel, RefModel),
                 (None, int), (int, None), (None, 1), (1, None)]:
      self.assertRaises(TypeError, model.KeyProperty, *args)

    self.assertRaises(TypeError, model.KeyProperty, RefModel, kind='K')
    self.assertRaises(TypeError, model.KeyProperty, None, RefModel, kind='k')
    self.assertRaises(TypeError, model.KeyProperty, 'n', RefModel, kind='k')

  def testPropertyCreationCounter(self):
    class MyModel(model.Model):
      foo = model.StringProperty()
      bar = model.StringProperty()
      baz = model.StringProperty()
    self.assertTrue(MyModel.foo._creation_counter <
                    MyModel.bar._creation_counter <
                    MyModel.baz._creation_counter)

  def testBlobKeyProperty(self):
    class MyModel(model.Model):
      image = model.BlobKeyProperty()
    test_blobkey = datastore_types.BlobKey('testkey123')
    m = MyModel()
    m.image = test_blobkey
    m.put()

    m = m.key.get()

    self.assertTrue(isinstance(m.image, datastore_types.BlobKey))
    self.assertEqual(str(m.image), str(test_blobkey))

  def testChoicesProperty(self):
    class MyModel(model.Model):
      a = model.StringProperty(choices=['a', 'b', 'c'])
      b = model.IntegerProperty(choices=[1, 2, 3], repeated=True)
    m = MyModel(a='a', b=[1, 2])
    m.a = 'b'
    m.a = None
    m.b = [1, 1, 3]
    m.b = []
    self.assertRaises(datastore_errors.BadValueError,
                      setattr, m, 'a', 'A')
    self.assertRaises(datastore_errors.BadValueError,
                      setattr, m, 'b', [42])

  def testValidatorProperty(self):
    def my_validator(prop, value):
      value = value.lower()
      if not value.startswith('a'):
        raise datastore_errors.BadValueError('%s does not start with "a"' %
                                             prop._name)
      return value

    class MyModel(model.Model):
      a = model.StringProperty(validator=my_validator)
      foos = model.StringProperty(validator=my_validator, repeated=True)
    m = MyModel()
    m.a = 'ABC'
    self.assertEqual(m.a, 'abc')
    self.assertRaises(datastore_errors.BadValueError,
                      setattr, m, 'a', 'def')
    m.foos = ['ABC', 'ABC', 'ABC']
    self.assertEqual(m.foos, ['abc', 'abc', 'abc'])
    self.assertRaises(datastore_errors.BadValueError,
                      setattr, m, 'foos', ['def'])

  def testUnindexedProperty(self):
    class MyModel(model.Model):
      t = model.TextProperty()
      b = model.BlobProperty()

    ent = MyModel()
    MyModel.t._set_value(ent, u'Hello world\u1234')
    MyModel.b._set_value(ent, '\x00\xff')
    self.assertEqual(MyModel.t._get_value(ent), u'Hello world\u1234')
    self.assertEqual(MyModel.b._get_value(ent), '\x00\xff')
    pb = ent._to_pb()
    self.assertEqual(str(pb), UNINDEXED_PB)

    ent = MyModel._from_pb(pb)
    self.assertEqual(ent._get_kind(), 'MyModel')
    k = model.Key(flat=['MyModel', None])
    self.assertEqual(ent.key, k)
    self.assertEqual(MyModel.t._get_value(ent), u'Hello world\u1234')
    self.assertEqual(MyModel.b._get_value(ent), '\x00\xff')

  def testUserPropertyAutoFlags(self):
    # Can't combind auto_current_user* with repeated.
    self.assertRaises(ValueError, model.UserProperty,
                      repeated=True, auto_current_user_add=True)
    self.assertRaises(ValueError, model.UserProperty,
                      repeated=True, auto_current_user=True)

    # Define a model with user properties.
    class MyModel(model.Model):
      u0 = model.UserProperty(auto_current_user_add=True)
      u1 = model.UserProperty(auto_current_user=True)

    # Without a current user, these remain None.
    x = MyModel()
    k = x.put()
    y = k.get()
    self.assertTrue(y.u0 is None)
    self.assertTrue(y.u1 is None)

    try:
      # When there is a current user, it sets both.
      os.environ['USER_EMAIL'] = 'test@example.com'
      x = MyModel()
      k = x.put()
      y = k.get()
      self.assertFalse(y.u0 is None)
      self.assertFalse(y.u1 is None)
      self.assertEqual(y.u0, users.User(email='test@example.com'))
      self.assertEqual(y.u1, users.User(email='test@example.com'))

      # When the current user changes, only u1 is changed.
      os.environ['USER_EMAIL'] = 'test2@example.com'
      x.put()
      y = k.get()
      self.assertEqual(y.u0, users.User(email='test@example.com'))
      self.assertEqual(y.u1, users.User(email='test2@example.com'))

      # When we delete the property values, both are reset.
      del x.u0
      del x.u1
      x.put()
      y = k.get()
      self.assertEqual(y.u0, users.User(email='test2@example.com'))
      self.assertEqual(y.u1, users.User(email='test2@example.com'))

      # When we set them to None, u0 stays None, u1 is reset.
      x.u0 = None
      x.u1 = None
      x.put()
      y = k.get()
      self.assertEqual(y.u0, None)
      self.assertEqual(y.u1, users.User(email='test2@example.com'))

    finally:
      # Reset environment.
      del os.environ['USER_EMAIL']

  def testPickleProperty(self):
    class MyModel(model.Model):
      pkl = model.PickleProperty()
    sample = {'one': 1, 2: [1, 2, '3'], 3.: model.Model}
    ent = MyModel(pkl=sample)
    ent.put()
    ent2 = ent.key.get()
    self.assertTrue(ent2.pkl == sample)

  def testJsonProperty(self):
    class MyModel(model.Model):
      pkl = model.JsonProperty('pkl')
    sample = [1, 2, {'a': 'one', 'b': [1, 2]}, 'xyzzy', [1, 2, 3]]
    ent = MyModel(pkl=sample)
    ent.put()
    ent2 = ent.key.get()
    self.assertTrue(ent2.pkl == sample)

  def testJsonPropertyTypeRestricted(self):
    class MyModel(model.Model):
      pkl = model.JsonProperty(json_type=dict)
      lst = model.JsonProperty(json_type=list)
    sample = [1, 2, {'a': 'one', 'b': [1, 2]}, 'xyzzy', [1, 2, 3]]
    sample2 = {'foo': 2, 'bar': [1, '2', 3, ['foo', 2]]}

    self.assertRaises(TypeError, MyModel, pkl=sample)
    self.assertRaises(TypeError, MyModel, lst=sample2)

    ent = MyModel(pkl=sample2, lst=sample)
    ent.put()
    ent2 = ent.key.get()
    self.assertTrue(ent2.pkl == sample2)
    self.assertTrue(ent2.lst == sample)

  def DateAndOrTimePropertyTest(self, propclass, nowfunc, t1, t2):
    class ClockInOut(model.Model):
      ctime = propclass(auto_now_add=True)
      mtime = propclass(auto_now=True)

    class Person(model.Model):
      name = model.StringProperty()
      ctime = propclass(auto_now_add=True)
      mtime = propclass(auto_now=True)
      atime = propclass()
      times = propclass(repeated=True)
      struct = model.StructuredProperty(ClockInOut)
      repstruct = model.StructuredProperty(ClockInOut, repeated=True)
      localstruct = model.LocalStructuredProperty(ClockInOut)
      replocalstruct = model.LocalStructuredProperty(ClockInOut, repeated=True)

    p = Person(id=1, struct=ClockInOut(), repstruct=[ClockInOut()],
               localstruct=ClockInOut(), replocalstruct=[ClockInOut()])
    p.atime = t1
    p.times = [t1, t2]
    self.assertEqual(p.ctime, None)
    self.assertEqual(p.mtime, None)
    self.assertEqual(p.struct.ctime, None)
    self.assertEqual(p.struct.mtime, None)
    self.assertEqual(p.repstruct[0].ctime, None)
    self.assertEqual(p.repstruct[0].mtime, None)
    self.assertEqual(p.localstruct.ctime, None)
    self.assertEqual(p.localstruct.mtime, None)
    self.assertEqual(p.replocalstruct[0].ctime, None)
    self.assertEqual(p.replocalstruct[0].mtime, None)
    timepoint0 = nowfunc()
    p.put()
    timepoint1 = nowfunc()
    self.assertBetween(p.ctime, timepoint0, timepoint1)
    self.assertBetween(p.mtime, timepoint0, timepoint1)
    self.assertBetween(p.struct.ctime, timepoint0, timepoint1)
    self.assertBetween(p.struct.mtime, timepoint0, timepoint1)
    self.assertBetween(p.repstruct[0].ctime, timepoint0, timepoint1)
    self.assertBetween(p.repstruct[0].mtime, timepoint0, timepoint1)
    self.assertBetween(p.localstruct.ctime, timepoint0, timepoint1)
    self.assertBetween(p.localstruct.mtime, timepoint0, timepoint1)
    self.assertBetween(p.replocalstruct[0].ctime, timepoint0, timepoint1)
    self.assertBetween(p.replocalstruct[0].mtime, timepoint0, timepoint1)
    pb = p._to_pb()
    q = Person._from_pb(pb)
    self.assertEqual(q.ctime, p.ctime)
    self.assertEqual(q.mtime, p.mtime)
    self.assertEqual(q.struct.ctime, p.struct.ctime)
    self.assertEqual(q.struct.mtime, p.struct.mtime)
    self.assertEqual(q.repstruct[0].ctime, p.repstruct[0].ctime)
    self.assertEqual(q.repstruct[0].mtime, p.repstruct[0].mtime)
    self.assertEqual(q.localstruct.ctime, p.localstruct.ctime)
    self.assertEqual(q.localstruct.mtime, p.localstruct.mtime)
    self.assertEqual(q.replocalstruct[0].ctime, p.replocalstruct[0].ctime)
    self.assertEqual(q.replocalstruct[0].mtime, p.replocalstruct[0].mtime)
    self.assertEqual(q.atime, t1)
    self.assertEqual(q.times, [t1, t2])

    property = pb.add_property()
    property.set_name('repstruct.country')
    property.mutable_value().set_stringvalue('iceland')
    q = Person._from_pb(pb)
    self.assertNotEqual(None, q)
    self.assertEqual(q.repstruct[0].ctime, p.repstruct[0].ctime)

  def PrepareForPutTests(self, propclass):
    class AuditedRecord(model.Model):
      created = propclass(auto_now_add=True)
      modified = propclass(auto_now=True)
    record = AuditedRecord(id=1)
    record._to_pb()
    self.assertEqual(record.created, None,
                     'auto_now_add set before entity was put')
    self.assertEqual(record.modified, None,
                     'auto_now set before entity was put')

  def MultiDateAndOrTimePropertyTest(self, *args):
    ctx = tasklets.get_context()

    # Run tests against datastore
    self.DateAndOrTimePropertyTest(*args)
    self.PrepareForPutTests(args[0])
    ctx.set_datastore_policy(False)

    # Run tests against process cache
    ctx.set_cache_policy(True)
    self.DateAndOrTimePropertyTest(*args)
    self.PrepareForPutTests(args[0])

  def MultiDateAndOrTimePropertyTestMemcache(self, *args):
    ctx = tasklets.get_context()

    # Run tests against memcache
    ctx.set_memcache_policy(True)
    self.DateAndOrTimePropertyTest(*args)
    self.PrepareForPutTests(args[0])
    ctx.set_memcache_policy(False)

  def testDateTimeProperty(self):
    self.MultiDateAndOrTimePropertyTest(model.DateTimeProperty,
                                        datetime.datetime.utcnow,
                                        datetime.datetime(1982, 12, 1, 9, 0, 0),
                                        datetime.datetime(1995, 4, 15, 5, 0, 0))

  def testDateTimePropertyMemcache(self):
    self.MultiDateAndOrTimePropertyTestMemcache(
        model.DateTimeProperty,
        datetime.datetime.utcnow,
        datetime.datetime(1982, 12, 1, 9, 0, 0),
        datetime.datetime(1995, 4, 15, 5, 0, 0))

  def testDateProperty(self):
    self.MultiDateAndOrTimePropertyTest(model.DateProperty,
                                        lambda: datetime.datetime.utcnow().date(),
                                        datetime.date(1982, 12, 1),
                                        datetime.date(1995, 4, 15))

  def testDatePropertyMemcache(self):
    self.MultiDateAndOrTimePropertyTestMemcache(
        model.DateProperty,
        lambda: datetime.datetime.utcnow().date(),
        datetime.date(1982, 12, 1),
        datetime.date(1995, 4, 15))

  def testTimeProperty(self):
    self.MultiDateAndOrTimePropertyTest(model.TimeProperty,
                                        lambda: datetime.datetime.utcnow().time(),
                                        datetime.time(9, 0, 0),
                                        datetime.time(5, 0, 0, 500))

  def testTimePropertyMemcache(self):
    self.MultiDateAndOrTimePropertyTestMemcache(
        model.TimeProperty,
        lambda: datetime.datetime.utcnow().time(),
        datetime.time(9, 0, 0),
        datetime.time(5, 0, 0, 500))

  def testStructuredProperty(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()

    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(Address)

    p = Person()
    p.name = 'Google'
    a = Address(street='1600 Amphitheatre')
    p.address = a
    p.address.city = 'Mountain View'
    self.assertEqual(Person.name._get_value(p), 'Google')
    self.assertEqual(p.name, 'Google')
    self.assertEqual(Person.address._get_value(p), a)
    self.assertEqual(Address.street._get_value(a), '1600 Amphitheatre')
    self.assertEqual(Address.city._get_value(a), 'Mountain View')

    pb = p._to_pb()
    self.assertEqual(str(pb), PERSON_PB)

    p = Person._from_pb(pb)
    self.assertEqual(p.name, 'Google')
    self.assertEqual(p.address.street, '1600 Amphitheatre')
    self.assertEqual(p.address.city, 'Mountain View')
    self.assertEqual(p.address, a)

    new_property = """\
property <
  name: "address.country"
  value <
    stringValue: "USA"
  >
  multiple: false
>
"""
    person_with_extra_data = PERSON_PB + new_property
    prop = pb.add_property()
    prop.set_name("address.country")
    prop.mutable_value().set_stringvalue("USA")
    prop.set_multiple(False)
    self.assertEqual(str(pb), person_with_extra_data)

    # Ensure the extra property is ignored.
    p = Person._from_pb(pb)
    self.assertNotEqual(None, p)
    self.assertEqual(p.name, 'Google')

  def testRepeatedStructPropUnknownProp(self):
    # See issue 220.  http://goo.gl/wh06E
    # TODO: Test compressed subproperty.
    class A(model.Model):
      name = model.StringProperty()
      extra = model.StringProperty()

    class B(model.Model):
      sp = model.StructuredProperty(A, repeated=True)
      lsp = model.LocalStructuredProperty(A, repeated=True)
    ent = B(
        sp=[A(name='sp0', extra='x'), A(name='sp1', extra='y')],
        lsp=[A(name='lsp0', extra='xx'), A(name='lsp1', extra='yy')])
    key = ent.put()
    del A.extra
    del A._properties['extra']
    ent2 = key.get()
    self.assertTrue(ent is not ent2)
    self.assertTrue(isinstance(ent2.sp[0]._properties['extra'],
                               model.GenericProperty))
    self.assertTrue(isinstance(ent2.sp[1]._properties['extra'],
                               model.GenericProperty))
    self.assertTrue('extra' in ent2.sp[0]._values)
    self.assertTrue('extra' in ent2.sp[1]._values)
    self.assertTrue(isinstance(ent2.lsp[0]._properties['extra'],
                               model.GenericProperty))
    self.assertTrue(isinstance(ent2.lsp[1]._properties['extra'],
                               model.GenericProperty))
    self.assertTrue('extra' in ent2.lsp[0]._values)
    self.assertTrue('extra' in ent2.lsp[1]._values)
    pb = ent2._to_pb()
    ent3 = B._from_pb(pb)
    self.assertEqual(ent3, ent2)
    self.assertTrue('extra' in ent3.sp[0]._properties)
    self.assertTrue('extra' in ent3.sp[1]._properties)
    self.assertTrue('extra' in ent3.sp[0]._values)
    self.assertTrue('extra' in ent3.sp[1]._values)
    self.assertTrue('extra' in ent3.lsp[0]._properties)
    self.assertTrue('extra' in ent3.lsp[1]._properties)
    self.assertTrue('extra' in ent3.lsp[0]._values)
    self.assertTrue('extra' in ent3.lsp[1]._values)

  def testRepeatedStructPropUnknownNestedProp(self):
    # Test handling for structured subproperty (ignore with warning).
    # See issue 220.  http://goo.gl/wh06E
    self.ExpectWarnings()

    class Sub(model.Model):
      val = model.IntegerProperty()

    class A(model.Model):
      name = model.StringProperty()
      extra = model.StructuredProperty(Sub)

    class B(model.Model):
      sp = model.StructuredProperty(A, repeated=True)
    ent = B(sp=[A(name='x', extra=Sub(val=1)), A(name='y', extra=Sub(val=2))])
    key = ent.put()
    del A.extra
    del A._properties['extra']
    ent2 = key.get()
    self.assertTrue(ent is not ent2)

  def testNestedStructuredProperty(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()

    class AddressPair(model.Model):
      home = model.StructuredProperty(Address)
      work = model.StructuredProperty(Address)

    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(AddressPair)

    p = Person()
    p.name = 'Google'
    p.address = AddressPair(home=Address(), work=Address())
    p.address.home.city = 'Mountain View'
    p.address.home.street = '1600 Amphitheatre'
    p.address.work.city = 'San Francisco'
    p.address.work.street = '345 Spear'
    pb = p._to_pb()
    self.assertEqual(str(pb), NESTED_PB)

    p = Person._from_pb(pb)
    self.assertEqual(p.name, 'Google')
    self.assertEqual(p.address.home.street, '1600 Amphitheatre')
    self.assertEqual(p.address.home.city, 'Mountain View')
    self.assertEqual(p.address.work.street, '345 Spear')
    self.assertEqual(p.address.work.city, 'San Francisco')

  def testRepeatedStructuredPropertiesWithSameType(self):
    class Person(model.Model):
      last_name = model.StringProperty()
      first_name = model.StringProperty()

    class CustomObject(model.Model):
      p1 = model.StructuredProperty(Person, repeated=True)
      p2 = model.StructuredProperty(Person, repeated=True)

    p1 = Person(first_name="Bob", last_name="C")
    p2 = Person(first_name="Joe", last_name="K")
    p3 = Person(first_name="Alice", last_name="B")
    p4 = Person(first_name="Peter", last_name="Q")

    orig = CustomObject(p1=[p1, p2], p2=[p3, p4])
    copy = CustomObject._from_pb(orig._to_pb())

    self.assertEqual("Bob", copy.p1[0].first_name)
    self.assertEqual("Joe", copy.p1[1].first_name)
    self.assertEqual("Alice", copy.p2[0].first_name)
    self.assertEqual("Peter", copy.p2[1].first_name)
    self.assertEqual(2, copy._subentity_counter.get(['p1', 'first_name']))
    self.assertEqual(2, copy._subentity_counter.get(['p2', 'first_name']))
    self.assertEqual(2, copy._subentity_counter.get(['p1', 'last_name']))
    self.assertEqual(2, copy._subentity_counter.get(['p2', 'last_name']))

  def testRepeatedNestedStructuredProperty(self):
    class Person(model.Model):
      first_name = model.StringProperty()
      last_name = model.StringProperty()

    class PersonPhone(model.Model):
      person = model.StructuredProperty(Person)
      phone = model.StringProperty()

    class Phonebook(model.Model):
      numbers = model.StructuredProperty(PersonPhone, repeated=True)

    book = Phonebook.get_or_insert('test')
    person = Person(first_name="John", last_name='Smith')
    phone = PersonPhone(person=person, phone='1-212-555-1212')
    book.numbers.append(phone)
    pb = book._to_pb()

    ent = Phonebook._from_pb(pb)
    self.assertEqual(ent.numbers[0].person.first_name, 'John')
    self.assertEqual(len(ent.numbers), 1)
    self.assertEqual(ent.numbers[0].person.last_name, 'Smith')
    self.assertEqual(ent.numbers[0].phone, '1-212-555-1212')

  def testRepeatedNestedStructuredPropertyWithEmptyModels(self):
    class F(model.Model):
      g = model.StringProperty()

      def __eq__(self, other):
        return self.g == other.g

    class D(model.Model):
      e = model.StringProperty()
      f = model.StructuredProperty(F)

      def __eq__(self, other):
        return self.e == other.e and self.f == other.f

    class A(model.Model):
      b = model.IntegerProperty()
      c = model.StringProperty()
      d = model.StructuredProperty(D)

      def __eq__(self, other):
        return self.b == other.b and self.c == other.c and self.d == other.d

    class Foo(model.Model):
      a = model.StructuredProperty(A, repeated=True)

      def __eq__(self, other):
        return (len(self.a) == len(other.a)
                and all(a == b for a, b in zip(self.a, other.a)))

    orig = Foo(a=[A(),
                  A(b=1,
                    c="a",
                    d=None),
                  A(b=2,
                    d=D(e="hi",
                        f=F(g="hello"))),
                  A(b=None,
                    c="b",
                    d=D(e="bye")),
                  A(b=3)
                 ])
    pb = orig._to_pb()
    copy = Foo._from_pb(pb)
    self.assertEqual(orig, copy)
    self.assertEqual(-1, copy._subentity_counter._absolute_counter())
    self.assertEqual(5, copy._subentity_counter.get(['a', 'b']))
    self.assertEqual(5, copy._subentity_counter.get(['a', 'd']))
    self.assertEqual(-1, copy._subentity_counter._absolute_counter())
    self.assertEqual(5, copy._subentity_counter.get(['a', 'd', 'e']))
    self.assertEqual(5, copy._subentity_counter.get(['a', 'd', 'f']))

    # Only entity with repeated StructuredProperty should have counter
    self.assertFalse(hasattr(copy.a[2], '_subentity_counter'))
    self.assertFalse(hasattr(copy.a[2].d, '_subentity_counter'))
    self.assertFalse(hasattr(copy.a[2].d.f, '_subentity_counter'))
    self.assertFalse(hasattr(copy.a[2].d, '_subentity_counter'))

  def testRepeatedNestedStructuredPropertyWithEmptyModelsFirst(self):
    class B(model.Model):
      e = model.StringProperty()

      def __eq__(self, other):
        return self.e == other.e

    class A(model.Model):
      b = model.StructuredProperty(B)
      c = model.IntegerProperty()
      d = model.StringProperty()

      def __eq__(self, other):
        return self.b == other.b and self.c == other.c and self.d == other.d

    class Foo(model.Model):
      a = model.StructuredProperty(A, repeated=True)

      def __eq__(self, other):
        return (len(self.a) == len(other.a)
                and all(a == b for a, b in zip(self.a, other.a)))

    orig = Foo(a=[A(b=B(e='ae'),
                    c=1,
                    d='a'),
                  A(b=None,
                    c=2,
                    d='b'),
                  A(b=None,
                    c=3,
                    d='c'),
                  A(b=None,
                    c=4,
                    d='d')])
    pb = orig._to_pb()
    copy = Foo._from_pb(pb)
    self.assertEqual(orig, copy)
    self.assertEqual(-1, copy._subentity_counter._absolute_counter())
    self.assertEqual(4, copy._subentity_counter.get(['a', 'c']))
    self.assertEqual(4, copy._subentity_counter.get(['a', 'd']))
    self.assertEqual(-1, copy._subentity_counter._absolute_counter())
    self.assertEqual(4, copy._subentity_counter.get(['a', 'b', 'e']))

    # Only entity with repeated StructuredProperty should have counter
    self.assertFalse(hasattr(copy.a[0], '_subentity_counter'))
    self.assertFalse(hasattr(copy.a[0].b, '_subentity_counter'))

  def testRepeatedNestedStructuredPropertyWithEmptyModelsFirstAndLast(self):
    class B(model.Model):
      e = model.StringProperty()

      def __eq__(self, other):
        return self.e == other.e

    class A(model.Model):
      b = model.StructuredProperty(B)
      c = model.IntegerProperty()

      def __eq__(self, other):
        return self.b == other.b and self.c == other.c

    class Foo(model.Model):
      a = model.StructuredProperty(A, repeated=True)

      def __eq__(self, other):
        return (len(self.a) == len(other.a)
                and all(a == b for a, b in zip(self.a, other.a)))

    orig = Foo(a=[A(b=B(e='ae'),
                    c=1),
                  A(b=None,
                    c=2),
                  A(b=None,
                    c=3),
                  A(b=None,
                    c=4),
                  A(b=B(e='de'),
                    c=5)])
    pb = orig._to_pb()
    copy = Foo._from_pb(pb)
    self.assertEqual(orig, copy)
    self.assertEqual(-1, copy._subentity_counter._absolute_counter())
    self.assertEqual(5, copy._subentity_counter.get(['a', 'c']))
    self.assertEqual(-1, copy._subentity_counter._absolute_counter())
    self.assertEqual(5, copy._subentity_counter.get(['a', 'b', 'e']))

    # Only entity with repeated StructuredProperty should have counter
    self.assertFalse(hasattr(copy.a[0], '_subentity_counter'))
    self.assertFalse(hasattr(copy.a[0].b, '_subentity_counter'))

  def testRepeatedNestedStructuredPropertyWithEmptyModelsLast(self):
    class B(model.Model):
      e = model.StringProperty()

      def __eq__(self, other):
        return self.e == other.e

    class A(model.Model):
      b = model.StructuredProperty(B)
      c = model.IntegerProperty()
      d = model.StringProperty()

      def __eq__(self, other):
        return self.b == other.b and self.c == other.c and self.d == other.d

    class Foo(model.Model):
      a = model.StructuredProperty(A, repeated=True)

      def __eq__(self, other):
        return (len(self.a) == len(other.a)
                and all(a == b for a, b in zip(self.a, other.a)))

    orig = Foo(a=[A(b=None,
                    c=1,
                    d='a'),
                  A(b=None,
                    c=2,
                    d='b'),
                  A(b=B(e='ce'),
                    c=3,
                    d='c')])
    pb = orig._to_pb()
    copy = Foo._from_pb(pb)
    self.assertEqual(orig, copy)
    self.assertEqual(-1, copy._subentity_counter._absolute_counter())
    self.assertEqual(3, copy._subentity_counter.get(['a', 'c']))
    self.assertEqual(3, copy._subentity_counter.get(['a', 'd']))
    self.assertEqual(-1, copy._subentity_counter._absolute_counter())
    self.assertEqual(3, copy._subentity_counter.get(['a', 'b', 'e']))

    # Only entity with repeated StructuredProperty should have counter
    self.assertFalse(hasattr(copy.a[2], '_subentity_counter'))
    self.assertFalse(hasattr(copy.a[2].b, '_subentity_counter'))

  def testRepeatedNestedStructuredPropertyWithEmptyModelsMiddle(self):
    class B(model.Model):
      e = model.StringProperty()

      def __eq__(self, other):
        return self.e == other.e

    class A(model.Model):
      b = model.StructuredProperty(B)
      c = model.IntegerProperty()
      d = model.StringProperty()

      def __eq__(self, other):
        return self.b == other.b and self.c == other.c and self.d == other.d

    class Foo(model.Model):
      a = model.StructuredProperty(A, repeated=True)

      def __eq__(self, other):
        return (len(self.a) == len(other.a)
                and all(a == b for a, b in zip(self.a, other.a)))

    orig = Foo(a=[A(b=None,
                    c=1,
                    d='a'),
                  A(b=B(e='ce'),
                    c=2,
                    d='b'),
                  A(b=None,
                    c=3,
                    d='c')])
    pb = orig._to_pb()
    copy = Foo._from_pb(pb)
    self.assertEqual(orig, copy)
    self.assertEqual(-1, copy._subentity_counter._absolute_counter())
    self.assertEqual(3, copy._subentity_counter.get(['a', 'c']))
    self.assertEqual(3, copy._subentity_counter.get(['a', 'd']))
    self.assertEqual(-1, copy._subentity_counter._absolute_counter())
    self.assertEqual(3, copy._subentity_counter.get(['a', 'b', 'e']))

    # Only entity with repeated StructuredProperty should have counter
    self.assertFalse(hasattr(copy.a[1], '_subentity_counter'))
    self.assertFalse(hasattr(copy.a[1].b, '_subentity_counter'))

  def testSubPropertyWithGrowingNones(self):
    class B(model.Model):
      c = model.IntegerProperty()

      def __eq__(self, other):
        return self.c == other.c

    class A(model.Model):
      b1 = model.StructuredProperty(B)
      b2 = model.StructuredProperty(B)

      def __eq__(self, other):
        return self.b1 == other.b1 and self.b2 == other.b2

    class Foo(model.Model):
      # top-level model
      a = model.StructuredProperty(A, repeated=True)

      def __eq__(self, other):
        return (len(self.a) == len(other.a)
                and all(a == b for a, b in zip(self.a, other.a)))

    orig = Foo(a=[A(b1=B(c=1),
                    b2=B(c=2)),
                  A(b1=B(c=3),
                    b2=None),
                  A(b1=None,
                    b2=None)])
    self.assertEqual(3, len(orig.a))
    copy = Foo._from_pb(orig._to_pb())
    self.assertEqual(3, len(copy.a))
    self.assertEqual(orig, copy)

  def testRecursiveStructuredProperty(self):
    class Node(model.Model):
      name = model.StringProperty()
    Node.left = model.StructuredProperty(Node)
    Node.right = model.StructuredProperty(Node, 'rite')
    Node._fix_up_properties()

    class Tree(model.Model):
      root = model.StructuredProperty(Node)

    k = model.Key(flat=['Tree', None])
    tree = Tree()
    tree.key = k
    tree.root = Node(name='a',
                     left=Node(name='a1',
                               left=Node(name='a1a'),
                               right=Node(name='a1b')),
                     right=Node(name='a2',
                                right=Node(name='a2b')))
    pb = tree._to_pb()
    self.assertEqual(str(pb), RECURSIVE_PB)

    tree2 = Tree._from_pb(pb)
    self.assertEqual(tree2, tree)

    # Also test querying nodes.
    tree.put()
    tree3 = Tree.query(Tree.root.left.right.name == 'a1b').get()
    self.assertEqual(tree3, tree)

  def testRenamedProperty(self):
    class MyModel(model.Model):
      bb = model.BooleanProperty('b')
      pp = model.IntegerProperty('p')
      qq = model.StringProperty('q')
      dd = model.FloatProperty('d')
      kk = model.KeyProperty('k')
      uu = model.UserProperty('u')
      xxyy = model.GeoPtProperty('xy')

    ent = MyModel()
    k = model.Key(flat=['MyModel', 42])
    ent.key = k
    MyModel.bb._set_value(ent, True)
    MyModel.pp._set_value(ent, 42)
    MyModel.qq._set_value(ent, 'hello')
    MyModel.dd._set_value(ent, 2.5)
    MyModel.kk._set_value(ent, k)
    MyModel.uu._set_value(ent, TESTUSER)
    MyModel.xxyy._set_value(ent, AMSTERDAM)
    self.assertEqual(MyModel.pp._get_value(ent), 42)
    self.assertEqual(MyModel.qq._get_value(ent), 'hello')
    self.assertEqual(MyModel.dd._get_value(ent), 2.5)
    self.assertEqual(MyModel.kk._get_value(ent), k)
    self.assertEqual(MyModel.uu._get_value(ent), TESTUSER)
    self.assertEqual(MyModel.xxyy._get_value(ent), AMSTERDAM)
    pb = self.conn.adapter.entity_to_pb(ent)
    self.assertEqual(str(pb), INDEXED_PB)

    ent = MyModel._from_pb(pb)
    self.assertEqual(ent._get_kind(), 'MyModel')
    k = model.Key(flat=['MyModel', 42])
    self.assertEqual(ent.key, k)
    self.assertEqual(MyModel.pp._get_value(ent), 42)
    self.assertEqual(MyModel.qq._get_value(ent), 'hello')
    self.assertEqual(MyModel.dd._get_value(ent), 2.5)
    self.assertEqual(MyModel.kk._get_value(ent), k)

  def testUnicodeRenamedProperty(self):
    class UModel(model.Model):
      val = model.StringProperty(u'\u00fc')

      @classmethod
      def _get_kind(cls):
        return u'UModel'  # Pure ASCII Unicode kind string is find.
    u = UModel(val='abc')
    u.put()
    v = u.key.get()
    self.assertFalse(u is v)
    self.assertEqual(u.val, v.val)

  def testUnicodeKind(self):
    def helper():
      class UModel(model.Model):
        val = model.StringProperty()

        @classmethod
        def _get_kind(cls):
          return u'\u00fcModel'
    self.assertRaises(model.KindError, helper)

  def testRenamedStructuredProperty(self):
    uhome = u'hom\u00e9'
    uhome_enc_repr = r'hom\303\251'

    class Address(model.Model):
      st = model.StringProperty('street')
      ci = model.StringProperty('city')

    class AddressPair(model.Model):
      ho = model.StructuredProperty(Address, uhome)
      wo = model.StructuredProperty(Address, 'work')

    class Person(model.Model):
      na = model.StringProperty('name')
      ad = model.StructuredProperty(AddressPair, 'address')

    p = Person()
    p.na = 'Google'
    p.ad = AddressPair(ho=Address(), wo=Address())
    p.ad.ho.ci = 'Mountain View'
    p.ad.ho.st = '1600 Amphitheatre'
    p.ad.wo.ci = 'San Francisco'
    p.ad.wo.st = '345 Spear'
    pb = p._to_pb()
    expected = NESTED_PB.replace('home', uhome_enc_repr)
    self.assertEqual(str(pb), expected)

    p = Person._from_pb(pb)
    self.assertEqual(p.na, 'Google')
    self.assertEqual(p.ad.ho.st, '1600 Amphitheatre')
    self.assertEqual(p.ad.ho.ci, 'Mountain View')
    self.assertEqual(p.ad.wo.st, '345 Spear')
    self.assertEqual(p.ad.wo.ci, 'San Francisco')

  def testKindMap(self):
    model.Model._reset_kind_map()

    class A1(model.Model):
      pass

    def get_kind_map():
      # Return the kind map with __* removed.
      d = model.Model._kind_map
      return dict(kv for kv in d.iteritems() if not kv[0].startswith('__'))
    self.assertEqual(get_kind_map(), {'A1': A1})

    class A2(model.Model):
      pass
    self.assertEqual(get_kind_map(), {'A1': A1, 'A2': A2})

  def testMultipleProperty(self):
    class Person(model.Model):
      name = model.StringProperty()
      address = model.StringProperty(repeated=True)

    m = Person(name='Google', address=['345 Spear', 'San Francisco'])
    m.key = model.Key(flat=['Person', None])
    self.assertEqual(m.address, ['345 Spear', 'San Francisco'])
    pb = m._to_pb()
    self.assertEqual(str(pb), MULTI_PB)

    m2 = Person._from_pb(pb)
    self.assertEqual(m2, m)

  def testMultipleInStructuredProperty(self):
    class Address(model.Model):
      label = model.StringProperty()
      line = model.StringProperty(repeated=True)

    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(Address)

    m = Person(name='Google',
               address=Address(label='work',
                               line=['345 Spear', 'San Francisco']))
    m.key = model.Key(flat=['Person', None])
    self.assertEqual(m.address.line, ['345 Spear', 'San Francisco'])
    pb = m._to_pb()
    self.assertEqual(str(pb), MULTIINSTRUCT_PB)

    m2 = Person._from_pb(pb)
    self.assertEqual(m2, m)

  def testMultipleStructuredPropertyProtocolBuffers(self):
    class Address(model.Model):
      label = model.StringProperty()
      text = model.StringProperty()

    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(Address, repeated=True)

    m = Person(name='Google',
               address=[Address(label='work', text='San Francisco'),
                        Address(label='home', text='Mountain View')])
    m.key = model.Key(flat=['Person', None])
    self.assertEqual(m.address[0].label, 'work')
    self.assertEqual(m.address[0].text, 'San Francisco')
    self.assertEqual(m.address[1].label, 'home')
    self.assertEqual(m.address[1].text, 'Mountain View')
    pb = m._to_pb()
    self.assertEqual(str(pb), MULTISTRUCT_PB)

    m2 = Person._from_pb(pb)
    self.assertEqual(m2, m)

  def testCannotMultipleInMultiple(self):
    class Inner(model.Model):
      tags = model.StringProperty(repeated=True)
    self.assertRaises(TypeError,
                      model.StructuredProperty, Inner, repeated=True)

    class Outer(model.Model):
      inner = model.StructuredProperty(Inner)
    self.assertRaises(TypeError,
                      model.StructuredProperty, Outer, repeated=True)
    try:
      model.StructuredProperty(Outer, repeated=True)
    except TypeError, err:
      self.assertEqual(str(err),
                       'This StructuredProperty cannot use repeated=True '
                       'because its model class (Outer) '
                       'contains repeated properties (directly or indirectly).')

  def testNullProperties(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()
      zipcode = model.IntegerProperty()

    class Person(model.Model):
      address = model.StructuredProperty(Address)
      age = model.IntegerProperty()
      name = model.StringProperty()
      k = model.KeyProperty()
    k = model.Key(flat=['Person', 42])
    p = Person()
    p.key = k
    self.assertEqual(p.address, None)
    self.assertEqual(p.age, None)
    self.assertEqual(p.name, None)
    self.assertEqual(p.k, None)
    pb = p._to_pb()
    q = Person._from_pb(pb)
    self.assertEqual(q.address, None)
    self.assertEqual(q.age, None)
    self.assertEqual(q.name, None)
    self.assertEqual(q.k, None)
    self.assertEqual(q, p)

  def testOrphanProperties(self):
    class Tag(model.Model):
      names = model.StringProperty(repeated=True)
      ratings = model.IntegerProperty(repeated=True)

    class Address(model.Model):
      line = model.StringProperty(repeated=True)
      city = model.StringProperty()
      zipcode = model.IntegerProperty()
      tags = model.StructuredProperty(Tag)

    class Person(model.Model):
      address = model.StructuredProperty(Address)
      age = model.IntegerProperty(repeated=True)
      name = model.StringProperty()
      k = model.KeyProperty()
    k = model.Key(flat=['Person', 42])
    p = Person(name='White House', k=k, age=[210, 211],
               address=Address(line=['1600 Pennsylvania', 'Washington, DC'],
                               tags=Tag(names=['a', 'b'], ratings=[1, 2]),
                               zipcode=20500))
    p.key = k
    pb = p._to_pb()
    q = model.Model._from_pb(pb)
    qb = q._to_pb()
    linesp = str(pb).splitlines(True)
    linesq = str(qb).splitlines(True)
    lines = difflib.unified_diff(linesp, linesq, 'Expected', 'Actual')
    self.assertEqual(pb, qb, ''.join(lines))

  def testMetaModelRepr(self):
    class MyModel(model.Model):
      name = model.StringProperty()
      tags = model.StringProperty(repeated=True)
      age = model.IntegerProperty(name='a')
      other = model.KeyProperty()
    self.assertEqual(repr(MyModel),
                     "MyModel<"
                     "age=IntegerProperty('a'), "
                     "name=StringProperty('name'), "
                     "other=KeyProperty('other'), "
                     "tags=StringProperty('tags', repeated=True)"
                     ">")

  def testModelToDict(self):
    class MyModel(model.Model):
      foo = model.StringProperty(name='f')
      bar = model.StringProperty(default='bar')
      baz = model.StringProperty(repeated=True)
    ent = MyModel()
    self.assertEqual({'foo': None, 'bar': 'bar', 'baz': []},
                     ent._to_dict())
    self.assertEqual({'foo': None}, ent._to_dict(include=['foo']))
    self.assertEqual({'bar': 'bar', 'baz': []},
                     ent._to_dict(exclude=frozenset(['foo'])))
    self.assertEqual({}, ent.to_dict(include=['foo'], exclude=['foo']))
    self.assertRaises(TypeError, ent._to_dict, include='foo')
    self.assertRaises(TypeError, ent._to_dict, exclude='foo')
    ent.foo = 'x'
    ent.bar = 'y'
    ent.baz = ['a']
    self.assertEqual({'foo': 'x', 'bar': 'y', 'baz': ['a']},
                     ent.to_dict())

  def testModelToDictStructures(self):
    class MySubmodel(model.Model):
      foo = model.StringProperty()
      bar = model.IntegerProperty()

    class MyModel(model.Model):
      a = model.StructuredProperty(MySubmodel)
      b = model.LocalStructuredProperty(MySubmodel, repeated=True)
      c = model.StructuredProperty(MySubmodel)
      d = model.LocalStructuredProperty(MySubmodel)
      e = model.StructuredProperty(MySubmodel, repeated=True)
    x = MyModel(a=MySubmodel(foo='foo', bar=42),
                b=[MySubmodel(foo='f'), MySubmodel(bar=4)])
    self.assertEqual({'a': {'foo': 'foo', 'bar': 42},
                      'b': [{'foo': 'f', 'bar': None, },
                            {'foo': None, 'bar': 4}],
                      'c': None,
                      'd': None,
                      'e': [],
                      },
                     x.to_dict())

  def testModelPickling(self):
    global MyModel

    class MyModel(model.Model):
      name = model.StringProperty()
      tags = model.StringProperty(repeated=True)
      age = model.IntegerProperty()
      other = model.KeyProperty()
    my = MyModel(name='joe', tags=['python', 'ruby'], age=42,
                 other=model.Key(MyModel, 42))
    for proto in 0, 1, 2:
      s = pickle.dumps(my, proto)
      mycopy = pickle.loads(s)
      self.assertEqual(mycopy, my)

  def testRejectOldPickles(self):
    global MyModel

    class MyModel(db.Model):
      name = db.StringProperty()

    dumped = []
    for proto in 0, 1, 2:
      x = MyModel()
      s = pickle.dumps(x, proto)
      dumped.append(s)
      x.name = 'joe'
      s = pickle.dumps(x, proto)
      dumped.append(s)
      db.put(x)
      s = pickle.dumps(x, proto)
      dumped.append(s)

    class MyModel(model.Model):
      name = model.StringProperty()

    for s in dumped:
      self.assertRaises(Exception, pickle.loads, s)

  def testModelRepr(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()

    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(Address)

    p = Person(name='Google', address=Address(street='345 Spear', city='SF'))
    self.assertEqual(
        repr(p),
        "Person(address=Address(city='SF', street='345 Spear'), name='Google')")
    p.key = model.Key(pairs=[('Person', 42)])
    self.assertEqual(
        repr(p),
        "Person(key=Key('Person', 42), "
        "address=Address(city='SF', street='345 Spear'), name='Google')")

  def testModelReprNoSideEffects(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()
    a = Address(street='345 Spear', city='SF')
    # White box test: values are 'top values'.
    self.assertEqual(a._values, {'street': '345 Spear', 'city': 'SF'})
    key_id = a.put().id()
    # White box test: put() has turned wrapped values in _BaseValue().
    self.assertEqual(a._values, {'street': model._BaseValue('345 Spear'),
                                 'city': model._BaseValue('SF')})
    self.assertEqual(repr(a),
                     "Address(key=Key('Address', %d), " % key_id +
                     # (Note: Unicode literals.)
                     "city=u'SF', street=u'345 Spear')")
    # White box test: _values is unchanged.
    self.assertEqual(a._values, {'street': model._BaseValue('345 Spear'),
                                 'city': model._BaseValue('SF')})

  def testModelRepr_RenamedProperty(self):
    class Address(model.Model):
      street = model.StringProperty('Street')
      city = model.StringProperty('City')
    a = Address(street='345 Spear', city='SF')
    self.assertEqual(repr(a), "Address(city='SF', street='345 Spear')")

  def testModelRepr_HugeProperty(self):
    class Biggy(model.Model):
      blob = model.BlobProperty()
      text = model.TextProperty()
    small = Biggy(blob='xyz', text=u'abc')
    self.assertEqual(repr(small), "Biggy(blob='xyz', text=u'abc')")
    large = Biggy(blob='x' * 1500, text='a' * 1500)
    self.assertEqual(repr(large),
                     "Biggy(blob='%s', text='%s')" % ('x' * 1500, 'a' * 1500))
    huge = Biggy(blob='x' * 2000, text='a' * 2000)
    self.assertEqual(repr(huge),
                     "Biggy(blob='%s...', text='%s...')" %
                     ('x' * 1499, 'a' * 1499))

  def testModelRepr_CustomRepr(self):
    # Demonstrate how to override a property's repr.
    class MyJsonProperty(model.JsonProperty):

      def _value_to_repr(self, value):
        val = self._opt_call_from_base_type(value)
        return json.dumps(val, indent=2)

    class Jumpy(model.Model):
      jsn = MyJsonProperty()
    jump = Jumpy(jsn={'a': [123, {'b': ['xyz', 'pqr']}]})
    self.assertEqual(repr(jump),
                     'Jumpy(jsn={\n'
                     '  "a": [\n'
                     '    123, \n'
                     '    {\n'
                     '      "b": [\n'
                     '        "xyz", \n'
                     '        "pqr"\n'
                     '      ]\n'
                     '    }\n'
                     '  ]\n'
                     '})')

  def testModel_RenameAlias(self):
    class Person(model.Model):
      name = model.StringProperty('Name')
    p = Person(name='Fred')
    self.assertRaises(AttributeError, getattr, p, 'Name')
    self.assertRaises(AttributeError, Person, Name='Fred')
    # Unfortunately, p.Name = 'boo' just sets p.__dict__['Name'] = 'boo'.
    self.assertRaises(AttributeError, getattr, p, 'foo')

  def testExpando_RenameAlias(self):
    class Person(model.Expando):
      name = model.StringProperty('Name')

    p = Person(name='Fred')
    self.assertEqual(p.name, 'Fred')
    self.assertEqual(p.Name, 'Fred')
    self.assertEqual(p._values, {'Name': 'Fred'})
    self.assertTrue(p._properties, Person._properties)

    p = Person(Name='Fred')
    self.assertEqual(p.name, 'Fred')
    self.assertEqual(p.Name, 'Fred')
    self.assertEqual(p._values, {'Name': 'Fred'})
    self.assertTrue(p._properties, Person._properties)

    p = Person()
    p.Name = 'Fred'
    self.assertEqual(p.name, 'Fred')
    self.assertEqual(p.Name, 'Fred')
    self.assertEqual(p._values, {'Name': 'Fred'})
    self.assertTrue(p._properties, Person._properties)

    self.assertRaises(AttributeError, getattr, p, 'foo')

  def testModel_RenameSwap(self):
    class Person(model.Model):
      foo = model.StringProperty('bar')
      bar = model.StringProperty('foo')
    p = Person(foo='foo', bar='bar')
    self.assertEqual(p._values,
                     {'foo': 'bar', 'bar': 'foo'})

  def testExpando_RenameSwap(self):
    class Person(model.Expando):
      foo = model.StringProperty('bar')
      bar = model.StringProperty('foo')
    p = Person(foo='foo', bar='bar', baz='baz')
    self.assertEqual(p._values,
                     {'foo': 'bar', 'bar': 'foo', 'baz': 'baz'})
    p = Person()
    p.foo = 'foo'
    p.bar = 'bar'
    p.baz = 'baz'
    self.assertEqual(p._values,
                     {'foo': 'bar', 'bar': 'foo', 'baz': 'baz'})

  def testExpando_Repr(self):
    class E(model.Expando):
      pass
    ent = E(a=1, b=[2], c=E(x=3, y=[4]))
    self.assertEqual(repr(ent),
                     "E(a=1, b=[2], c=E(x=3, y=[4]))")
    pb = ent._to_pb(set_key=False)
    ent2 = E._from_pb(pb)
    # NOTE: The 'E' kind name for the inner instance is not persisted,
    # so it comes out as Expando.
    self.assertEqual(repr(ent2),
                     "E(a=1, b=[2], c=Expando(x=3, y=[4]))")

  def testPropertyRepr(self):
    p = model.Property()
    self.assertEqual(repr(p), 'Property()')

    p = model.IntegerProperty('foo', indexed=False, repeated=True)
    self.assertEqual(repr(p),
                     "IntegerProperty('foo', indexed=False, repeated=True)")

    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()
    p = model.StructuredProperty(Address, 'foo')
    self.assertEqual(repr(p), "StructuredProperty(Address, 'foo')")
    q = model.LocalStructuredProperty(Address, 'bar')
    self.assertEqual(repr(q), "LocalStructuredProperty(Address, 'bar')")

    class MyModel(model.Model):
      boolp = model.BooleanProperty()
      intp = model.IntegerProperty()
      floatp = model.FloatProperty()
      strp = model.StringProperty()
      txtp = model.TextProperty()
      blobp = model.BlobProperty()
      geoptp = model.GeoPtProperty()
      userp = model.UserProperty()
      keyp = model.KeyProperty()
      blobkeyp = model.BlobKeyProperty()
      datetimep = model.DateTimeProperty()
      datep = model.DateProperty()
      timep = model.TimeProperty()
      structp = model.StructuredProperty(Address)
      localstructp = model.LocalStructuredProperty(Address)
      genp = model.GenericProperty()
      compp = model.ComputedProperty(lambda _: 'x')
    self.assertEqual(repr(MyModel.key), "ModelKey('__key__')")
    for prop in MyModel._properties.itervalues():
      s = repr(prop)
      self.assertTrue(s.startswith(prop.__class__.__name__ + '('), s)

  def testLengthRestriction(self):
    # Check the following rules for size validation of blobs and texts:
    # - Unindexed blob and text properties can be unlimited in size.
    # - Indexed blob properties are limited to 1500 bytes.
    # - Indexed text properties are limited to 1500 bytes.
    class MyModel(model.Model):
      ublob = model.BlobProperty()  # Defaults to indexed=False.
      iblob = model.BlobProperty(indexed=True)
      utext = model.TextProperty()  # Defaults to indexed=False.
      itext = model.TextProperty(indexed=True)
      ustr = model.StringProperty(indexed=False)
      istr = model.StringProperty()  # Defaults to indexed=True.
      ugen = model.GenericProperty(indexed=False)
      igen = model.GenericProperty(indexed=True)
    largeblob = 'x' * 1500
    toolargeblob = 'x' * 1501
    hugeblob = 'x' * 10000
    largetext = u'\u1234' * (1500 / 3)  # 3 bytes per char
    toolargetext = u'\u1234' * (1500 / 3) + 'x'
    hugetext = u'\u1234' * 10000

    ent = MyModel()
    # These should all fail:
    self.assertRaises(datastore_errors.BadValueError,
                      setattr, ent, 'iblob', toolargeblob)
    self.assertRaises(datastore_errors.BadValueError,
                      setattr, ent, 'itext', toolargetext)
    self.assertRaises(datastore_errors.BadValueError,
                      setattr, ent, 'itext', toolargeblob)
    self.assertRaises(datastore_errors.BadValueError,
                      setattr, ent, 'istr', toolargetext)
    self.assertRaises(datastore_errors.BadValueError,
                      setattr, ent, 'istr', toolargeblob)
    self.assertRaises(datastore_errors.BadValueError,
                      setattr, ent, 'igen', toolargetext)
    self.assertRaises(datastore_errors.BadValueError,
                      setattr, ent, 'igen', toolargeblob)
    # These should all work:
    ent.ublob = hugeblob
    ent.iblob = largeblob
    ent.utext = hugetext
    ent.itext = largetext
    ent.ustr = hugetext
    ent.istr = largetext
    ent.ugen = hugetext
    ent.igen = largetext
    # Writing the entity should work:
    key = ent.put()
    # Reading it back should work:
    ent2 = key.get()
    self.assertEqual(ent2, ent)
    self.assertTrue(ent2 is not ent)

  def testValidation(self):
    class All(model.Model):
      s = model.StringProperty()
      i = model.IntegerProperty()
      f = model.FloatProperty()
      t = model.TextProperty()
      b = model.BlobProperty()
      k = model.KeyProperty()
    BVE = datastore_errors.BadValueError
    a = All()

    a.s = None
    a.s = 'abc'
    a.s = u'def'
    a.s = u'\xff'
    a.s = u'\u1234'
    a.s = u'\U00012345'
    self.assertRaises(BVE, setattr, a, 's', 0)
    self.assertRaises(BVE, setattr, a, 's', '\xff')

    a.i = None
    a.i = 42
    a.i = 123L
    self.assertRaises(BVE, setattr, a, 'i', '')

    a.f = None
    a.f = 42
    a.f = 3.14
    self.assertRaises(BVE, setattr, a, 'f', '')

    a.t = None
    a.t = 'abc'
    a.t = u'def'
    a.t = u'\xff'
    a.t = u'\u1234'
    a.t = u'\U00012345'
    self.assertRaises(BVE, setattr, a, 't', 0)
    self.assertRaises(BVE, setattr, a, 't', '\xff')

    a.b = None
    a.b = 'abc'
    a.b = '\xff'
    self.assertRaises(BVE, setattr, a, 'b', u'')
    self.assertRaises(BVE, setattr, a, 'b', u'\u1234')

    a.k = None
    a.k = model.Key('Foo', 42)
    self.assertRaises(BVE, setattr, a, 'k', '')

  def testLocalStructuredProperty(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()

    class Person(model.Model):
      name = model.StringProperty()
      address = model.LocalStructuredProperty(Address)

    p = Person()
    p.name = 'Google'
    a = Address(street='1600 Amphitheatre')
    p.address = a
    p.address.city = 'Mountain View'
    self.assertEqual(p.address.key, None)
    self.assertEqual(Person.name._get_value(p), 'Google')
    self.assertEqual(p.name, 'Google')
    self.assertEqual(Person.address._get_value(p), a)
    self.assertEqual(Address.street._get_value(a), '1600 Amphitheatre')
    self.assertEqual(Address.city._get_value(a), 'Mountain View')

    pb = p._to_pb()
    self.assertEqual(pb.raw_property(0).meaning(),
                     entity_pb.Property.ENTITY_PROTO)
    # TODO: Validate pb

    # Check we can enable and disable compression and have old data still
    # be understood.
    Person.address._compressed = True
    p = Person._from_pb(pb)
    self.assertEqual(p.address.key, None)
    self.assertEqual(p.name, 'Google')
    self.assertEqual(p.address.street, '1600 Amphitheatre')
    self.assertEqual(p.address.city, 'Mountain View')
    self.assertEqual(p.address.key, None)
    self.assertEqual(p.address, a)
    self.assertEqual(repr(Person.address),
                     "LocalStructuredProperty(Address, 'address', "
                     "compressed=True)")
    pb = p._to_pb()

    Person.address._compressed = False
    p = Person._from_pb(pb)
    self.assertEqual(p.address.key, None)

    # Now try with an empty address
    p = Person()
    p.name = 'Google'
    self.assertTrue(p.address is None)
    pb = p._to_pb()
    p = Person._from_pb(pb)
    self.assertTrue(p.address is None)
    self.assertEqual(p.name, 'Google')

  def testLocalStructuredPropertyCompressed(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()

    class Person(model.Model):
      name = model.StringProperty()
      address = model.LocalStructuredProperty(Address, compressed=True)

    k = model.Key('Person', 'google')
    p = Person(key=k)
    p.name = 'Google'
    p.address = Address(street='1600 Amphitheatre', city='Mountain View')
    p.put()

    # To test compression and deserialization with untouched properties.
    p = k.get()
    p.put()

    p = k.get()
    self.assertEqual(p.name, 'Google')
    self.assertEqual(p.address.street, '1600 Amphitheatre')
    self.assertEqual(p.address.city, 'Mountain View')

    # To test compression and deserialization after properties were accessed.
    p.put()

  def testLocalStructuredPropertyRepeated(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()

    class Person(model.Model):
      name = model.StringProperty()
      address = model.LocalStructuredProperty(Address, repeated=True)

    k = model.Key('Person', 'google')
    p = Person(key=k)
    p.name = 'Google'
    p.address.append(Address(street='1600 Amphitheatre', city='Mountain View'))
    p.address.append(Address(street='Webb crater', city='Moon'))
    p.put()

    # To test compression and deserialization with untouched properties.
    p = k.get()
    p.put()

    p = k.get()
    self.assertEqual(p.name, 'Google')
    self.assertEqual(p.address[0].street, '1600 Amphitheatre')
    self.assertEqual(p.address[0].city, 'Mountain View')
    self.assertEqual(p.address[1].street, 'Webb crater')
    self.assertEqual(p.address[1].city, 'Moon')

    # To test compression and deserialization after properties were accessed.
    p.put()

  def testLocalStructuredPropertyRepeatedCompressed(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()

    class Person(model.Model):
      name = model.StringProperty()
      address = model.LocalStructuredProperty(Address, repeated=True,
                                              compressed=True)

    k = model.Key('Person', 'google')
    p = Person(key=k)
    p.name = 'Google'
    p.address.append(Address(street='1600 Amphitheatre', city='Mountain View'))
    p.address.append(Address(street='Webb crater', city='Moon'))
    p.put()

    # To test compression and deserialization with untouched properties.
    p = k.get()
    p.put()

    p = k.get()
    self.assertEqual(p.name, 'Google')
    self.assertEqual(p.address[0].street, '1600 Amphitheatre')
    self.assertEqual(p.address[0].city, 'Mountain View')
    self.assertEqual(p.address[1].street, 'Webb crater')
    self.assertEqual(p.address[1].city, 'Moon')

    # To test compression and deserialization after properties were accessed.
    p.put()

  def testLocalStructuredPropertyRepeatedRepeated(self):
    class Inner(model.Model):
      a = model.IntegerProperty(repeated=True)
    self.assertTrue(Inner._has_repeated)

    class Outer(model.Model):
      b = model.LocalStructuredProperty(Inner, repeated=True)
    self.assertTrue(Inner._has_repeated)
    x = Outer(b=[Inner(a=[1, 2]), Inner(a=[3, 4, 5])])
    k = x.put()
    y = k.get()
    self.assertTrue(x is not y)
    self.assertEqual(x, y)

  def testLocalStructuredPropertyRepeatedNone(self):
    class Inner(model.Model):
      a = model.IntegerProperty()

    class Outer(model.Model):
      b = model.LocalStructuredProperty(Inner, repeated=True)
    x = Outer()
    x.b.append(None)
    # Should raise "Expected Inner instance, got None"
    self.assertRaises(datastore_errors.BadValueError, x.put)

  def testLocalStructuredPropertyWithoutKey(self):
    class Inner(model.Model):
      pass

    class Outer(model.Model):
      inner = model.LocalStructuredProperty(Inner, keep_keys=False)
    outer1 = Outer(inner=Inner(key=model.Key(Inner, None)))
    key1 = outer1.put()
    self.assertEqual(outer1.inner.key, None)
    outer2 = Outer(inner=Inner(id=42))
    key2 = outer2.put()
    self.assertEqual(outer2.inner.key, None)

    # Redefine the class with keep_keys=True; no key is loaded.
    class Outer(model.Model):
      inner = model.LocalStructuredProperty(Inner, keep_keys=True)
    other1 = key1.get()
    self.assertEqual(other1.inner.key, None)
    other2 = key2.get()
    self.assertEqual(other2.inner.key, None)

  def testLocalStructuredPropertyWithKey(self):
    class Inner(model.Model):
      pass

    class Outer(model.Model):
      inner = model.LocalStructuredProperty(Inner, keep_keys=True)
    outer1 = Outer(inner=Inner())
    key1 = outer1.put()  # This forces an incomplete key on inner
    self.assertEqual(outer1.inner.key.id(), None)
    outer2 = Outer(inner=Inner(id=42))
    key2 = outer2.put()
    self.assertEqual(outer2.inner.key.id(), 42)

    # Check that incomplete and complete keys are read back.
    x1 = key1.get()
    self.assertEqual(x1.inner.key.id(), None)  # Incomplete key is kept
    x2 = key2.get()
    self.assertEqual(x2.inner.key.id(), 42)
    self.assertNotEqual(x1.inner, x2.inner)  # Different key -> unequal

    # Redefine the class without keep_keys.
    class Outer(model.Model):
      inner = model.LocalStructuredProperty(Inner)
    a1 = key1.get()
    self.assertEqual(a1.inner.key, None)  # Incomplete key is dropped
    a2 = key2.get()
    self.assertEqual(a2.inner.key, None)  # Complete key is dropped, too

    # Now read this back using a GenericProperty.
    class Outer(model.Model):
      inner = model.GenericProperty(indexed=False)
    b1 = key1.get()
    self.assertEqual(b1.inner.key.id(), None)  # Incomplete key is kept
    b2 = key2.get()
    self.assertEqual(b2.inner.key.id(), 42)

    # A write/read cycle should preserve the inner key.
    b2.put()
    b2 = key2.get()
    self.assertEqual(b2.inner.key.id(), 42)

    # Finally, write back a2 (!) and read it back; the inner key is lost.
    a2.put()
    b2 = key2.get()
    self.assertEqual(b2.inner.key, None)

  def testLocalStructuredPropertyKeyDisagreement(self):
    class Inner(model.Model):
      pass

    class Outer(model.Model):
      inner = model.LocalStructuredProperty(Inner, keep_keys=True)
    outer1 = Outer(inner=Inner())
    outer2 = Outer(inner=Inner(id=42))
    key1 = outer1.put()
    key2 = outer2.put()
    self.assertEqual(outer1.inner.key, model.Key(Inner, None))
    self.assertEqual(outer2.inner.key, model.Key(Inner, 42))

    # This is a dead end.
    class AnotherInner(model.Model):
      pass

    class Outer(model.Model):
      inner = model.LocalStructuredProperty(AnotherInner, keep_keys=True)
    other1 = key1.get()
    self.assertRaises(model.KindError, lambda: other1.inner)
    self.assertRaises(model.KindError, other1.put)
    other2 = key2.get()
    self.assertRaises(model.KindError, lambda: other2.inner)
    self.assertRaises(model.KindError, other2.put)

    # This works.
    class YetAnotherInner(model.Model):
      pass

    class Outer(model.Model):
      inner = model.LocalStructuredProperty(YetAnotherInner, keep_keys=False)
    another1 = key1.get()
    self.assertTrue(isinstance(another1.inner, YetAnotherInner))
    self.assertEqual(another1.inner.key, None)
    another1.put()
    another2 = key2.get()
    self.assertTrue(isinstance(another2.inner, YetAnotherInner))
    self.assertEqual(another2.inner.key, None)
    another2.put()

  def testEmptyList(self):
    class Person(model.Model):
      name = model.StringProperty(repeated=True)
    p = Person()
    self.assertEqual(p.name, [])
    pb = p._to_pb()
    q = Person._from_pb(pb)
    self.assertEqual(q.name, [], str(pb))

  def testEmptyListSerialized(self):
    class Person(model.Model):
      name = model.StringProperty(repeated=True)
    p = Person()
    pb = p._to_pb()
    q = Person._from_pb(pb)
    self.assertEqual(q.name, [], str(pb))

  empty_proto = object()

  def makeEntityProto(self, entity_contents=empty_proto):
    proto = entity_pb.EntityProto()
    proto.mutable_key().set_app('ndb-test-app-id')
    proto.mutable_entity_group()
    elem = proto.mutable_key().mutable_path().add_element()
    elem.set_type('Strung')

    if entity_contents == self.empty_proto:
      return proto

    prop = proto.add_property()
    prop.set_multiple(False)
    prop.set_name('string_list')
    prop.mutable_value()

    if entity_contents is None:
      pass  # It is a null proto
    elif not entity_contents:
      prop.set_meaning(entity_pb.Property.EMPTY_LIST)
    elif entity_contents == [None]:
      prop.set_multiple(True)
    elif isinstance(entity_contents, list) and len(entity_contents) == 1:
      prop.set_multiple(True)
      prop.mutable_value().set_int64value(entity_contents[0])
    else:
      raise AssertionError(
          "Don't know how to convert %s to proto", entity_contents)
    return proto

  def testEmptyListWithFloatProperty(self):
    def static_instance_property_only_float(write_empty_list):

      class Strung(model.Model):
        string_list = (
            model.FloatProperty(
                repeated=True,
                write_empty_list=write_empty_list))
      return ("static_instance_property_only_float", Strung)

    self.RunEmptyListAndNoneBehaviorTest(
        model_config=static_instance_property_only_float,
        write_empty_list=False,
        model_to_protobuf=(
            (None, db.BadValueError),
            ([None], db.BadValueError),
            ([], self.empty_proto)
        ),
        protobuf_to_model=())

    self.RunEmptyListAndNoneBehaviorTest(
        model_config=static_instance_property_only_float,
        write_empty_list=True,
        model_to_protobuf=(
            (None, db.BadValueError),
            ([None], db.BadValueError),
            ([], [])
        ),
        protobuf_to_model=(
            (self.empty_proto, []),
            (None, [None]),
            ([None], [None]),
            ([], [])))

  def testEmptyListGlobalDefaults(self):
    self.assertIsNone(model.Expando._write_empty_list_for_dynamic_properties)
    self.assertFalse(model.Property._write_empty_list)

  def testStaticEmptyListBehavior(self):

    def static_model_property_only(write_empty_list):
      model.Property._write_empty_list = write_empty_list

      class Strung(model.Model):
        string_list = model.GenericProperty(repeated=True)
      return ("static_model_property_only", Strung)

    def static_instance_property_only(write_empty_list):

      class Strung(model.Model):
        string_list = (
            model.GenericProperty(
                repeated=True,
                write_empty_list=write_empty_list))
      return ("static_instance_property_only", Strung)

    def dynamic_with_static_prop(write_empty_list):

      class Strung(model.Expando):
        string_list = (
            model.GenericProperty(
                repeated=True,
                write_empty_list=write_empty_list))
      Strung._write_empty_list_for_dynamic_properties = not write_empty_list
      return ("dynamic_with_static_prop", Strung)

    def static_model_disagrees_with_prop(write_empty_list):
      model.Property._write_empty_list = not write_empty_list

      class Strung(model.Model):
        string_list = (
            model.GenericProperty(
                repeated=True,
                write_empty_list=write_empty_list))
      return ("static_model_disagrees_with_prop", Strung)

    for model_config in (
        static_model_property_only,
        static_instance_property_only,
        dynamic_with_static_prop,
        static_model_disagrees_with_prop):
      self.RunEmptyListAndNoneBehaviorTest(
          model_config=model_config,
          write_empty_list=False,
          model_to_protobuf=(
              (None, db.BadValueError),
              ([None], db.BadValueError),
              ([7], [7]),
              ([], self.empty_proto)
          ),
          protobuf_to_model=(
              (self.empty_proto, []),
              (None, [None]),
              ([None], [None]),
              ([7], [7]),
              ([], [])))
      self.RunEmptyListAndNoneBehaviorTest(
          model_config=model_config,
          write_empty_list=True,
          model_to_protobuf=(
              (None, db.BadValueError),
              ([None], db.BadValueError),
              ([7], [7]),
              ([], [])
          ),
          protobuf_to_model=(
              (self.empty_proto, []),
              (None, [None]),
              ([None], [None]),
              ([7], [7]),
              ([], [])))

  def testDynamicEmptyListBehavior(self):

    def dynamic_model_property_only(write_empty_list):
      model.Expando._write_empty_list_for_dynamic_properties = write_empty_list

      class Strung(model.Expando):
        pass
      return ('dynamic_model_property_only', Strung)

    def dynamic_instance_property_only(write_empty_list):

      class Strung(model.Expando):
        pass
      Strung._write_empty_list_for_dynamic_properties = write_empty_list
      return ("dynamic_instance_property_only", Strung)

    def dynamic_set_from_prop_default(write_empty_list):
      model.Property._write_empty_list = write_empty_list

      class Strung(model.Expando):
        pass
      return ("dynamic_set_from_prop_default", Strung)

    def dynamic_mismatch_between_props(write_empty_list):
      model.Property._write_empty_list = not write_empty_list

      class Strung(model.Expando):
        pass
      Strung._write_empty_list_for_dynamic_properties = write_empty_list
      return ("dynamic_mismatch_between_props", Strung)

    for model_config in (
        dynamic_instance_property_only,
        dynamic_model_property_only,
        dynamic_set_from_prop_default,
        dynamic_mismatch_between_props):
      self.RunEmptyListAndNoneBehaviorTest(
          model_config=model_config,
          write_empty_list=False,
          model_to_protobuf=(
              (None, None),
              ([None], db.BadValueError),
              ([7], [7]),
              ([], self.empty_proto)
          ),
          protobuf_to_model=(
              (None, None),
              ([None], [None]),
              ([7], [7]),
              ([], [])))
      self.RunEmptyListAndNoneBehaviorTest(
          model_config=model_config,
          write_empty_list=True,
          model_to_protobuf=(
              (None, None),
              ([None], db.BadValueError),
              ([7], [7]),
              ([], [])
          ),
          protobuf_to_model=(
              (None, None),
              ([None], [None]),
              ([7], [7]),
              ([], [])))

  def RunEmptyListAndNoneBehaviorTest(
      self, model_config, write_empty_list,
      model_to_protobuf, protobuf_to_model):
    try:
      property_before = model.Property._write_empty_list
      expando_before = model.Expando._write_empty_list_for_dynamic_properties
      (test_config, db_model) = model_config(write_empty_list)

      for input_, output in protobuf_to_model:
        desc = 'proto(%s) -> %s model(%s) test ' % (input_, test_config, output)
        self.RunOneEmptyListTest(True, input_, output, desc, db_model)

      for input_, output in model_to_protobuf:
        desc = '%s model(%s) -> proto(%s) test ' % (input_, test_config, output)
        self.RunOneEmptyListTest(False, input_, output, desc, db_model)
    finally:
      model.Property._write_empty_list = property_before
      model.Expando._write_empty_list_for_dynamic_properties = expando_before

  def RunOneEmptyListTest(
          self, is_proto_to_model, test_input, expected_output, desc, db_model):
    expect_error = isinstance(expected_output, type) and issubclass(
        expected_output, Exception)
    try:
      if is_proto_to_model:  # See what db_model we get from the protobuf
        model_instance = db_model._from_pb(self.makeEntityProto(test_input))
        self.assertTrue(
            isinstance(model_instance, db_model), 'Model should be a db_model')
        real_output = model_instance.string_list
        self.assertEqual(expected_output, real_output, desc)
      else:  # See what protobuf we get from the db_model.
        s = db_model()
        s.string_list = test_input
        try:
          as_proto = s._to_pb()
        except AssertionError:
          raise db.BadValueError('Cannot Serialize')
        self.assertEqual(
            self.makeEntityProto(expected_output), as_proto, desc)
        s.put()
      self.assertFalse(expect_error, desc)
    except AssertionError:
      raise
    except BaseException as e:
      self.assertTrue(expect_error, desc)
      self.assertTrue(isinstance(e, expected_output), desc)

  def testDatetimeSerializing(self):
    class Person(model.Model):
      t = model.GenericProperty()
    p = Person(t=datetime.datetime.utcnow())
    pb = p._to_pb()
    q = Person._from_pb(pb)
    self.assertEqual(p.t, q.t)

  def testExpandoKey(self):
    class Ex(model.Expando):
      pass
    e = Ex()
    self.assertEqual(e.key, None)
    k = model.Key('Ex', 'abc')
    e.key = k
    self.assertEqual(e.key, k)
    k2 = model.Key('Ex', 'def')
    e2 = Ex(key=k2)
    self.assertEqual(e2.key, k2)
    e2.key = k
    self.assertEqual(e2.key, k)
    self.assertEqual(e, e2)
    del e.key
    self.assertEqual(e.key, None)

  def testExpandoRead(self):
    class Person(model.Model):
      name = model.StringProperty()
      city = model.StringProperty()
    p = Person(name='Guido', city='SF')
    pb = p._to_pb()
    q = model.Expando._from_pb(pb)
    self.assertEqual(q.name, 'Guido')
    self.assertEqual(q.city, 'SF')

  def testExpandoWrite(self):
    k = model.Key(flat=['Model', 42])
    p = model.Expando(key=k)
    p.k = k
    p.p = 42
    p.q = 'hello'
    p.u = TESTUSER
    p.d = 2.5
    p.b = True
    p.xy = AMSTERDAM
    pb = p._to_pb()
    self.assertEqual(str(pb), GOLDEN_PB)

  def testExpandoDelAttr(self):
    class Ex(model.Expando):
      static = model.StringProperty()

    e = Ex()
    self.assertEqual(e.static, None)
    self.assertRaises(AttributeError, getattr, e, 'dynamic')
    self.assertRaises(AttributeError, getattr, e, '_absent')

    e.static = 'a'
    e.dynamic = 'b'
    self.assertEqual(e.static, 'a')
    self.assertEqual(e.dynamic, 'b')

    e = Ex(static='a', dynamic='b')
    self.assertEqual(e.static, 'a')
    self.assertEqual(e.dynamic, 'b')

    del e.static
    del e.dynamic
    self.assertEqual(e.static, None)
    self.assertRaises(AttributeError, getattr, e, 'dynamic')

  def testExpandoRepr(self):
    class Person(model.Expando):
      name = model.StringProperty('Name')
      city = model.StringProperty('City')
    p = Person(name='Guido', zip='00000')
    p.city = 'SF'
    self.assertEqual(repr(p),
                     "Person(city='SF', name='Guido', zip='00000')")
    # White box confirmation.
    self.assertEqual(p._values,
                     {'City': 'SF', 'Name': 'Guido', 'zip': '00000'})

  def testExpandoNested(self):
    p = model.Expando()
    nest = model.Expando()
    nest.foo = 42
    nest.bar = 'hello'
    p.nest = nest
    self.assertEqual(p.nest.foo, 42)
    self.assertEqual(p.nest.bar, 'hello')
    pb = p._to_pb()
    q = model.Expando._from_pb(pb)
    self.assertEqual(q.nest.foo, 42)
    self.assertEqual(q.nest.bar, 'hello')

  def testExpandoSubclass(self):
    class Person(model.Expando):
      name = model.StringProperty()
    p = Person()
    p.name = 'Joe'
    p.age = 7
    self.assertEqual(p.name, 'Joe')
    self.assertEqual(p.age, 7)

  def testExpandoConstructor(self):
    p = model.Expando(foo=42, bar='hello')
    self.assertEqual(p.foo, 42)
    self.assertEqual(p.bar, 'hello')
    pb = p._to_pb()
    q = model.Expando._from_pb(pb)
    self.assertEqual(q.foo, 42)
    self.assertEqual(q.bar, 'hello')

  def testExpandoNestedConstructor(self):
    p = model.Expando(foo=42, bar=model.Expando(hello='hello'))
    self.assertEqual(p.foo, 42)
    self.assertEqual(p.bar.hello, 'hello')
    pb = p._to_pb()
    q = model.Expando._from_pb(pb)
    self.assertEqual(q.foo, 42)
    self.assertEqual(q.bar.hello, 'hello')

  def testExpandoRepeatedProperties(self):
    p = model.Expando(foo=1, bar=[1, 2])
    p.baz = [3]
    self.assertFalse(p._properties['foo']._repeated)
    self.assertTrue(p._properties['bar']._repeated)
    self.assertTrue(p._properties['baz']._repeated)
    p.bar = 'abc'
    self.assertFalse(p._properties['bar']._repeated)
    pb = p._to_pb()
    q = model.Expando._from_pb(pb)
    q.key = None
    self.assertFalse(p._properties['foo']._repeated)
    self.assertFalse(p._properties['bar']._repeated)
    self.assertTrue(p._properties['baz']._repeated)
    self.assertEqual(q, model.Expando(foo=1, bar='abc', baz=[3]))

  def testExpandoUnindexedProperties(self):
    class Mine(model.Expando):
      pass
    a = Mine(foo=1, bar=['a', 'b'])
    self.assertTrue(a._properties['foo']._indexed)
    self.assertTrue(a._properties['bar']._indexed)
    a._default_indexed = False
    a.baz = 'baz'
    self.assertFalse(a._properties['baz']._indexed)
    Mine._default_indexed = False
    b = Mine(foo=1)
    b.bar = ['a', 'b']
    self.assertFalse(b._properties['foo']._indexed)
    self.assertFalse(b._properties['bar']._indexed)

  def testExpandoLocalStructuredProperty(self):
    class Inner(model.Model):
      name = model.StringProperty()

    class Outer(model.Model):
      inner = model.LocalStructuredProperty(Inner)
    x = Outer(inner=Inner(name='x'))
    key = x.put()

    class Outer(model.Expando):
      pass
    y = key.get()
    self.assertEqual(y.inner, model.Expando(name='x'))
    y.inner.name = 'x2'
    y.put()
    z = key.get()
    self.assertEqual(z.inner, model.Expando(name='x2'))
    z.inner = '42'
    z.put()
    u = key.get()
    self.assertEqual(u.inner, '42')

  def testExpandoLocalStructuredPropertyKeepKeys(self):
    class Inner(model.Model):
      name = model.StringProperty()

    class Outer(model.Model):
      inner = model.LocalStructuredProperty(Inner, keep_keys=True)
    x = Outer(inner=Inner(name='x'))
    key = x.put()

    class Outer(model.Expando):
      pass
    y = key.get()
    self.assertEqual(y.inner, Inner(name='x', key=model.Key(Inner, None)))
    y.inner.name = 'x2'
    y.put()
    z = key.get()
    self.assertEqual(z.inner, Inner(name='x2', key=model.Key(Inner, None)))

    # Remove Inner from the kind map.  We'll get inner back as Expando.
    del model.Model._kind_map['Inner']
    w = key.get()
    self.assertEqual(w.inner, model.Expando(name='x2',
                                            key=model.Key('Inner', None)))

    # Restore Inner in the kind map.
    model.Model._kind_map['Inner'] = Inner

    # It should be possible to replace an entity with a non-entity.
    z.inner = '42'
    z.put()
    u = key.get()
    self.assertEqual(u.inner, '42')

  def testExpandoLocalStructuredPropertyBadKey(self):
    class Inner(model.Model):
      name = model.StringProperty()

    class Outer(model.Model):
      inner = model.LocalStructuredProperty(Inner, keep_keys=True)
    x = Outer(inner=Inner(name='x'))
    pb = x._to_pb()
    v = pb.raw_property(0).value()
    sval = v.stringvalue()
    inner_pb = entity_pb.EntityProto()
    inner_pb.MergePartialFromString(sval)
    inner_pb.key().path().clear_element()
    sval = inner_pb.SerializePartialToString()
    v.set_stringvalue(sval)
    y = Outer._from_pb(pb)
    self.assertEqual(y.inner, Inner(name='x'))

  def testGenericProperty(self):
    class Goo(model.Model):
      prop = model.GenericProperty()

    for value in ("test",
                  1234,
                  12.34,
                  model.Key(flat=['Parent', 'a', 'Child', 'b']),
                  datetime.datetime(1982, 12, 1, 9, 0, 0),
                  model.GeoPt(1, 2),
                  users.User(email='test@example.com'),
                  model.BlobKey('testkey123')):
      g = Goo(prop=value)
      gKey = g.put()
      gCopy = gKey.get()
      self.assertEqual(g, gCopy)
      self.assertEqual(value, gCopy.prop)

  def testGenericPropertyWithModel(self):
    class Goo(model.Expando):
      prop = model.GenericProperty(indexed=False)

    g = Goo(prop=Goo(prop='a'))
    gKey = g.put()
    gCopy = gKey.get()
    self.assertEqual(g.prop.prop, gCopy.prop.prop)

  def testGenericPropertyInvalidLong(self):
    class Goo(model.Model):
      prop = model.GenericProperty()

    g = Goo(prop=key._MAX_LONG + 1)
    self.assertRaises(TypeError, g.put)

  def testGenericPropertyInvalidType(self):
    class Goo(model.Model):
      prop = model.GenericProperty()

    g = Goo(prop=datetime.time(12))
    self.assertRaises(NotImplementedError, g.put)

  def testGenericPropertyCompressedRefusesIndexed(self):
    self.assertRaises(NotImplementedError,
                      model.GenericProperty, compressed=True, indexed=True)

  def testGenericPropertyCompressed(self):
    class Goo(model.Model):
      comp = model.GenericProperty(compressed=True)
      comps = model.GenericProperty(compressed=True, repeated=True)
    self.assertFalse(Goo.comp._indexed)
    self.assertFalse(Goo.comps._indexed)
    a = Goo(comp='fizzy', comps=['x' * 1000, 'y' * 1000])
    a.put()
    self.assertTrue(isinstance(a._values['comp'].b_val,
                               model._CompressedValue))
    self.assertTrue(isinstance(a._values['comps'][0].b_val,
                               model._CompressedValue))
    self.assertTrue(isinstance(a._values['comps'][1].b_val,
                               model._CompressedValue))
    b = a.key.get()
    self.assertEqual(a, b)
    self.assertTrue(a is not b)
    # Extra-double-check.
    self.assertEqual(b.comp, 'fizzy')
    self.assertEqual(b.comps, ['x' * 1000, 'y' * 1000])
    # Now try some non-string values.
    x = Goo(comp=42, comps=[u'\u1234' * 1000, datetime.datetime(2012, 2, 23)])
    x.put()
    self.assertFalse(isinstance(x._values['comp'].b_val,
                                model._CompressedValue))
    self.assertFalse(isinstance(x._values['comps'][0].b_val,
                                model._CompressedValue))
    self.assertFalse(isinstance(x._values['comps'][1].b_val,
                                model._CompressedValue))
    y = x.key.get()
    self.assertEqual(x, y)

  def testExpandoReadsCompressed(self):
    class Goo(model.Model):
      comp = model.BlobProperty(compressed=True)
    x = Goo(comp='foo')
    x.put()

    class Goo(model.Expando):
      pass
    y = x.key.get()
    self.assertTrue(y._properties['comp']._compressed)
    self.assertEqual(y.comp, 'foo')

  def testComputedProperty(self):
    class ComputedTest(model.Model):
      name = model.StringProperty()
      name_lower = model.ComputedProperty(lambda self: self.name.lower())

      @model.ComputedProperty
      def length(self):
        return len(self.name)

      def _compute_hash(self):
        return hash(self.name)
      computed_hash = model.ComputedProperty(_compute_hash, name='hashcode')

    m = ComputedTest(name='Foobar')
    m._prepare_for_put()
    pb = m._to_pb()

    for p in pb.property_list():
      if p.name() == 'name_lower':
        self.assertEqual(p.value().stringvalue(), 'foobar')
        break
    else:
      self.assert_(False, "name_lower not found in PB")

    m = ComputedTest._from_pb(pb)
    self.assertEqual(m.name, 'Foobar')
    self.assertEqual(m.name_lower, 'foobar')
    self.assertEqual(m.length, 6)
    self.assertEqual(m.computed_hash, hash('Foobar'))

    self.assertRaises(model.ComputedPropertyError, m.__delattr__, 'name_lower')
    try:
      del m.name_lower
    except model.ComputedPropertyError:
      pass
    else:
      self.fail('A ComputedProperty cannot be deleted.')

    func = lambda unused_ent: None
    self.assertRaises(TypeError, model.ComputedProperty, func,
                      choices=('foo', 'bar'))
    self.assertRaises(TypeError, model.ComputedProperty, func, default='foo')
    self.assertRaises(TypeError, model.ComputedProperty, func, required=True)
    self.assertRaises(TypeError, model.ComputedProperty, func, validator=func)

  def testComputedPropertyRepeated(self):
    class StopWatch(model.Model):
      start = model.IntegerProperty()
      end = model.IntegerProperty()
      cp = model.ComputedProperty(lambda self: range(self.start, self.end),
                                  repeated=True)
    e = StopWatch(start=1, end=10)
    self.assertEqual(e.cp, [1, 2, 3, 4, 5, 6, 7, 8, 9])
    k = e.put()
    self.assertEqual(k.get().cp, [1, 2, 3, 4, 5, 6, 7, 8, 9])

    # Check that the computed property works when retrieved without cache
    ctx = tasklets.get_context()
    ctx.set_cache_policy(False)
    ctx.set_memcache_policy(False)
    self.assertEqual(k.get().cp, [1, 2, 3, 4, 5, 6, 7, 8, 9])

  def testComputedPropertyInRepeatedStructuredProperty(self):
    class Inner(model.Model):
      arg = model.IntegerProperty()
      comp1 = model.ComputedProperty(lambda ent: 1)
      comp2 = model.ComputedProperty(lambda ent: 2)

    class Outer(model.Model):
      wrap = model.StructuredProperty(Inner, repeated=True)
    orig = Outer(wrap=[Inner(arg=1), Inner(arg=2)])
    orig.put()
    copy = Outer.query().get()
    self.assertEqual(copy, orig)

  def testComputedPropertyWithVerboseName(self):
    class A(model.Model):
      arg = model.IntegerProperty()
      comp1 = model.ComputedProperty(lambda ent: 1, verbose_name="foo")
    a = A(arg=5)
    self.assertEqual(A.comp1._verbose_name, "foo")

  def testLargeValues(self):
    class Demo(model.Model):
      bytes = model.BlobProperty()
      text = model.TextProperty()
    x = Demo(bytes='x' * 1000, text=u'a' * 1000)
    key = x.put()
    y = key.get()
    self.assertEqual(x, y)
    self.assertTrue(isinstance(y.bytes, str))
    self.assertTrue(isinstance(y.text, unicode))

  def testMultipleStructuredPropertyDatastore(self):
    class Address(model.Model):
      label = model.StringProperty()
      text = model.StringProperty()

    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(Address, repeated=True)

    m = Person(name='Google',
               address=[Address(label='work', text='San Francisco'),
                        Address(label='home', text='Mountain View')])
    m.key = model.Key(flat=['Person', None])
    self.assertEqual(m.address[0].label, 'work')
    self.assertEqual(m.address[0].text, 'San Francisco')
    self.assertEqual(m.address[1].label, 'home')
    self.assertEqual(m.address[1].text, 'Mountain View')
    [k] = self.conn.put([m])
    m.key = k  # Connection.put() doesn't do this!
    [m2] = self.conn.get([k])
    self.assertEqual(m2, m)

  def testEmptyListWorksWithStructuredPropertyWithRepeatedSubstructure(self):
    class Address(model.Model):
      label = model.StringProperty(repeated=True, write_empty_list=True)
      text = model.StringProperty()

    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(Address)

    m = Person(name='Google',
               address=Address(label=[], text='San Francisco'))
    m.key = model.Key(flat=['Person', None])
    self.assertEqual(m.address.label, [])
    self.assertEqual(m.address.text, 'San Francisco')
    k = m.put()
    m2 = k.get()
    self.assertEqual(m2, m)

  def testEmptyListWorksWithRepeatedStructuredPropertys(self):
    class Address(model.Model):
      label = model.StringProperty()
      text = model.StringProperty()

    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(
          Address, repeated=True, write_empty_list=True)

    m = Person(name='Google',
               address=[])
    m.key = model.Key(flat=['Person', None])
    self.assertEqual(m.address, [])
    k = m.put()
    m2 = k.get()
    self.assertEqual(m2, m)

  def testIdAndParentPut(self):
    # id
    m = model.Model(id='bar')
    self.assertEqual(m.put(), model.Key('Model', 'bar'))

    # id + parent
    p = model.Key('ParentModel', 'foo')
    m = model.Model(id='bar', parent=p)
    self.assertEqual(m.put(), model.Key('ParentModel', 'foo', 'Model', 'bar'))

    # parent without id
    p = model.Key('ParentModel', 'foo')
    m = model.Model(parent=p)
    m.put()
    self.assertTrue(m.key.id())

  def testAllocateIds(self):
    class MyModel(model.Model):
      pass

    res = MyModel.allocate_ids(size=100)
    self.assertEqual(res, (1, 100))

    # with parent
    key = model.Key(flat=(MyModel._get_kind(), 1))
    res = MyModel.allocate_ids(size=200, parent=key)
    self.assertEqual(res, (101, 300))

  def testGetOrInsert(self):
    class MyModel(model.Model):
      text = model.StringProperty()

    key = model.Key(flat=(MyModel._get_kind(), 'baz'))
    self.assertEqual(key.get(), None)

    MyModel.get_or_insert('baz', text='baz')
    self.assertNotEqual(key.get(), None)
    self.assertEqual(key.get().text, 'baz')

  def testGetOrInsertAsync(self):
    class Mod(model.Model):
      data = model.StringProperty()

    @tasklets.tasklet
    def foo():
      ent = yield Mod.get_or_insert_async('a', data='hello')
      self.assertTrue(isinstance(ent, Mod))
      ent2 = yield Mod.get_or_insert_async('a', data='hello')
      self.assertEqual(ent2, ent)
    foo().check_success()

  def testGetOrInsertAsyncWithParent(self):
    class Mod(model.Model):
      data = model.StringProperty()

    @tasklets.tasklet
    def foo():
      parent = model.Key(flat=('Foo', 1))
      ent = yield Mod.get_or_insert_async('a', _parent=parent, data='hello')
      self.assertTrue(isinstance(ent, Mod))
      ent2 = yield Mod.get_or_insert_async('a', parent=parent, data='hello')
      self.assertEqual(ent2, ent)
    foo().check_success()

  def testGetOrInsertAsyncInTransaction(self):
    class Mod(model.Model):
      data = model.StringProperty()

    def txn():
      ent = Mod.get_or_insert('a', data='hola')
      self.assertTrue(isinstance(ent, Mod))
      ent2 = Mod.get_or_insert('a', data='hola2')
      self.assertEqual(ent2, ent)
      self.assertTrue(ent2 is ent)
      raise model.Rollback()

    # First with caching turned off.  (This works because the
    # transactional context always starts out with caching turned on.)
    model.transaction(txn)
    self.assertEqual(Mod.query().get(), None)

    # And again with caching turned on.
    ctx = tasklets.get_context()
    ctx.set_cache_policy(None)  # Restore default cache policy.
    model.transaction(txn)
    self.assertEqual(Mod.query().get(), None)

  def testGetOrInsertAsyncInTransactionUncacheableModel(self):
    class Mod(model.Model):
      _use_cache = False
      data = model.StringProperty()

    def txn():
      ent = Mod.get_or_insert('a', data='hola')
      self.assertTrue(isinstance(ent, Mod))
      ent2 = Mod.get_or_insert('a', data='hola2')
      self.assertEqual(ent2.data, 'hola2')
      raise model.Rollback()

    # First with caching turned off.
    model.transaction(txn)
    self.assertEqual(Mod.query().get(), None)

    # And again with caching turned on.
    ctx = tasklets.get_context()
    ctx.set_cache_policy(None)  # Restore default cache policy.
    model.transaction(txn)
    self.assertEqual(Mod.query().get(), None)

  def testGetById(self):
    class MyModel(model.Model):
      pass

    kind = MyModel._get_kind()

    # key id
    ent1 = MyModel(key=model.Key(pairs=[(kind, 1)]))
    ent1.put()
    res = MyModel.get_by_id(1)
    self.assertEqual(res, ent1)

    # key name
    ent2 = MyModel(key=model.Key(pairs=[(kind, 'foo')]))
    ent2.put()
    res = MyModel.get_by_id('foo')
    self.assertEqual(res, ent2)

    # key id + parent
    ent3 = MyModel(key=model.Key(pairs=[(kind, 1), (kind, 2)]))
    ent3.put()
    res = MyModel.get_by_id(2, parent=model.Key(pairs=[(kind, 1)]))
    self.assertEqual(res, ent3)

    # key name + parent (positional)
    ent4 = MyModel(key=model.Key(pairs=[(kind, 1), (kind, 'bar')]))
    ent4.put()
    res = MyModel.get_by_id('bar', ent1.key)
    self.assertEqual(res, ent4)

    # None
    res = MyModel.get_by_id('idontexist')
    self.assertEqual(res, None)

    # key id + namespace
    ent5 = MyModel(key=model.Key(kind, 1, namespace='ns'))
    ent5.put()
    res = MyModel.get_by_id(1, namespace='ns')
    self.assertEqual(res, ent5)

    # Invalid parent
    self.assertRaises(datastore_errors.BadValueError, MyModel.get_by_id,
                      'bar', parent=1)

  def testDelete(self):
    class MyModel(model.Model):
      pass

    ent1 = MyModel()
    key1 = ent1.put()
    ent2 = key1.get()
    self.assertEqual(ent1, ent2)
    key1.delete()
    ent3 = key1.get()
    self.assertEqual(ent3, None)

  def testPopulate(self):
    class MyModel(model.Model):
      name = model.StringProperty()
    m = MyModel()
    m.populate(name='abc')
    self.assertEqual(m.name, 'abc')
    m.populate(name='def')
    self.assertEqual(m.name, 'def')
    self.assertRaises(AttributeError, m.populate, foo=42)

  def testPopulate_Expando(self):
    class Ex(model.Expando):
      name = model.StringProperty()
    m = Ex()
    m.populate(name='abc')
    self.assertEqual(m.name, 'abc')
    m.populate(foo=42)
    self.assertEqual(m.foo, 42)

  def testTransaction(self):
    class MyModel(model.Model):
      text = model.StringProperty()

    key = model.Key(MyModel, 'babaz')
    self.assertEqual(key.get(), None)

    def callback():
      # Emulate get_or_insert()
      a = key.get()
      if a is None:
        a = MyModel(text='baz', key=key)
        a.put()
      return a

    b = model.transaction(callback)
    self.assertNotEqual(b, None)
    self.assertEqual(b.text, 'baz')
    self.assertEqual(key.get(), b)

    key = model.Key(MyModel, 'bababaz')
    self.assertEqual(key.get(), None)
    c = model.transaction(callback, retries=0)
    self.assertNotEqual(c, None)
    self.assertEqual(c.text, 'baz')
    self.assertEqual(key.get(), c)

  def testNoNestedTransactions(self):
    self.ExpectWarnings()

    class MyModel(model.Model):
      text = model.StringProperty()

    key = model.Key(MyModel, 'schtroumpf')
    self.assertEqual(key.get(), None)

    def inner():
      self.fail('Should not get here')

    def outer():
      model.transaction(inner)

    self.assertRaises(datastore_errors.BadRequestError,
                      model.transaction, outer)

  def testGetMultiAsync(self):
    model.Model._kind_map['Model'] = model.Model
    ent1 = model.Model(key=model.Key('Model', 1))
    ent2 = model.Model(key=model.Key('Model', 2))
    ent3 = model.Model(key=model.Key('Model', 3))
    key1 = ent1.put()
    key2 = ent2.put()
    key3 = ent3.put()

    @tasklets.tasklet
    def foo():
      ents = yield model.get_multi_async([key1, key2, key3])
      raise tasklets.Return(ents)

    res = foo().get_result()
    self.assertEqual(res, [ent1, ent2, ent3])

  def testGetMulti(self):
    model.Model._kind_map['Model'] = model.Model
    ent1 = model.Model(key=model.Key('Model', 1))
    ent2 = model.Model(key=model.Key('Model', 2))
    ent3 = model.Model(key=model.Key('Model', 3))
    key1 = ent1.put()
    key2 = ent2.put()
    key3 = ent3.put()

    res = model.get_multi((key1, key2, key3))
    self.assertEqual(res, [ent1, ent2, ent3])

  def testPutMultiAsync(self):
    ent1 = model.Model(key=model.Key('Model', 1))
    ent2 = model.Model(key=model.Key('Model', 2))
    ent3 = model.Model(key=model.Key('Model', 3))

    @tasklets.tasklet
    def foo():
      ents = yield model.put_multi_async([ent1, ent2, ent3])
      raise tasklets.Return(ents)

    res = foo().get_result()
    self.assertEqual(res, [ent1.key, ent2.key, ent3.key])

  def testPutMulti(self):
    ent1 = model.Model(key=model.Key('Model', 1))
    ent2 = model.Model(key=model.Key('Model', 2))
    ent3 = model.Model(key=model.Key('Model', 3))

    res = model.put_multi((ent1, ent2, ent3))
    self.assertEqual(res, [ent1.key, ent2.key, ent3.key])

  def testPutMultiRepeatedKey(self):
    ent1 = model.Model(key=model.Key('Model', 1))

    res = model.put_multi((ent1, ent1))
    self.assertEqual(res, [ent1.key, ent1.key])

  def testDeleteMultiAsync(self):
    model.Model._kind_map['Model'] = model.Model
    ent1 = model.Model(key=model.Key('Model', 1))
    ent2 = model.Model(key=model.Key('Model', 2))
    ent3 = model.Model(key=model.Key('Model', 3))
    key1 = ent1.put()
    key2 = ent2.put()
    key3 = ent3.put()

    self.assertEqual(key1.get(), ent1)
    self.assertEqual(key2.get(), ent2)
    self.assertEqual(key3.get(), ent3)

    @tasklets.tasklet
    def foo():
      ents = yield model.delete_multi_async([key1, key2, key3])
      raise tasklets.Return(ents)

    foo().get_result()
    self.assertEqual(key1.get(), None)
    self.assertEqual(key2.get(), None)
    self.assertEqual(key3.get(), None)

  def testDeleteMulti(self):
    model.Model._kind_map['Model'] = model.Model
    ent1 = model.Model(key=model.Key('Model', 1))
    ent2 = model.Model(key=model.Key('Model', 2))
    ent3 = model.Model(key=model.Key('Model', 3))
    key1 = ent1.put()
    key2 = ent2.put()
    key3 = ent3.put()

    self.assertEqual(key1.get(), ent1)
    self.assertEqual(key2.get(), ent2)
    self.assertEqual(key3.get(), ent3)

    model.delete_multi((key1, key2, key3))

    self.assertEqual(key1.get(), None)
    self.assertEqual(key2.get(), None)
    self.assertEqual(key3.get(), None)

  def testDeleteMultiRepeatedKey(self):
    model.Model._kind_map['Model'] = model.Model
    ent1 = model.Model(key=model.Key('Model', 1))
    key1 = ent1.put()

    self.assertEqual(key1.get(), ent1)

    model.delete_multi((key1, key1))

    self.assertEquals(key1.get(), None)

  def testContextOptions(self):
    ctx = tasklets.get_context()
    ctx.set_cache_policy(True)
    ctx.set_memcache_policy(True)
    ctx.set_memcache_timeout_policy(0)
    # Create an entity and put it in the caches.

    class MyModel(model.Model):
      name = model.StringProperty()
    key = model.Key(MyModel, 'yo')
    ent = MyModel(key=key, name='yo')
    ent.put(use_memcache=False)  # Don't lock memcache.
    key.get(use_cache=False)  # Write to memcache.
    eventloop.run()  # Wait for async memcache request to complete.
    # Verify that it is in both caches.
    self.assertTrue(ctx._cache[key] is ent)
    self.assertEqual(memcache.get(ctx._memcache_prefix + key.urlsafe()),
                     ent._to_pb(set_key=False).SerializePartialToString())
    # Get it bypassing the in-process cache.
    ent_copy = key.get(use_cache=False)
    self.assertEqual(ent_copy, ent)
    self.assertFalse(ent_copy is ent)
    # Put it bypassing both caches.
    ent_copy.name = 'yoyo'
    ent_copy.put(use_cache=False, use_memcache=False)
    # Get it from the in-process cache.
    ent2 = key.get()
    self.assertTrue(ent2 is ent)
    self.assertEqual(ent2.name, 'yo')
    self.assertEqual(ent_copy.name, 'yoyo')  # Should not have changed.
    # Get it from memcache.
    ent3 = key.get(use_cache=False)
    self.assertFalse(ent3 is ent)
    self.assertFalse(ent3 is ent2)
    self.assertEqual(ent3.name, 'yo')
    self.assertEqual(ent_copy.name, 'yoyo')  # Should not have changed.
    # Get it from Cloud Datastore.
    ent4 = key.get(use_cache=False, use_memcache=False)
    self.assertFalse(ent4 is ent)
    self.assertFalse(ent4 is ent2)
    self.assertFalse(ent4 is ent3)
    self.assertFalse(ent4 is ent_copy)
    self.assertEqual(ent4.name, 'yoyo')
    # Delete it from Cloud Datastore but leave it in the caches.
    key.delete(use_cache=False, use_memcache=False)
    # Assure it is gone from Cloud Datastore.
    [ent5] = model.get_multi([key],
                             use_cache=False, use_memcache=False)
    self.assertEqual(ent5, None)
    # Assure it is still in memcache.
    ent6 = key.get(use_cache=False)
    self.assertEqual(ent6.name, 'yo')
    self.assertEqual(memcache.get(ctx._memcache_prefix + key.urlsafe()),
                     ent._to_pb(set_key=False).SerializePartialToString())
    # Assure it is still in the in-memory cache.
    ent7 = key.get()
    self.assertEqual(ent7.name, 'yo')
    self.assertTrue(ctx._cache[key] is ent7)
    # Delete it from memcache.
    model.delete_multi([key], use_cache=False)
    # Assure it is gone from memcache.
    ent8 = key.get(use_cache=False)
    self.assertEqual(ent8, None)
    # Assure it is still in the in-memory cache.
    ent9 = key.get()
    self.assertEqual(ent9.name, 'yo')
    self.assertTrue(ctx._cache[key] is ent9)
    # Delete it from the in-memory cache.
    key.delete()
    # Assure it is gone.
    ent10 = key.get()
    self.assertEqual(ent10, None)

  def testContextOptions_Timeouts(self):
    # Tweak the context.
    ctx = tasklets.get_context()
    ctx.set_cache_policy(True)
    ctx.set_memcache_policy(True)
    ctx.set_memcache_timeout_policy(0)
    # Mock memcache.cas_multi_async().
    save_memcache_cas_multi_async = ctx._memcache.cas_multi_async
    memcache_args_log = []

    def mock_memcache_cas_multi_async(*args, **kwds):
      memcache_args_log.append((args, kwds))
      return save_memcache_cas_multi_async(*args, **kwds)
    # Mock conn.async_put().
    save_conn_async_put = ctx._conn.async_put
    conn_args_log = []

    def mock_conn_async_put(*args, **kwds):
      conn_args_log.append((args, kwds))
      return save_conn_async_put(*args, **kwds)
    # Create some entities.

    class MyModel(model.Model):
      name = model.StringProperty()
    e1 = MyModel(name='1')
    e2 = MyModel(name='2')
    e3 = MyModel(name='3')
    e4 = MyModel(name='4')
    e5 = MyModel(name='5')
    # Test that the timeouts make it through to memcache and Cloud Datastore.
    try:
      ctx._memcache.cas_multi_async = mock_memcache_cas_multi_async
      ctx._conn.async_put = mock_conn_async_put
      [f1, f3] = model.put_multi_async([e1, e3],
                                       memcache_timeout=7,
                                       deadline=3)
      [f4] = model.put_multi_async([e4],
                                   deadline=2)
      [x2, x5] = model.put_multi([e2, e5],
                                 memcache_timeout=5)
      x4 = f4.get_result()
      x1 = f1.get_result()
      x3 = f3.get_result()
      # Write to memcache.
      model.get_multi([x1, x3], use_cache=False, memcache_timeout=7)
      model.get_multi([x4], use_cache=False)
      model.get_multi([x2, x5], use_cache=False, memcache_timeout=5)
      eventloop.run()  # Wait for async memcache request to complete.
      # (And there are straggler events too, but they don't matter here.)
    finally:
      ctx._memcache.cas_multi_async = save_memcache_cas_multi_async
      ctx._conn.async_put = save_conn_async_put
    self.assertEqual([e1.key, e2.key, e3.key, e4.key, e5.key],
                     [x1, x2, x3, x4, x5])
    self.assertEqual(len(memcache_args_log), 3, memcache_args_log)
    timeouts = set(kwds['time'] for _, kwds in memcache_args_log)
    self.assertEqual(timeouts, set([0, 5, 7]))
    self.assertEqual(len(conn_args_log), 3)
    deadlines = set(args[0]._values.get('deadline')
                    for (args, kwds) in conn_args_log)
    self.assertEqual(deadlines, set([None, 2, 3]))

  def testContextOptions_ThreeLevels(self):
    # Reset policies to default.
    ctx = tasklets.get_context()
    ctx.set_cache_policy(None)
    ctx.set_memcache_policy(None)
    ctx.set_memcache_timeout_policy(None)

    class M(model.Model):
      s = model.StringProperty()

    k = model.Key(M, '1')
    a = M(s='a', key=k)
    b = M(s='b', key=k)
    c = M(s='c', key=k)

    a.put(use_cache=True, use_memcache=False, use_datastore=False)
    b.put(use_cache=False, use_memcache=True, use_datastore=False)
    c.put(use_cache=False, use_memcache=False, use_datastore=True)

    self.assertEqual(ctx._cache[k], a)
    self.assertEqual(memcache.get(ctx._memcache_prefix + k.urlsafe()),
                     b._to_pb(set_key=False).SerializePartialToString())
    self.assertEqual(ctx._conn.get([k]), [c])

    self.assertEqual(k.get(), a)
    self.assertEqual(k.get(use_cache=False), b)
    self.assertEqual(k.get(use_cache=False, use_memcache=False), c)

    k.delete(use_cache=True, use_memcache=False, use_datastore=False)
    # Note: it is now in the Context cache marked as deleted.
    self.assertEqual(k.get(use_cache=False), b)
    k.delete(use_cache=False, use_memcache=True, use_datastore=False)
    self.assertEqual(k.get(use_cache=False), c)
    k.delete(use_cache=False, use_memcache=False, use_datastore=True)
    self.assertEqual(k.get(use_cache=False), None)

  def testContextOptions_PerClass(self):
    # Reset policies to default.
    ctx = tasklets.get_context()
    ctx.set_cache_policy(None)
    ctx.set_memcache_policy(None)
    ctx.set_memcache_timeout_policy(None)

    class M(model.Model):
      s = model.StringProperty()
      _use_cache = False

      @classmethod
      def _use_memcache(cls, key):
        return bool(key.string_id())

      @classmethod
      def _use_datastore(cls, key):
        return not bool(key.string_id())

    a = M(s='a', key=model.Key(M, 'a'))  # Uses memcache only
    b = M(s='b', key=model.Key(M, None))  # Uses datastore only
    a.put()
    b.put()

    self.assertFalse(a.key in ctx._cache)
    self.assertFalse(b.key in ctx._cache)
    self.assertEqual(memcache.get(ctx._memcache_prefix + a.key.urlsafe()),
                     a._to_pb(set_key=False).SerializePartialToString())
    self.assertEqual(memcache.get(ctx._memcache_prefix + b.key.urlsafe()), None)
    self.assertEqual(ctx._conn.get([a.key]), [None])
    self.assertEqual(ctx._conn.get([b.key]), [b])

  def testNamespaces(self):
    save_namespace = namespace_manager.get_namespace()
    try:
      namespace_manager.set_namespace('ns1')
      k1 = model.Key('A', 1)
      self.assertEqual(k1.namespace(), 'ns1')
      k2 = model.Key('B', 2, namespace='ns2')
      self.assertEqual(k2.namespace(), 'ns2')
      namespace_manager.set_namespace('ns3')
      self.assertEqual(k1.namespace(), 'ns1')
      k3 = model.Key('C', 3, parent=k1)
      self.assertEqual(k3.namespace(), 'ns1')

      # Test that namespaces survive serialization
      namespace_manager.set_namespace('ns2')
      km = model.Key('M', 1, namespace='ns4')

      class M(model.Model):
        keys = model.KeyProperty(repeated=True)
      m1 = M(keys=[k1, k2, k3], key=km)
      pb = m1._to_pb()
      namespace_manager.set_namespace('ns3')
      m2 = M._from_pb(pb)
      self.assertEqual(m1, m2)
      self.assertEqual(m2.keys[0].namespace(), 'ns1')
      self.assertEqual(m2.keys[1].namespace(), 'ns2')
      self.assertEqual(m2.keys[2].namespace(), 'ns1')

      # Now test the same thing for Expando
      namespace_manager.set_namespace('ns2')
      ke = model.Key('E', 1)

      class E(model.Expando):
        pass
      e1 = E(keys=[k1, k2, k3], key=ke)
      pb = e1._to_pb()
      namespace_manager.set_namespace('ns3')
      e2 = E._from_pb(pb)
      self.assertEqual(e1, e2)

      # Test that an absent namespace always means the empty namespace
      namespace_manager.set_namespace('')
      k3 = model.Key('E', 2)
      e3 = E(key=k3, k=k3)
      pb = e3._to_pb()
      namespace_manager.set_namespace('ns4')
      e4 = E._from_pb(pb)
      self.assertEqual(e4.key.namespace(), '')
      self.assertEqual(e4.k.namespace(), '')

    finally:
      namespace_manager.set_namespace(save_namespace)

  def testOverrideModelKey(self):
    class MyModel(model.Model):
      # key, overridden
      key = model.StringProperty()
      # aha, here it is!
      real_key = model.ModelKey()

    class MyExpando(model.Expando):
      # key, overridden
      key = model.StringProperty()
      # aha, here it is!
      real_key = model.ModelKey()

    m = MyModel()
    k = model.Key('MyModel', 'foo')
    m.key = 'bar'
    m.real_key = k
    m.put()

    res = k.get()
    self.assertEqual(res, m)
    self.assertEqual(res.key, 'bar')
    self.assertEqual(res.real_key, k)

    q = MyModel.query(MyModel.real_key == k)
    res = q.get()
    self.assertEqual(res, m)
    self.assertEqual(res.key, 'bar')
    self.assertEqual(res.real_key, k)

    m = MyExpando()
    k = model.Key('MyExpando', 'foo')
    m.key = 'bar'
    m.real_key = k
    m.put()

    res = k.get()
    self.assertEqual(res, m)
    self.assertEqual(res.key, 'bar')
    self.assertEqual(res.real_key, k)

    q = MyExpando.query(MyModel.real_key == k)
    res = q.get()
    self.assertEqual(res, m)
    self.assertEqual(res.key, 'bar')
    self.assertEqual(res.real_key, k)

  def testTransactionalDecorator(self):
    # This tests @model.transactional and model.in_transaction(), and
    # indirectly context.Context.in_transaction().
    logs = []

    @model.transactional
    def foo(a, b):
      self.assertTrue(model.in_transaction())
      logs.append(tasklets.get_context()._conn)  # White box
      return a + b

    @model.transactional
    def bar(a):
      self.assertTrue(model.in_transaction())
      logs.append(tasklets.get_context()._conn)  # White box
      return foo(a, 42)
    before = tasklets.get_context()._conn
    self.assertFalse(model.in_transaction())
    x = bar(100)
    self.assertFalse(model.in_transaction())
    after = tasklets.get_context()._conn
    self.assertEqual(before, after)
    self.assertEqual(x, 142)
    self.assertEqual(len(logs), 2)
    self.assertEqual(logs[0], logs[1])
    self.assertNotEqual(before, logs[0])

  def testTransactionAync(self):
    @model.transactional_async(propagation=context.TransactionOptions.ALLOWED)
    def fn(x):
      assert model.in_transaction()
      return 'hi'

    self.assertEqual('hi', fn(0.2).get_result())

  def testTransactionTasklet(self):
    @model.transactional_tasklet(propagation=context.TransactionOptions.ALLOWED)
    def fn(x):
      assert model.in_transaction()
      yield tasklets.sleep(x)
      raise tasklets.Return('hi')

    self.assertEqual('hi', fn(0.2).get_result())

  def testTransactionalDecoratorExtensions(self):
    # Test that @transactional(flag=value, ...) works too.
    @model.transactional()
    def callback1(log):
      self.assertTrue(model.in_transaction())
      ctx = tasklets.get_context()
      orig_async_commit = ctx._conn.async_commit

      def wrap_async_commit(options):
        log.append(options)
        return orig_async_commit(options)
      ctx._conn.async_commit = wrap_async_commit
    log = []
    callback1(log)
    self.assertEqual(
        log,
        [context.TransactionOptions(
            propagation=context.TransactionOptions.ALLOWED)])

    @model.transactional(retries=42)
    def callback2(log):
      self.assertTrue(model.in_transaction())
      ctx = tasklets.get_context()
      orig_async_commit = ctx._conn.async_commit

      def wrap_async_commit(options):
        log.append(options)
        return orig_async_commit(options)
      ctx._conn.async_commit = wrap_async_commit
    log = []
    callback2(log)
    self.assertEqual(len(log), 1)
    self.assertEqual(log[0].retries, 42)

    @model.transactional(retries=2)
    def callback3():
      self.assertTrue(model.in_transaction())
      ctx = tasklets.get_context()
      orig_async_commit = ctx._conn.async_commit

      def wrap_async_commit(options):
        log.append(options)
        return orig_async_commit(options)
      ctx._conn.async_commit = wrap_async_commit
    log = []
    callback3()
    self.assertEqual(len(log), 1)
    self.assertEqual(log[0].retries, 2)

  def testTransactionalDecoratorPropagationOptions(self):
    # Test @transactional(propagation=<flag>) for all supported <flag>
    # values and in_transaction() states.
    self.ExpectWarnings()

    class Counter(model.Model):
      count = model.IntegerProperty(default=0)

    def increment(key, delta=1):
      ctx = tasklets.get_context()
      ent = key.get()
      if ent is None:
        ent = Counter(count=delta, key=key)
      else:
        ent.count += delta
      ent.put()
      return (ent.key, ctx)

    # *** Not currently in a transaction. ***
    octx = tasklets.get_context()
    self.assertFalse(octx.in_transaction())
    key = model.Key(Counter, 'a')
    # Undecorated -- runs in current context
    nkey, nctx = increment(key)
    self.assertTrue(nctx is octx)
    self.assertEqual(nkey, key)
    self.assertEqual(key.get().count, 1)
    # propagation=NESTED -- creates new transaction
    flag = context.TransactionOptions.NESTED
    nkey, nctx = model.transactional(propagation=flag)(increment)(key)
    self.assertRaises(TypeError, model.transactional,
                      increment, propagation=flag)
    self.assertRaises(TypeError, model.transactional(flag), increment)
    self.assertTrue(nctx is not octx)
    self.assertTrue(nctx.in_transaction())
    self.assertEqual(nkey, key)
    self.assertEqual(nkey.get().count, 2)
    # propagation=MANDATORY -- error
    flag = context.TransactionOptions.MANDATORY
    self.assertRaises(datastore_errors.BadRequestError,
                      model.transactional(propagation=flag)(increment), key)
    # propagation=ALLOWED -- creates new transaction
    flag = context.TransactionOptions.ALLOWED
    nkey, nctx = model.transactional(propagation=flag)(increment)(key)
    self.assertTrue(nctx is not octx)
    self.assertTrue(nctx.in_transaction())
    self.assertEqual(nkey, key)
    self.assertEqual(nkey.get().count, 3)
    # propagation=INDEPENDENT -- creates new transaction
    flag = context.TransactionOptions.INDEPENDENT
    nkey, nctx = model.transactional(propagation=flag)(increment)(key)
    self.assertTrue(nctx is not octx)
    self.assertTrue(nctx.in_transaction())
    self.assertEqual(nkey, key)
    self.assertEqual(nkey.get().count, 4)
    # propagation=None -- creates new transaction
    flag = None
    nkey, nctx = model.transactional(propagation=flag)(increment)(key)
    self.assertTrue(nctx is not octx)
    self.assertTrue(nctx.in_transaction())
    self.assertEqual(nkey, key)
    self.assertEqual(nkey.get().count, 5)
    # propagation not set -- creates new transaction
    nkey, nctx = model.transactional()(increment)(key)
    self.assertTrue(nctx is not octx)
    self.assertTrue(nctx.in_transaction())
    self.assertEqual(nkey, key)
    self.assertEqual(nkey.get().count, 6)

    # *** Currently in a transaction. ***
    def callback():
      octx = tasklets.get_context()
      self.assertTrue(octx.in_transaction())
      key = model.Key(Counter, 'b')
      # Undecorated -- runs in current context
      nkey, nctx = increment(key)
      self.assertTrue(nctx is octx)
      self.assertEqual(nkey, key)
      self.assertEqual(key.get().count, 1)
      # propagation=NESTED -- error
      flag = context.TransactionOptions.NESTED
      self.assertRaises(datastore_errors.BadRequestError,
                        model.transactional(propagation=flag)(increment), key)
      # propagation=MANDATORY -- runs in current context
      flag = context.TransactionOptions.MANDATORY
      nkey, nctx = model.transactional(propagation=flag)(increment)(key)
      self.assertTrue(nctx is octx)
      self.assertEqual(nkey, key)
      self.assertEqual(nkey.get().count, 2)
      # propagation=ALLOWED -- runs in current context
      flag = context.TransactionOptions.ALLOWED
      nkey, nctx = model.transactional(propagation=flag)(increment)(key)
      self.assertTrue(nctx is octx)
      self.assertEqual(nkey, key)
      self.assertEqual(nkey.get().count, 3)
      # propagation=INDEPENDENT -- creates new transaction
      flag = context.TransactionOptions.INDEPENDENT
      nkey, nctx = model.transactional(propagation=flag)(increment)(key)
      self.assertTrue(nctx is not octx)
      self.assertTrue(nctx.in_transaction())
      self.assertEqual(nkey, key)
      # Interesting!  The current transaction doesn't see the update
      self.assertEqual(nkey.get().count, 3)
      # Outside the transaction it's up to 1
      get_count = model.non_transactional(lambda: nkey.get().count)
      self.assertEqual(get_count(), 1)
      # propagation=None -- implies NESTED, raises an error
      flag = None
      self.assertRaises(datastore_errors.BadRequestError,
                        model.transactional(propagation=flag)(increment), key)
      # propagation not set -- implies ALLOWED, runs in current context
      nkey, nctx = model.transactional()(increment)(key)
      self.assertTrue(nctx is octx)
      self.assertEqual(nkey, key)
      self.assertEqual(nkey.get().count, 4)
      # Raise a unique exception so the outer test code can tell we
      # made it all the way here.
      raise ZeroDivisionError

    # Run the callback in a transaction.  It should reach the end and
    # then raise ZeroDivisionError.
    self.assertRaises(ZeroDivisionError, model.transaction, callback)
    # One independent transaction has bumped the count.
    self.assertEqual(model.Key(Counter, 'b').get().count, 1)

  def testNonTransactionalDecorator(self):
    # Test @non_transactional() with all possible formats and all
    # possible values for allow_existing and in_transaction().
    self.ExpectWarnings()

    class Counter(model.Model):
      count = model.IntegerProperty(default=0)

    class DbCounter(db.Model):
      count = db.IntegerProperty(default=0)

      @classmethod
      def kind(cls):
        return Counter._get_kind()

    def increment(key, delta=1):
      ctx = tasklets.get_context()
      ent = key.get()
      if ent is None:
        ent = Counter(count=delta, key=key)
      else:
        ent.count += delta
      ent.put()
      return (ent.key, ctx)

    def increment_db(key, delta=1):
      ent = db.get(key.to_old_key())
      if ent is None:
        ent = DbCounter(count=delta, key=key.to_old_key())
      else:
        ent.count += delta
      ent.put()
      return db.is_in_transaction()

    # *** Not currently in a transaction. ***
    octx = tasklets.get_context()
    self.assertFalse(octx.in_transaction())
    key = model.Key(Counter, 'x')
    # Undecorated
    nkey, nctx = increment(key)
    self.assertTrue(nctx is octx)
    self.assertEqual(nkey, key)
    self.assertEqual(key.get().count, 1)
    # Vanilla decorated
    key, nctx = model.non_transactional(increment)(key)
    self.assertTrue(nctx is octx)
    self.assertEqual(nkey, key)
    self.assertEqual(key.get().count, 2)
    # Decorated without options
    key, nctx = model.non_transactional()(increment)(key)
    self.assertTrue(nctx is octx)
    self.assertEqual(nkey, key)
    self.assertEqual(key.get().count, 3)
    # Decorated with allow_existing=True
    key, nctx = model.non_transactional(allow_existing=True)(increment)(key)
    self.assertTrue(nctx is octx)
    self.assertEqual(nkey, key)
    self.assertEqual(key.get().count, 4)
    # Decorated with allow_existing=False
    key, nctx = model.non_transactional(allow_existing=False)(increment)(key)
    self.assertTrue(nctx is octx)
    self.assertEqual(nkey, key)
    self.assertEqual(key.get().count, 5)

    # *** Currently in a transaction. ***
    def callback():
      octx = tasklets.get_context()
      self.assertTrue(octx.in_transaction())
      key = model.Key(Counter, 'y')
      # Undecorated -- runs in this context
      nkey, nctx = increment(key)
      self.assertTrue(nctx is octx)
      self.assertEqual(nkey, key)
      self.assertEqual(key.get().count, 1)
      # Vanilla decorated -- runs in different context
      key, nctx = model.non_transactional(increment)(key)
      self.assertTrue(nctx is not octx)
      self.assertFalse(nctx.in_transaction())
      self.assertEqual(nkey, key)
      self.assertEqual(key.get().count, 1)
      # Decorated without options -- runs in different context
      key, nctx = model.non_transactional()(increment)(key)
      self.assertTrue(nctx is not octx)
      self.assertFalse(nctx.in_transaction())
      self.assertEqual(nkey, key)
      self.assertEqual(key.get().count, 1)
      # Decorated with allow_existing=True
      key, nctx = model.non_transactional(allow_existing=True)(increment)(key)
      self.assertTrue(nctx is not octx)
      self.assertFalse(nctx.in_transaction())
      self.assertEqual(key.get().count, 1)
      # Decorated with allow_existing=False -- raises exception
      self.assertRaises(
          datastore_errors.BadRequestError,
          model.non_transactional(allow_existing=False)(increment),
          key)
      # db also respects non_transactional.
      self.assertFalse(model.non_transactional(increment_db)(key))
      self.assertEqual(key.get().count, 1)
      # db non_transactional fails (technically this is undefined behavior, but
      # we prefer a loud failure).
      self.assertRaises(Exception, db.non_transactional(increment_db), key)
      # Make sure we are still in a transaction.
      self.assertTrue(tasklets.get_context().in_transaction())
      self.assertTrue(db.is_in_transaction())
      # Raise a unique exception so the outer test code can tell we
      # made it all the way here.
      raise ZeroDivisionError

    # Run the callback in a transaction.  It should reach the end and
    # then raise ZeroDivisionError.
    self.assertRaises(ZeroDivisionError, model.transaction, callback)
    # Three non-transactional calls have bumped the count.
    self.assertEqual(model.Key(Counter, 'y').get().count, 4)

  def testPropertyFilters(self):
    class M(model.Model):
      dt = model.DateTimeProperty()
      d = model.DateProperty()
      t = model.TimeProperty()
      f = model.FloatProperty()
      s = model.StringProperty()
      k = model.KeyProperty()
      b = model.BooleanProperty()
      i = model.IntegerProperty()
      g = model.GeoPtProperty()

      @model.ComputedProperty
      def c(self):
        return self.i + 1
      u = model.UserProperty()

    values = {
        'dt': datetime.datetime.now(),
        'd': datetime.date.today(),
        't': datetime.datetime.now().time(),
        'f': 4.2,
        's': 'foo',
        'k': model.Key('Foo', 'bar'),
        'b': False,
        'i': 42,
        'g': AMSTERDAM,
        'u': TESTUSER,
    }

    m = M(**values)
    m.put()

    q = M.query(M.dt == values['dt'])
    self.assertEqual(q.get(), m)

    q = M.query(M.d == values['d'])
    self.assertEqual(q.get(), m)

    q = M.query(M.t == values['t'])
    self.assertEqual(q.get(), m)

    q = M.query(M.f == values['f'])
    self.assertEqual(q.get(), m)

    q = M.query(M.s == values['s'])
    self.assertEqual(q.get(), m)

    q = M.query(M.k == values['k'])
    self.assertEqual(q.get(), m)

    q = M.query(M.b == values['b'])
    self.assertEqual(q.get(), m)

    q = M.query(M.i == values['i'])
    self.assertEqual(q.get(), m)

    q = M.query(M.g == values['g'])
    self.assertEqual(q.get(), m)

    q = M.query(M.c == values['i'] + 1)
    self.assertEqual(q.get(), m)

    q = M.query(M.u == values['u'])
    self.assertEqual(q.get(), m)

  def testNonRepeatedListValue(self):
    class ReprProperty(model.BlobProperty):

      def _validate(self, value):
        # dummy
        return value

      def _to_base_type(self, value):
        if not isinstance(value, str):
          value = value.__repr__()
        return value

      def _from_base_type(self, value):
        if isinstance(value, str):
          value = eval(value)
        return value

    class M(model.Model):
      p1 = ReprProperty()
      p2 = ReprProperty(compressed=True)
      p3 = ReprProperty(repeated=True)
      p4 = ReprProperty(compressed=True, repeated=True)

    key1 = model.Key(M, 'test')
    value = [{'foo': 'bar'}, {'baz': 'ding'}]
    m1 = M(key=key1, p1=value, p2=value, p3=[value, value], p4=[value, value])
    m1.put()

    # To test compression and deserialization with untouched properties.
    m2 = key1.get()
    m2.put()

    m2 = key1.get()
    self.assertEqual(m2.p1, value)
    self.assertEqual(m2.p2, value)
    self.assertEqual(m2.p3, [value, value])
    self.assertEqual(m2.p4, [value, value])

    # To test compression and deserialization after properties were accessed.
    m2.put()

  def testCompressedProperty(self):
    class M(model.Model):
      t1 = model.TextProperty()
      t2 = model.TextProperty(compressed=True)
      t3 = model.TextProperty(repeated=True)
      t4 = model.TextProperty(compressed=True, repeated=True)
      t5 = model.TextProperty()
      t6 = model.TextProperty(compressed=True)
      t7 = model.TextProperty(repeated=True)
      t8 = model.TextProperty(compressed=True, repeated=True)
      b1 = model.BlobProperty()
      b2 = model.BlobProperty(compressed=True)
      b3 = model.BlobProperty(repeated=True)
      b4 = model.BlobProperty(compressed=True, repeated=True)

    key1 = model.Key(M, 'test')
    value1 = 'foo bar baz ding'
    value2 = u'f\xd6\xd6 b\xe4r b\xe4z d\xefng'  # Umlauts on the vowels.
    m1 = M(key=key1,
           t1=value1, t2=value1, t3=[value1], t4=[value1],
           t5=value2, t6=value2, t7=[value2], t8=[value2],
           b1=value1, b2=value1, b3=[value1], b4=[value1])
    m1.put()

    # To test compression and deserialization with untouched properties.
    m2 = key1.get()
    m2.put()

    m2 = key1.get()
    self.assertEqual(m2.t1, value1)
    self.assertEqual(m2.t2, value1)
    self.assertEqual(m2.t3, [value1])
    self.assertEqual(m2.t4, [value1])
    self.assertEqual(m2.t5, value2)
    self.assertEqual(m2.t6, value2)
    self.assertEqual(m2.t7, [value2])
    self.assertEqual(m2.t8, [value2])
    self.assertEqual(m2.b1, value1)
    self.assertEqual(m2.b2, value1)
    self.assertEqual(m2.b3, [value1])
    self.assertEqual(m2.b4, [value1])

    # To test compression and deserialization after properties were accessed.
    m2.put()

  def testCompressedProperty_Repr(self):
    class Foo(model.Model):
      name = model.StringProperty()

    class M(model.Model):
      b = model.BlobProperty(compressed=True)
      t = model.TextProperty(compressed=True)
      l = model.LocalStructuredProperty(Foo, compressed=True)
    x = M(b='b' * 100, t=u't' * 100, l=Foo(name='joe'))
    key_id = x.put().id()
    y = x.key.get()
    self.assertFalse(x is y)
    self.assertEqual(
        repr(y),
        'M(key=Key(\'M\', %d), ' % key_id +
        'b=%r, ' % ('b' * 100) +
        'l=%r, ' % Foo(name=u'joe') +
        't=%r)' % (u't' * 100))

  def testCorruption(self):
    # Thanks to Ricardo Banffy
    class Evil(model.Model):
      x = model.IntegerProperty()

      def __init__(self, *a, **k):
        super(Evil, self).__init__(*a, **k)
        self.x = 42
    e = Evil()
    e.x = 50
    pb = e._to_pb()
    y = Evil._from_pb(pb)
    self.assertEqual(y.x, 50)

  def testAllocateIdsHooksCalled(self):
    self.pre_counter = 0
    self.post_counter = 0

    self.size = 25
    self.max = None
    self.parent = key.Key('Foo', 'Bar')

    class HatStand(model.Model):

      @classmethod
      def _pre_allocate_ids_hook(cls, size, max, parent):
        self.pre_counter += 1
        self.assertEqual(size, self.size)
        self.assertEqual(max, self.max)
        self.assertEqual(parent, self.parent)

      @classmethod
      def _post_allocate_ids_hook(cls, size, max, parent, future):
        self.post_counter += 1
        self.assertEqual(size, self.size)
        self.assertEqual(max, self.max)
        self.assertEqual(parent, self.parent)
        low, high = future.get_result()
        self.assertEqual(high - low + 1, self.size)

    self.assertEqual(self.pre_counter, 0, 'Pre allocate ids hook called early')
    future = HatStand.allocate_ids_async(size=self.size, max=self.max,
                                         parent=self.parent)
    self.assertEqual(self.pre_counter, 1, 'Pre allocate ids hook not called')
    self.assertEqual(self.post_counter, 0,
                     'Post allocate ids hook called early')
    future.get_result()
    self.assertEqual(self.post_counter, 1, 'Post allocate ids hook not called')

  def testNoDefaultAllocateIdsCallback(self):
    # See issue 58.  http://goo.gl/hPN6j
    ctx = tasklets.get_context()
    ctx.set_cache_policy(False)

    class EmptyModel(model.Model):
      pass
    fut = EmptyModel.allocate_ids_async(1)
    self.assertFalse(fut._immediate_callbacks,
                     'Allocate ids hook queued default no-op.')

  def testPutHooksCalled(self):
    test = self  # Closure for inside hooks
    self.pre_counter = 0
    self.post_counter = 0

    class HatStand(model.Model):

      def _pre_put_hook(self):
        test.pre_counter += 1

      def _post_put_hook(self, future):
        test.post_counter += 1
        test.assertEqual(future.get_result(), test.entity.key)

    furniture = HatStand()
    self.entity = furniture
    self.assertEqual(self.pre_counter, 0, 'Pre put hook called early')
    future = furniture.put_async()
    self.assertEqual(self.pre_counter, 1, 'Pre put hook not called')
    self.assertEqual(self.post_counter, 0, 'Post put hook called early')
    future.get_result()
    self.assertEqual(self.post_counter, 1, 'Post put hook not called')

    # All counters now read 1, calling put_multi for 10 entities makes this 11
    new_furniture = [HatStand() for _ in range(10)]
    multi_future = model.put_multi_async(new_furniture)
    self.assertEqual(self.pre_counter, 11,
                     'Pre put hooks not called on put_multi')
    self.assertEqual(self.post_counter, 1,
                     'Post put hooks called early on put_multi')
    for fut, ent in zip(multi_future, new_furniture):
      self.entity = ent
      fut.get_result()
    self.assertEqual(self.post_counter, 11,
                     'Post put hooks not called on put_multi')

  def testGetByIdHooksCalled(self):
    # See issue 95.  http://goo.gl/QSRQH
    # Adapted from testGetHooksCalled in key_test.py.
    test = self  # Closure for inside hook
    self.pre_counter = 0
    self.post_counter = 0

    class HatStand(model.Model):

      @classmethod
      def _pre_get_hook(cls, key):
        test.pre_counter += 1
        if test.pre_counter == 1:  # Cannot test for key in get_multi
          self.assertEqual(key, self.key)

      @classmethod
      def _post_get_hook(cls, key, future):
        test.post_counter += 1
        self.assertEqual(key, self.key)
        self.assertEqual(future.get_result(), self.entity)

    furniture = HatStand()
    self.entity = furniture
    key = furniture.put()
    self.key = key
    self.assertEqual(self.pre_counter, 0, 'Pre get hook called early')
    future = HatStand.get_by_id_async(key.id())
    self.assertEqual(self.pre_counter, 1, 'Pre get hook not called')
    self.assertEqual(self.post_counter, 0, 'Post get hook called early')
    future.get_result()
    self.assertEqual(self.post_counter, 1, 'Post get hook not called')

    # All counters now read 1, calling get for 10 keys should make this 11
    new_furniture = [HatStand() for _ in range(10)]
    keys = [furniture.put() for furniture in new_furniture]  # Sequential keys
    multi_future = [HatStand.get_by_id_async(key.id()) for key in keys]
    self.assertEqual(self.pre_counter, 11,
                     'Pre get hooks not called on get_multi')
    self.assertEqual(self.post_counter, 1,
                     'Post get hooks called early on get_multi')
    for fut, key, entity in zip(multi_future, keys, new_furniture):
      self.key = key
      self.entity = entity
      fut.get_result()
    self.assertEqual(self.post_counter, 11,
                     'Post get hooks not called on get_multi')

  def testGetOrInsertHooksCalled(self):
    # See issue 98.  http://goo.gl/7ak2i
    test = self  # Closure for inside hooks

    class HatStand(model.Model):

      @classmethod
      def _pre_get_hook(cls, key):
        test.pre_get_counter += 1

      @classmethod
      def _post_get_hook(cls, key, future):
        test.post_get_counter += 1

      def _pre_put_hook(self):
        test.pre_put_counter += 1

      def _post_put_hook(self, future):
        test.post_put_counter += 1

    # First call creates it.  This calls get() twice (once outside the
    # transaction and once inside it) and put() once (from inside the
    # transaction).
    self.pre_get_counter = 0
    self.post_get_counter = 0
    self.pre_put_counter = 0
    self.post_put_counter = 0
    HatStand.get_or_insert('classic')
    self.assertEqual(self.pre_get_counter, 2)
    self.assertEqual(self.post_get_counter, 2)
    self.assertEqual(self.pre_put_counter, 1)
    self.assertEqual(self.post_put_counter, 1)

    # Second call gets it without needing a transaction.
    self.pre_get_counter = 0
    self.post_get_counter = 0
    self.pre_put_counter = 0
    self.post_put_counter = 0
    HatStand.get_or_insert_async('classic').get_result()
    self.assertEqual(self.pre_get_counter, 1)
    self.assertEqual(self.post_get_counter, 1)
    self.assertEqual(self.pre_put_counter, 0)
    self.assertEqual(self.post_put_counter, 0)

  def testMonkeyPatchHooks(self):
    test = self  # Closure for inside put hooks
    hook_attr_names = ('_pre_allocate_ids_hook', '_post_allocate_ids_hook',
                       '_pre_put_hook', '_post_put_hook')
    original_hooks = {}

    # Backup the original hooks
    for name in hook_attr_names:
      original_hooks[name] = getattr(model.Model, name)

    self.pre_allocate_ids_flag = False
    self.post_allocate_ids_flag = False
    self.pre_put_flag = False
    self.post_put_flag = False

    # TODO: Should the unused arguments to Monkey Patched tests be tested?
    class HatStand(model.Model):

      @classmethod
      def _pre_allocate_ids_hook(cls, unused_size, unused_max, unused_parent):
        self.pre_allocate_ids_flag = True

      @classmethod
      def _post_allocate_ids_hook(cls, unused_size, unused_max, unused_parent,
                                  unused_future):
        self.post_allocate_ids_flag = True

      def _pre_put_hook(self):
        test.pre_put_flag = True

      def _post_put_hook(self, unused_future):
        test.post_put_flag = True

    # Monkey patch the hooks
    for name in hook_attr_names:
      hook = getattr(HatStand, name)
      setattr(model.Model, name, hook)

    try:
      HatStand.allocate_ids(1)
      self.assertTrue(self.pre_allocate_ids_flag,
                      'Pre allocate ids hook not called when model is '
                      'monkey patched')
      self.assertTrue(self.post_allocate_ids_flag,
                      'Post allocate ids hook not called when model is '
                      'monkey patched')
      furniture = HatStand()
      furniture.put()
      self.assertTrue(self.pre_put_flag,
                      'Pre put hook not called when model is monkey patched')
      self.assertTrue(self.post_put_flag,
                      'Post put hook not called when model is monkey patched')
    finally:
      # Restore the original hooks
      for name in hook_attr_names:
        setattr(model.Model, name, original_hooks[name])

  def testPreHooksCannotCancelRPC(self):
    class HatStand(model.Model):

      @classmethod
      def _pre_allocate_ids_hook(cls, unused_size, unused_max, unused_parent):
        raise tasklets.Return()

      def _pre_put_hook(self):
        raise tasklets.Return()
    self.assertRaises(tasklets.Return, HatStand.allocate_ids)
    entity = HatStand()
    self.assertRaises(tasklets.Return, entity.put)

  def testNoDefaultPutCallback(self):
    # See issue 58.  http://goo.gl/hPN6j
    ctx = tasklets.get_context()
    ctx.set_cache_policy(False)

    class EmptyModel(model.Model):
      pass
    entity = EmptyModel()
    fut = entity.put_async()
    self.assertFalse(fut._immediate_callbacks, 'Put hook queued default no-op.')

  def testKeyValidation(self):
    # See issue 75.  http://goo.gl/k0Gfv
    class Foo(model.Model):
      # Override the default Model method with our own.

      def _validate_key(self, key):
        if key.parent() is None:
          raise TypeError
        elif key.parent().kind() != 'Foo':
          raise TypeError
        elif key.id().startswith('a'):
          raise ValueError
        return key

    # Using no arguments
    self.assertRaises(TypeError, Foo().put)

    # Using id/parent arguments
    rogue_parent = model.Key('Bar', 1)
    self.assertRaises(TypeError, Foo, parent=rogue_parent, id='b')
    parent = model.Key(Foo, 1)
    self.assertRaises(ValueError, Foo, parent=parent, id='a')

    # Using key argument
    rogue_key = model.Key(Foo, 1, Foo, 'a')
    self.assertRaises(ValueError, Foo, key=rogue_key)

    # Using Key assignment
    entity = Foo()
    self.assertRaises(ValueError, setattr, entity, 'key', rogue_key)

    # None assignment (including delete) should work correctly
    entity.key = None
    self.assertTrue(entity.key is None)
    del entity.key
    self.assertTrue(entity.key is None)

    # Sanity check a valid key
    key = Foo(parent=parent, id='b').put()
    self.assertEqual(key.parent(), parent)
    self.assertEqual(key.id(), 'b')
    self.assertEqual(key.kind(), 'Foo')

  def testExpandoBlobKey(self):
    class Foo(model.Expando):
      pass
    bk = model.BlobKey('blah')
    foo = Foo(bk=bk)
    foo.put()
    bar = foo.key.get(use_memcache=False, use_cache=False)
    self.assertTrue(isinstance(bar.bk, model.BlobKey))
    self.assertEqual(bar.bk, bk)


class ModelV3Tests(test_utils.NDBTest, BaseModelTestMixin):
  """Model tests that use a connection to a Datastore V3 stub."""

  def setUp(self):
    test_utils.NDBTest.setUp(self)
    BaseModelTestMixin.setUp(self)


@real_unittest.skipUnless(datastore_pbs._CLOUD_DATASTORE_ENABLED,
                          'V1 must be supported to run V1 tests.')
class ModelV1Tests(test_utils.NDBCloudDatastoreV1Test, BaseModelTestMixin):
  """Model tests that use a connection to a Cloud Datastore V1 stub."""

  def setUp(self):
    test_utils.NDBCloudDatastoreV1Test.setUp(self)
    BaseModelTestMixin.setUp(self)

  def testDeleteMultiRepeatedKey(self):
    # TODO(eddavisson): b/21268199.
    pass

  def testKeyPropertyNonUtf8Kind(self):
    # TODO(eddavisson): b/9793610.
    pass

  def testPutMultiRepeatedKey(self):
    # TODO(eddavisson): b/21268199.
    pass

  def testAllocateIds(self):
    # TODO(eddavisson): b/20055469.
    pass

  def testAllocateIdsHooksCalled(self):
    # TODO(eddavisson): b/20055469.
    pass

  def testContextOptions(self):
    # TODO(eddavisson): This test requires memcache.
    pass

  def testContextOptions_PerClass(self):
    # TODO(eddavisson): This test requires memcache.
    pass

  def testContextOptions_ThreeLevels(self):
    # TODO(eddavisson): This test requires memcache.
    pass

  def testContextOptions_Timeouts(self):
    # TODO(eddavisson): This test requires memcache.
    pass

  def testDatePropertyMemcache(self):
    # Memcache not supported in v1.
    pass

  def testDateTimePropertyMemcache(self):
    # Memcache not supported in v1.
    pass

  def testTimePropertyMemcache(self):
    # Memcache not supported in v1.
    pass

  def testMonkeyPatchHooks(self):
    # TODO(eddavisson): This tests allocates IDs. b/20055469.
    pass

  def testNonTransactionalDecorator(self):
    # TODO(eddavisson): This test uses db.
    pass

  def testRejectOldPickles(self):
    # TODO(eddavisson): This test uses db to insert an entity.
    pass


class IndexTests(test_utils.NDBTest):

  def generate_index(self):
    """Run a query to trigger index generation.

    Note, index generation is idempotent. Running same query multiple times does
    not change existing index. Hence we don't need to clear indexes between
    tests.
    """

    # pylint: disable=unused-variable
    class Kind(model.Model):
      property1 = model.TextProperty()
      property2 = model.TextProperty()
    # pylint: enable=unused-variable

    qry = query.gql('SELECT * FROM Kind ORDER BY property1 DESC, property2')
    qry.fetch()

  def testGetIndexes(self):
    self.assertEqual([], model.get_indexes())

    self.generate_index()

    self.assertEqual(
        model.Index(kind='Kind',
                    properties=[
                        model.IndexProperty(name='property1',
                                            direction='desc'),
                        model.IndexProperty(name='property2',
                                            direction='asc'),
                    ],
                    ancestor=False),
        model.get_indexes()[0].definition)

  def testGetIndexesAsync(self):
    fut = model.get_indexes_async()
    self.assertTrue(isinstance(fut, tasklets.Future))
    self.assertEqual([], fut.get_result())

    self.generate_index()

    self.assertEqual(
        model.Index(kind='Kind',
                    properties=[
                        model.IndexProperty(name='property1',
                                            direction='desc'),
                        model.IndexProperty(name='property2',
                                            direction='asc'),
                    ],
                    ancestor=False),
        model.get_indexes_async().get_result()[0].definition)


class CacheTests(test_utils.NDBTest):

  def SetupContextCache(self):
    """Set up the context cache.

    We only need cache active when testing the cache, so the default behavior
    is to disable it to avoid misleading test results. Override this when
    needed.
    """
    ctx = tasklets.make_default_context()
    tasklets.set_context(ctx)
    ctx.set_cache_policy(True)
    ctx.set_memcache_policy(True)

  def testCachedEntityKeyMatchesGetArg(self):
    # See issue 13.  http://goo.gl/jxjOP
    class Employee(model.Model):
      pass

    e = Employee(key=model.Key(Employee, 'joe'))
    e.put()
    e._key = model.Key(Employee, 'fred')

    f = model.Key(Employee, 'joe').get()

    # Now f is e;
    # With bug this is True.
    # self.assertEqual(f.key, model.Key(Employee, 'fred'))

    # Removing key from context cache when it is set to a different one
    # makes the test correct.
    self.assertEqual(f.key, model.Key(Employee, 'joe'))

  def testTransactionalDeleteClearsCache(self):
    # See issue 57.  http://goo.gl/bXkib
    class Employee(model.Model):
      pass
    ctx = tasklets.get_context()
    ctx.set_cache_policy(True)
    ctx.set_memcache_policy(False)
    e = Employee()
    key = e.put()
    key.get()  # Warm the cache

    def trans():
      key.delete()
    model.transaction(trans)
    e = key.get()
    self.assertEqual(e, None)

  def testTransactionalDeleteClearsMemcache(self):
    # See issue 57.  http://goo.gl/bXkib
    class Employee(model.Model):
      pass
    ctx = tasklets.get_context()
    ctx.set_cache_policy(False)
    ctx.set_memcache_policy(True)
    e = Employee()
    key = e.put()
    key.get()  # Warm the cache

    def trans():
      key.delete()
    model.transaction(trans)
    e = key.get()
    self.assertEqual(e, None)

  def testCustomStructuredPropertyInRepeatedStructuredProperty(self):
    class FuzzyDate(object):

      def __init__(self, first, last=None):
        assert isinstance(first, datetime.date)
        assert last is None or isinstance(last, datetime.date)
        self.first = first
        self.last = last or first

      def __eq__(self, other):
        if not isinstance(other, FuzzyDate):
          return NotImplemented
        return self.first == other.first and self.last == other.last

      def __ne__(self, other):
        eq = self.__eq__(other)
        if eq is not NotImplemented:
          eq = not eq
        return eq

      def __repr__(self):
        return 'FuzzyDate(%r, %r)' % (self.first, self.last)

    class FuzzyDateModel(model.Model):
      first = model.DateProperty()
      last = model.DateProperty()

    class FuzzyDateProperty(model.StructuredProperty):

      def __init__(self, **kwds):
        super(FuzzyDateProperty, self).__init__(FuzzyDateModel, **kwds)

      def _validate(self, value):
        assert isinstance(value, FuzzyDate)

      def _to_base_type(self, value):
        return FuzzyDateModel(first=value.first, last=value.last)

      def _from_base_type(self, value):
        return FuzzyDate(value.first, value.last)

    class Inner(model.Model):
      date = FuzzyDateProperty()

    class Outer(model.Model):
      wrap = model.StructuredProperty(Inner, repeated=True)

    d = datetime.date(1900, 1, 1)
    fd = FuzzyDate(d)
    orig = Outer(wrap=[Inner(date=fd), Inner(date=fd)])
    key = orig.put()
    q = Outer.query()
    copy = q.get()
    self.assertEqual(copy, orig)

  def testSubStructureEqualToNone(self):
    class IntRangeModel(model.Model):
      first = model.IntegerProperty()
      last = model.IntegerProperty()

    class Inner(model.Model):
      range = model.StructuredProperty(IntRangeModel)
      other = model.IntegerProperty()

    class Outer(model.Model):
      wrap = model.StructuredProperty(Inner, repeated=True)

    orig = Outer(wrap=[Inner(other=2),
                       Inner(range=IntRangeModel(first=0, last=10), other=4),
                       Inner(other=5)])
    orig.put()
    q = Outer.query()
    copy = q.get()
    self.assertEqual(copy.wrap[0].range, None)
    self.assertEqual(copy.wrap[1].range, IntRangeModel(first=0, last=10))
    self.assertEqual(copy.wrap[2].range, None)

  def testStructuredPropertyFromDict(self):
    # See issue 207.  http://goo.gl/IQXS6
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()

    class AddressList(model.Model):
      addresses = model.StructuredProperty(Address, repeated=True)
      backup = model.StructuredProperty(Address)

    class Person(model.Model):
      name = model.StringProperty()
      home = model.StructuredProperty(Address)
      alist = model.StructuredProperty(AddressList)
      blist = model.LocalStructuredProperty(AddressList)
    joe = Person(name='Joe',
                 home=Address(street='Main', city='Springfield'),
                 alist=AddressList(addresses=[Address(street='A', city='B'),
                                              Address(street='C', city='D')],
                                   backup=Address(street='X', city='Y')),
                 blist=AddressList(addresses=[Address(street='E', city='F')],
                                   backup=None))
    data = joe._to_dict()
    new_joe = Person(**data)
    self.assertEqual(new_joe, joe)

  # See issue 216 for the following testExpando* tests.  http://goo.gl/wNN3g

  def testExpandoInModelFromDict(self):
    class E(model.Expando):
      pass

    class M(model.Model):
      m1 = model.StructuredProperty(E)
    e = E(e1='e1test')
    a = M(m1=e)
    new_a = M(**a.to_dict())
    self.assertEqual(a, new_a)

  def testExpandoInExpandoFromDict(self):
    class A(model.Expando):
      pass
    e = model.Expando(b1='b1test')
    a = A(a1=e)
    new_a = A(**a.to_dict())
    self.assertEqual(a, new_a)

  def testExpandoInExpandoWithoutStructureFromDict(self):
    class A(model.Expando):
      pass
    e = model.Expando(b1='b1test')
    a = A(a1=e)
    a_new = A(c1='c1test')
    a_new.populate(**a.to_dict())
    a.c1 = 'c1test'
    self.assertEqual(a, a_new)

  def testExpandoInExpandoWithStructureFromDict(self):
    class A(model.Expando):
      pass
    e = model.Expando(b1='b1test')
    a = A(a1=e)
    a_new = A(a1=model.Expando())
    a_new.populate(**a.to_dict())
    self.assertEqual(a, a_new)

  def testExpandoInExpandoWithListsFromDict(self):
    class B(model.Expando):
      pass

    class A(model.Expando):
      pass
    bs = [B(b1=[0, 1, 2, 3]), B(b2='b2test')]
    a = A(a1=bs)
    new_a = A(**a.to_dict())
    self.assertEqual(a, new_a)

  def testTransactionsCachingAndQueries(self):
    # Prompted by a doc question and this (outdated) thread:
    # https://groups.google.com/forum/?fromgroups#!topic/appengine-ndb-discuss/idxsAZNHsqI
    class Root(model.Model):
      pass

    class Foo(model.Model):
      name = model.StringProperty()
    root = Root()
    root.put()
    foo1 = Foo(name='foo1', parent=root.key)
    foo1.put()

    @model.transactional
    def txn1():
      foo2 = foo1.key.get()
      assert foo2.name == 'foo1'
      foo2.name = 'foo2'
      foo2.put()
      foo3 = foo1.key.get(use_cache=False)
      foos = Foo.query(ancestor=root.key).fetch(use_cache=False)
      assert foo1 == foo3 == foos[0]
      assert foo1 != foo2
      raise model.Rollback

    @model.transactional
    def txn2():
      foo2 = foo1.key.get()
      assert foo2.name == 'foo1'
      foo2.name = 'foo2'
      foo2.put()
      foo3 = foo1.key.get(use_cache=True)
      foos = Foo.query(ancestor=root.key).fetch(use_cache=True)
      assert foo2 == foo3 == foos[0]
      assert foo1 != foo2
      raise model.Rollback
    txn1()
    txn2()

if __name__ == '__main__':
  unittest.main()
