"""Tests for model.py."""

import base64
import datetime
import difflib
import pickle
import re
import unittest

from google.appengine.api import datastore_errors
from google.appengine.api import datastore_types
from google.appengine.api import memcache
from google.appengine.api import namespace_manager
from google.appengine.api import users
from google.appengine.datastore import entity_pb

from ndb import model, query, tasklets, test_utils

TESTUSER = users.User('test@example.com', 'example.com', '123')
AMSTERDAM = model.GeoPt(52.35, 4.9166667)

GOLDEN_PB = """\
key <
  app: "_"
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
      app: "_"
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
  app: "_"
  path <
    Element {
      type: "MyModel"
      id: 0
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
  app: "_"
  path <
    Element {
      type: "Person"
      id: 0
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
  app: "_"
  path <
    Element {
      type: "Person"
      id: 0
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
  app: "_"
  path <
    Element {
      type: "Tree"
      id: 0
    }
  >
>
entity_group <
>
raw_property <
  meaning: 15
  name: "root.left.left.name"
  value <
    stringValue: "a1a"
  >
  multiple: false
>
raw_property <
  meaning: 15
  name: "root.left.name"
  value <
    stringValue: "a1"
  >
  multiple: false
>
raw_property <
  meaning: 15
  name: "root.left.rite.name"
  value <
    stringValue: "a1b"
  >
  multiple: false
>
raw_property <
  meaning: 15
  name: "root.name"
  value <
    stringValue: "a"
  >
  multiple: false
>
raw_property <
  meaning: 15
  name: "root.rite.name"
  value <
    stringValue: "a2"
  >
  multiple: false
>
raw_property <
  meaning: 15
  name: "root.rite.rite.name"
  value <
    stringValue: "a2b"
  >
  multiple: false
>
"""

MULTI_PB = """\
key <
  app: "_"
  path <
    Element {
      type: "Person"
      id: 0
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
  app: "_"
  path <
    Element {
      type: "Person"
      id: 0
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
  app: "_"
  path <
    Element {
      type: "Person"
      id: 0
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

class ModelTests(test_utils.DatastoreTest):

  def tearDown(self):
    self.assertTrue(model.Model._properties == {})
    self.assertTrue(model.Expando._properties == {})
    super(ModelTests, self).tearDown()

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

  def testRepeatedRequiredDefaultConflict(self):
    # Allow at most one of repeated=True, required=True, default=<non-None>.
    class MyModel(model.Model):
      self.assertRaises(Exception,
                        model.StringProperty, repeated=True, default='')
      self.assertRaises(Exception,
                        model.StringProperty, repeated=True, required=True)
      self.assertRaises(Exception,
                        model.StringProperty, required=True, default='')
      self.assertRaises(Exception,
                        model.StringProperty,
                        repeated=True, required=True, default='')

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
    m = MyModel()
    m.a = 'ABC'
    self.assertEqual(m.a, 'abc')
    self.assertRaises(datastore_errors.BadValueError,
                      setattr, m, 'a', 'def')

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

  def DateAndOrTimePropertyTest(self, propclass, t1, t2):
    class Person(model.Model):
      name = model.StringProperty()
      ctime = propclass(auto_now_add=True)
      mtime = propclass(auto_now=True)
      atime = propclass()
      times =  propclass(repeated=True)

    p = Person()
    p.atime = t1
    p.times = [t1, t2]
    self.assertEqual(p.ctime, None)
    self.assertEqual(p.mtime, None)
    pb = p._to_pb()
    self.assertNotEqual(p.ctime, None)
    self.assertNotEqual(p.mtime, None)
    q = Person._from_pb(pb)
    self.assertEqual(q.ctime, p.ctime)
    self.assertEqual(q.mtime, p.mtime)
    self.assertEqual(q.atime, t1)
    self.assertEqual(q.times, [t1, t2])

  def testDateTimeProperty(self):
    self.DateAndOrTimePropertyTest(model.DateTimeProperty,
                                   datetime.datetime(1982, 12, 1, 9, 0, 0),
                                   datetime.datetime(1995, 4, 15, 5, 0, 0))

  def testDateProperty(self):
    self.DateAndOrTimePropertyTest(model.DateProperty,
                                   datetime.date(1982, 12, 1),
                                   datetime.date(1995, 4, 15))

  def testTimeProperty(self):
    self.DateAndOrTimePropertyTest(model.TimeProperty,
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

  def testRecursiveStructuredProperty(self):
    class Node(model.Model):
      name = model.StringProperty(indexed=False)
    Node.left = model.StructuredProperty(Node)
    Node.rite = model.StructuredProperty(Node)
    Node._fix_up_properties()
    class Tree(model.Model):
      root = model.StructuredProperty(Node)

    k = model.Key(flat=['Tree', None])
    tree = Tree()
    tree.key = k
    tree.root = Node(name='a',
                     left=Node(name='a1',
                               left=Node(name='a1a'),
                               rite=Node(name='a1b')),
                     rite=Node(name='a2',
                               rite=Node(name='a2b')))
    pb = tree._to_pb()
    self.assertEqual(str(pb), RECURSIVE_PB)

    tree2 = Tree._from_pb(pb)
    self.assertEqual(tree2, tree)

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
    self.assertEqual(model.Model._get_kind_map(), {'A1': A1})
    class A2(model.Model):
      pass
    self.assertEqual(model.Model._get_kind_map(), {'A1': A1, 'A2': A2})

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

  def testMultipleStructuredProperty(self):
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
      innerval = model.StringProperty(repeated=True)
    self.assertRaises(AssertionError,
                      model.StructuredProperty, Inner, repeated=True)

  def testNullProperties(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()
      zip = model.IntegerProperty()
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
      zip = model.IntegerProperty()
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
                               zip=20500))
    p.key = k
    pb = p._to_pb()
    q = model.Model._from_pb(pb)
    qb = q._to_pb()
    linesp = str(pb).splitlines(True)
    linesq = str(qb).splitlines(True)
    lines = difflib.unified_diff(linesp, linesq, 'Expected', 'Actual')
    self.assertEqual(pb, qb, ''.join(lines))

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
    from google.appengine.ext import db
    class MyModel(db.Model):
      name = db.StringProperty()
    dumped = []
    for proto in 0, 1, 2:
      x = MyModel()
      s = pickle.dumps(x)
      dumped.append(s)
      x.name = 'joe'
      s = pickle.dumps(x)
      dumped.append(s)
      db.put(x)
      s = pickle.dumps(x)
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

  def testModelRepr_RenamedProperty(self):
    class Address(model.Model):
      street = model.StringProperty('Street')
      city = model.StringProperty('City')
    a = Address(street='345 Spear', city='SF')
    self.assertEqual(repr(a), "Address(city='SF', street='345 Spear')")

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
      compp = model.ComputedProperty(lambda e: 'x')
    self.assertEqual(repr(MyModel.key), "ModelKey('__key__')")
    for name, prop in MyModel._properties.iteritems():
      s = repr(prop)
      self.assertTrue(s.startswith(prop.__class__.__name__ + '('), s)


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
    a.s = '\xff'  # Not UTF-8.
    self.assertRaises(BVE, setattr, a, 's', 0)

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
    a.t = '\xff'  # Not UTF-8.
    self.assertRaises(BVE, setattr, a, 't', 0)

    a.b = None
    a.b = 'abc'
    a.b = '\xff'
    self.assertRaises(BVE, setattr, a, 'b', u'')
    self.assertRaises(BVE, setattr, a, 'b', u'')

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
    # TODO: Validate pb

    # Check we can enable and disable compression and have old data still
    # be understood.
    Person.address._compressed = True
    p = Person._from_pb(pb)
    self.assertEqual(p.address.key, None)
    self.assertEqual(p.name, 'Google')
    self.assertEqual(p.address.street, '1600 Amphitheatre')
    self.assertEqual(p.address.city, 'Mountain View')
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
    p.city= 'SF'
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
    b.bar=['a', 'b']
    self.assertFalse(b._properties['foo']._indexed)
    self.assertFalse(b._properties['bar']._indexed)

  def testComputedProperty(self):
    class ComputedTest(model.Model):
      name = model.StringProperty()
      name_lower = model.ComputedProperty(lambda self: self.name.lower())

      @model.ComputedProperty
      def length(self):
        return len(self.name)

      def _compute_hash(self):
        return hash(self.name)
      hash = model.ComputedProperty(_compute_hash, name='hashcode')

    m = ComputedTest(name='Foobar')
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
    self.assertEqual(m.hash, hash('Foobar'))

  def testLargeValues(self):
    class Demo(model.Model):
      bytes = model.BlobProperty()
      text = model.TextProperty()
    x = Demo(bytes='x'*1000, text=u'a'*1000)
    key = x.put()
    y = key.get()
    self.assertEqual(x, y)
    self.assertTrue(isinstance(y.bytes, str))
    self.assertTrue(isinstance(y.text, unicode))

  def testMultipleStructuredProperty(self):
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

  def testGetById(self):
    class MyModel(model.Model):
      pass

    kind = MyModel._get_kind()

    # key id
    ent1 = MyModel(key=model.Key(pairs=[(kind, 1)]))
    key = ent1.put()
    res = MyModel.get_by_id(1)
    self.assertEqual(res, ent1)

    # key name
    ent2 = MyModel(key=model.Key(pairs=[(kind, 'foo')]))
    key = ent2.put()
    res = MyModel.get_by_id('foo')
    self.assertEqual(res, ent2)

    # key id + parent
    ent3 = MyModel(key=model.Key(pairs=[(kind, 1), (kind, 2)]))
    key = ent3.put()
    res = MyModel.get_by_id(2, parent=model.Key(pairs=[(kind, 1)]))
    self.assertEqual(res, ent3)

    # key name + parent
    ent4 = MyModel(key=model.Key(pairs=[(kind, 1), (kind, 'bar')]))
    key = ent4.put()
    res = MyModel.get_by_id('bar', parent=ent1.key)
    self.assertEqual(res, ent4)

    # None
    res = MyModel.get_by_id('idontexist')
    self.assertEqual(res, None)

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
    c = model.transaction(callback, retry=0, entity_group=key)
    self.assertNotEqual(c, None)
    self.assertEqual(c.text, 'baz')
    self.assertEqual(key.get(), c)

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

    res = foo().get_result()
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

    res = model.delete_multi((key1, key2, key3))

    self.assertEqual(key1.get(), None)
    self.assertEqual(key2.get(), None)
    self.assertEqual(key3.get(), None)

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
    ent.put()
    key.get(use_cache=False)  # Write to memcache.
    # Verify that it is in both caches.
    self.assertTrue(ctx._cache[key] is ent)
    self.assertEqual(memcache.get(ctx._memcache_prefix + key.urlsafe()),
                     ent._to_pb())
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
    # Get it from the datastore.
    ent4 = key.get(use_cache=False, use_memcache=False)
    self.assertFalse(ent4 is ent)
    self.assertFalse(ent4 is ent2)
    self.assertFalse(ent4 is ent3)
    self.assertFalse(ent4 is ent_copy)
    self.assertEqual(ent4.name, 'yoyo')
    # Delete it from the datastore but leave it in the caches.
    key.delete(use_cache=False, use_memcache=False)
    # Assure it is gone from the datastore.
    [ent5] = model.get_multi([key],
                             use_cache=False, use_memcache=False)
    self.assertEqual(ent5, None)
    # Assure it is still in memcache.
    ent6 = key.get(use_cache=False)
    self.assertEqual(ent6.name, 'yo')
    self.assertEqual(memcache.get(ctx._memcache_prefix + key.urlsafe()),
                     ent._to_pb())
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
    # Mock memcache.add_multi().
    save_memcache_add_multi = memcache.add_multi
    memcache_args_log = []
    def mock_memcache_add_multi(*args, **kwds):
      memcache_args_log.append((args, kwds))
      return save_memcache_add_multi(*args, **kwds)
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
    # Test that the timeouts make it through to memcache and the datastore.
    try:
      memcache.add_multi = mock_memcache_add_multi
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
    finally:
      memcache.add_multi = save_memcache_add_multi
      ctx._conn.async_put = save_conn_async_put
    self.assertEqual([e1.key, e2.key, e3.key, e4.key, e5.key],
                     [x1, x2, x3, x4, x5])
    self.assertEqual(len(memcache_args_log), 3)
    timeouts = set(kwds['time'] for args, kwds in memcache_args_log)
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
    self.assertEqual(memcache.get('NDB:' + k.urlsafe()), b._to_pb())
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
    self.assertEqual(memcache.get('NDB:' + a.key.urlsafe()), a._to_pb())
    self.assertEqual(memcache.get('NDB:' + b.key.urlsafe()), None)
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
      # key, overriden
      key = model.StringProperty()
      # aha, here it is!
      real_key = model.ModelKey()

    class MyExpando(model.Expando):
      # key, overriden
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

      def _serialize_value(self, value):
        return value.__repr__()

      def _deserialize_value(self, value):
        return eval(value)

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
    x = M(b='b'*100, t=u't'*100, l=Foo(name='joe'))
    x.put()
    y = x.key.get()
    self.assertFalse(x is y)
    self.assertEqual(
      repr(y),
      'M(key=Key(\'M\', 1), '
      'b=_CompressedValue(\'x\\x9cKJ\\xa2=\\x00\\x00\\x8e\\x01&I\'), '
      'l=_CompressedValue(\'x\\x9c+\\xe2\\x97b\\xc9K\\xccMU`\\xd0b'
      '\\x95b\\xce\\xcaO\\x05\\x00"\\x87\\x03\\xeb\'), '
      't=_CompressedValue(\'x\\x9c+)\\xa1=\\x00\\x00\\xf1$-Q\'))')

class CacheTests(test_utils.DatastoreTest):
  def SetupContextCache(self):
    """Set up the context cache.

    We only need cache active when testing the cache, so the default behavior
    is to disable it to avoid misleading test results. Override this when
    needed.
    """
    from ndb import tasklets
    ctx = tasklets.get_context()
    ctx.set_cache_policy(lambda key: True)
    ctx.set_memcache_policy(lambda key: True)

  def test_issue_13(self):
    class Employee(model.Model):
      pass

    e = Employee(key=model.Key(Employee, 'joe'))
    e.put()
    e.key = model.Key(Employee, 'fred')

    f = model.Key(Employee, 'joe').get()

    # Now f is e;
    # With bug this is True.
    # self.assertEqual(f.key, model.Key(Employee, 'fred'))

    # Removing key from context cache when it is set to a different one
    # makes the test correct.
    self.assertEqual(f.key, model.Key(Employee, 'joe'))


def main():
  unittest.main()

if __name__ == '__main__':
  main()
