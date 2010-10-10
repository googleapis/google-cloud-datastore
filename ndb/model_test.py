"""Tests for model.py."""

import base64
import datetime
import difflib
import pickle
import re
import unittest

from google.appengine.datastore import entity_pb

from ndb import model

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
  name: "b"
  value <
    stringValue: "\\000\\377"
  >
  multiple: false
>
raw_property <
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
  name: "root.left.left.name"
  value <
    stringValue: "a1a"
  >
  multiple: false
>
raw_property <
  name: "root.left.name"
  value <
    stringValue: "a1"
  >
  multiple: false
>
raw_property <
  name: "root.left.rite.name"
  value <
    stringValue: "a1b"
  >
  multiple: false
>
raw_property <
  name: "root.name"
  value <
    stringValue: "a"
  >
  multiple: false
>
raw_property <
  name: "root.rite.name"
  value <
    stringValue: "a2"
  >
  multiple: false
>
raw_property <
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

# NOTE: When a structured property is repeated its fields are not marked so.
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
  multiple: false
>
property <
  name: "address.text"
  value <
    stringValue: "San Francisco"
  >
  multiple: false
>
property <
  name: "address.label"
  value <
    stringValue: "home"
  >
  multiple: false
>
property <
  name: "address.text"
  value <
    stringValue: "Mountain View"
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

class ModelTests(unittest.TestCase):

  def setUp(self):
    model.kind_map.clear()

  def tearDown(self):
    model.kind_map.clear()

  def testKey(self):
    m = model.Model()
    self.assertEqual(m.key, None)
    k = model.Key(flat=['ParentModel', 42, 'Model', 'foobar'])
    m.key = k
    self.assertEqual(m.key, k)
    del m.key
    self.assertEqual(m.key, None)

  def testIncompleteKey(self):
    m = model.Model()
    k = model.Key(flat=['Model', None])
    m.key = k
    pb = m.ToPb()
    m2 = model.Model()
    m2.FromPb(pb)
    self.assertEqual(m2, m)

  def testProperty(self):
    class MyModel(model.Model):
      p = model.IntegerProperty()
      q = model.StringProperty()
      k = model.KeyProperty()
    model.FixUpProperties(MyModel)

    ent = MyModel()
    k = model.Key(flat=['MyModel', 42])
    ent.key = k
    MyModel.p.SetValue(ent, 42)
    MyModel.q.SetValue(ent, 'hello')
    MyModel.k.SetValue(ent, k)
    self.assertEqual(MyModel.p.GetValue(ent), 42)
    self.assertEqual(MyModel.q.GetValue(ent), 'hello')
    self.assertEqual(MyModel.k.GetValue(ent), k)
    pb = model.conn.adapter.entity_to_pb(ent)
    self.assertEqual(str(pb), INDEXED_PB)

    ent = MyModel()
    ent.FromPb(pb)
    self.assertEqual(ent.getkind(), 'MyModel')
    k = model.Key(flat=['MyModel', 42])
    self.assertEqual(ent.key, k)
    self.assertEqual(MyModel.p.GetValue(ent), 42)
    self.assertEqual(MyModel.q.GetValue(ent), 'hello')
    self.assertEqual(MyModel.k.GetValue(ent), k)

  def testUnindexedProperty(self):
    class MyModel(model.Model):
      t = model.TextProperty()
      b = model.BlobProperty()
    model.FixUpProperties(MyModel)

    ent = MyModel()
    MyModel.t.SetValue(ent, u'Hello world\u1234')
    MyModel.b.SetValue(ent, '\x00\xff')
    self.assertEqual(MyModel.t.GetValue(ent), u'Hello world\u1234')
    self.assertEqual(MyModel.b.GetValue(ent), '\x00\xff')
    pb = ent.ToPb()
    self.assertEqual(str(pb), UNINDEXED_PB)

    ent = MyModel()
    ent.FromPb(pb)
    self.assertEqual(ent.getkind(), 'MyModel')
    k = model.Key(flat=['MyModel', None])
    self.assertEqual(ent.key, k)
    self.assertEqual(MyModel.t.GetValue(ent), u'Hello world\u1234')
    self.assertEqual(MyModel.b.GetValue(ent), '\x00\xff')

  def testStructuredProperty(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()
    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(Address)
    model.FixUpProperties(Person)

    p = Person()
    p.name = 'Google'
    a = Address(street='1600 Amphitheatre')
    p.address = a
    p.address.city = 'Mountain View'
    self.assertEqual(Person.name.GetValue(p), 'Google')
    self.assertEqual(p.name, 'Google')
    self.assertEqual(Person.address.GetValue(p), a)
    self.assertEqual(Address.street.GetValue(a), '1600 Amphitheatre')
    self.assertEqual(Address.city.GetValue(a), 'Mountain View')

    pb = p.ToPb()
    self.assertEqual(str(pb), PERSON_PB)

    p = Person()
    p.FromPb(pb)
    self.assertEqual(p.name, 'Google')
    self.assertEqual(p.address.street, '1600 Amphitheatre')
    self.assertEqual(p.address.city, 'Mountain View')
    self.assertEqual(p.address, a)

  def testNestedStructuredProperty(self):
    class Address(model.Model):
      street = model.StringProperty()
      city = model.StringProperty()
    model.FixUpProperties(Address)
    class AddressPair(model.Model):
      home = model.StructuredProperty(Address)
      work = model.StructuredProperty(Address)
    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(AddressPair)
    model.FixUpProperties(Person)

    p = Person()
    p.name = 'Google'
    p.address = AddressPair(home=Address(), work=Address())
    p.address.home.city = 'Mountain View'
    p.address.home.street = '1600 Amphitheatre'
    p.address.work.city = 'San Francisco'
    p.address.work.street = '345 Spear'
    pb = p.ToPb()
    self.assertEqual(str(pb), NESTED_PB)

    p = Person()
    p.FromPb(pb)
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
    model.FixUpProperties(Node)
    class Tree(model.Model):
      root = model.StructuredProperty(Node)
    model.FixUpProperties(Tree)

    k = model.Key(flat=['Tree', None])
    tree = Tree()
    tree.key = k
    tree.root = Node(name='a',
                     left=Node(name='a1',
                               left=Node(name='a1a'),
                               rite=Node(name='a1b')),
                     rite=Node(name='a2',
                               rite=Node(name='a2b')))
    pb = tree.ToPb()
    self.assertEqual(str(pb), RECURSIVE_PB)

    tree2 = Tree()
    tree2.FromPb(pb)
    self.assertEqual(tree2, tree)

  def testRenamedProperty(self):
    class MyModel(model.Model):
      pp = model.IntegerProperty('p')
      qq = model.StringProperty('q')
      kk = model.KeyProperty('k')
    model.FixUpProperties(MyModel)

    ent = MyModel()
    k = model.Key(flat=['MyModel', 42])
    ent.key = k
    MyModel.pp.SetValue(ent, 42)
    MyModel.qq.SetValue(ent, 'hello')
    MyModel.kk.SetValue(ent, k)
    self.assertEqual(MyModel.pp.GetValue(ent), 42)
    self.assertEqual(MyModel.qq.GetValue(ent), 'hello')
    self.assertEqual(MyModel.kk.GetValue(ent), k)
    pb = model.conn.adapter.entity_to_pb(ent)
    self.assertEqual(str(pb), INDEXED_PB)

    ent = MyModel()
    ent.FromPb(pb)
    self.assertEqual(ent.getkind(), 'MyModel')
    k = model.Key(flat=['MyModel', 42])
    self.assertEqual(ent.key, k)
    self.assertEqual(MyModel.pp.GetValue(ent), 42)
    self.assertEqual(MyModel.qq.GetValue(ent), 'hello')
    self.assertEqual(MyModel.kk.GetValue(ent), k)

  def testRenamedStructuredProperty(self):
    class Address(model.Model):
      st = model.StringProperty('street')
      ci = model.StringProperty('city')
    model.FixUpProperties(Address)
    class AddressPair(model.Model):
      ho = model.StructuredProperty(Address, 'home')
      wo = model.StructuredProperty(Address, 'work')
    class Person(model.Model):
      na = model.StringProperty('name')
      ad = model.StructuredProperty(AddressPair, 'address')
    model.FixUpProperties(Person)

    p = Person()
    p.na = 'Google'
    p.ad = AddressPair(ho=Address(), wo=Address())
    p.ad.ho.ci = 'Mountain View'
    p.ad.ho.st = '1600 Amphitheatre'
    p.ad.wo.ci = 'San Francisco'
    p.ad.wo.st = '345 Spear'
    pb = p.ToPb()
    self.assertEqual(str(pb), NESTED_PB)

    p = Person()
    p.FromPb(pb)
    self.assertEqual(p.na, 'Google')
    self.assertEqual(p.ad.ho.st, '1600 Amphitheatre')
    self.assertEqual(p.ad.ho.ci, 'Mountain View')
    self.assertEqual(p.ad.wo.st, '345 Spear')
    self.assertEqual(p.ad.wo.ci, 'San Francisco')

  def testKindMap(self):
    model.kind_map.clear()
    class A1(model.Model):
      pass
    class A2(model.Model):
      pass
    model.FixUpProperties(A1)
    self.assertEqual(model.kind_map, {'A1': A1})
    model.FixUpProperties(A2)
    self.assertEqual(model.kind_map, {'A1': A1, 'A2': A2})

  def testMultipleProperty(self):
    class Person(model.Model):
      name = model.StringProperty()
      address = model.StringProperty(repeated=True)
    model.FixUpProperties(Person)

    m = Person(name='Google', address=['345 Spear', 'San Francisco'])
    m.key = model.Key(flat=['Person', None])
    self.assertEqual(m.address, ['345 Spear', 'San Francisco'])
    pb = m.ToPb()
    self.assertEqual(str(pb), MULTI_PB)

    m2 = Person()
    m2.FromPb(pb)
    self.assertEqual(m2, m)

  def testMultipleInStructuredProperty(self):
    class Address(model.Model):
      label = model.StringProperty()
      line = model.StringProperty(repeated=True)
    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(Address)
    model.FixUpProperties(Person)

    m = Person(name='Google',
               address=Address(label='work',
                               line=['345 Spear', 'San Francisco']))
    m.key = model.Key(flat=['Person', None])
    self.assertEqual(m.address.line, ['345 Spear', 'San Francisco'])
    pb = m.ToPb()
    self.assertEqual(str(pb), MULTIINSTRUCT_PB)

    m2 = Person()
    m2.FromPb(pb)
    self.assertEqual(m2, m)

  def testMultipleStructuredProperty(self):
    class Address(model.Model):
      label = model.StringProperty()
      text = model.StringProperty()
    class Person(model.Model):
      name = model.StringProperty()
      address = model.StructuredProperty(Address, repeated=True)
    model.FixUpProperties(Person)

    m = Person(name='Google',
               address=[Address(label='work', text='San Francisco'),
                        Address(label='home', text='Mountain View')])
    m.key = model.Key(flat=['Person', None])
    self.assertEqual(m.address[0].label, 'work')
    self.assertEqual(m.address[0].text, 'San Francisco')
    self.assertEqual(m.address[1].label, 'home')
    self.assertEqual(m.address[1].text, 'Mountain View')
    pb = m.ToPb()
    self.assertEqual(str(pb), MULTISTRUCT_PB)

    m2 = Person()
    m2.FromPb(pb)
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
    model.FixUpProperties(Address)
    class Person(model.Model):
      address = model.StructuredProperty(Address)
      age = model.IntegerProperty()
      name = model.StringProperty()
      k = model.KeyProperty()
    model.FixUpProperties(Person)
    k = model.Key(flat=['Person', 42])
    p = Person()
    p.key = k
    self.assertEqual(p.address, None)
    self.assertEqual(p.age, None)
    self.assertEqual(p.name, None)
    self.assertEqual(p.k, None)
    pb = p.ToPb()
    q = Person()
    q.FromPb(pb)
    self.assertEqual(q.address, None)
    self.assertEqual(q.age, None)
    self.assertEqual(q.name, None)
    self.assertEqual(q.k, None)
    self.assertEqual(q, p)

  def testOrphanProperties(self):
    class Tag(model.Model):
      names = model.StringProperty(repeated=True)
      ratings = model.IntegerProperty(repeated=True)
    model.FixUpProperties(Tag)
    class Address(model.Model):
      line = model.StringProperty(repeated=True)
      city = model.StringProperty()
      zip = model.IntegerProperty()
      tags = model.StructuredProperty(Tag)
    model.FixUpProperties(Address)
    class Person(model.Model):
      address = model.StructuredProperty(Address)
      age = model.IntegerProperty(repeated=True)
      name = model.StringProperty()
      k = model.KeyProperty()
    model.FixUpProperties(Person)
    k = model.Key(flat=['Person', 42])
    p = Person(name='White House', k=k, age=[210, 211],
               address=Address(line=['1600 Pennsylvania', 'Washington, DC'],
                               tags=Tag(names=['a', 'b'], ratings=[1, 2]),
                               zip=20500))
    p.key = k
    pb = p.ToPb()
    q = model.Model()
    q.FromPb(pb)
    qb = q.ToPb()
    linesp = str(pb).splitlines(True)
    linesq = str(qb).splitlines(True)
    lines = difflib.unified_diff(linesp, linesq, 'Expected', 'Actual')
    self.assertEqual(pb, qb, ''.join(lines))

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
      "Person(key=Key(pairs=[('Person', 42)]), "
      "address=Address(city='SF', street='345 Spear'), name='Google')")

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

  def testEmptyList(self):
    class Person(model.Model):
      name = model.StringProperty(repeated=True)
    p = Person()
    self.assertEqual(p.name, [])
    pb = p.ToPb()
    q = Person()
    q.FromPb(pb)
    self.assertEqual(q.name, [], str(pb))

  def testEmptyListSerialized(self):
    class Person(model.Model):
      name = model.StringProperty(repeated=True)
    p = Person()
    pb = p.ToPb()
    q = Person()
    q.FromPb(pb)
    self.assertEqual(q.name, [], str(pb))

  def testDatetimeSerializing(self):
    class Person(model.Model):
      t = model.GenericProperty()
    p = Person(t=datetime.datetime.utcnow())
    pb = p.ToPb()
    q = Person()
    q.FromPb(pb)
    self.assertEqual(p.t, q.t)

def main():
  unittest.main()

if __name__ == '__main__':
  main()
