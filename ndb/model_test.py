"""Tests for model.py."""

import base64
import pickle
import re
import unittest

from google.appengine.datastore import entity_pb

from ndb import key, model

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

class ModelTests(unittest.TestCase):

  def testProperties(self):
    m = model.Model()
    self.assertEqual(m.propnames(), [])
    self.assertEqual(m.getvalue('p'), None)
    m.setvalue('p', 42)
    self.assertEqual(m.getvalue('p'), 42)
    self.assertEqual(m.propnames(), ['p'])
    m.delvalue('p')
    self.assertEqual(m.propnames(), [])
    self.assertEqual(m.getvalue('p'), None)

  def testKey(self):
    m = model.Model()
    self.assertEqual(m.key, None)
    k = key.Key(flat=['ParentModel', 42, 'Model', 'foobar'])
    m.key = k
    self.assertEqual(m.key, k)
    del m.key
    self.assertEqual(m.key, None)

  def testSerialize(self):
    m = model.Model()
    k = key.Key(flat=['Model', 42])
    m.key = k
    m.setvalue('p', 42)
    m.setvalue('q', 'hello')
    m.setvalue('k', key.Key(flat=['Model', 42]))
    pb = m.ToPb()
    self.assertEqual(str(pb), GOLDEN_PB)
    m2 = model.Model()
    m2.FromPb(pb)
    self.assertEqual(str(m2.ToPb()), GOLDEN_PB)

  def testIncompleteKey(self):
    m = model.Model()
    k = key.Key(flat=['Model', None])
    m.key = k
    m.setvalue('p', 42)
    pb = m.ToPb()
    m2 = model.Model()
    m2.FromPb(pb)
    self.assertEqual(m2, m)

  def testNewProperties(self):
    class MyModel(model.Model):
      p = model.IntegerProperty()
      q = model.StringProperty()
      k = model.KeyProperty()
    model.FixUpProperties(MyModel)
    ent = MyModel()
    k = model.Key(flat=['MyModel', 42])
    ent.key = k
    ent.p.SetValue(ent, 42)
    ent.q.SetValue(ent, 'hello')
    ent.k.SetValue(ent, k)
    self.assertEqual(ent.p.GetValue(ent), 42)
    self.assertEqual(ent.q.GetValue(ent), 'hello')
    pb = model.conn.adapter.entity_to_pb(ent)
    self.assertEqual(str(pb), re.sub('Model', 'MyModel', GOLDEN_PB))

  def testUnindexedProperties(self):
    class MyModel(model.Model):
      t = model.TextProperty()
      b = model.BlobProperty()
    model.FixUpProperties(MyModel)
    ent = MyModel()
    ent.t.SetValue(ent, u'Hello world\u1234')
    ent.b.SetValue(ent, '\x00\xff')
    self.assertEqual(ent.t.GetValue(ent), u'Hello world\u1234')
    self.assertEqual(ent.b.GetValue(ent), '\x00\xff')
    pb = ent.ToPb()
    self.assertEqual(str(pb), UNINDEXED_PB)


def main():
  unittest.main()

if __name__ == '__main__':
  main()
