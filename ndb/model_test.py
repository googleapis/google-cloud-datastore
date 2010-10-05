"""Tests for model.py."""

import base64
import pickle
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


def main():
  unittest.main()

if __name__ == '__main__':
  main()
