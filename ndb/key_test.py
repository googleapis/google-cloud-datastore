"""Tests for key.py."""

import base64
import pickle
import unittest

from google.appengine.datastore import entity_pb

from ndb import key

class KeyTests(unittest.TestCase):

  def testShort(self):
    k0 = key.Key('Kind', None)
    self.assertEqual(k0.flat(), ['Kind', 0])
    k1 = key.Key('Kind', 1)
    self.assertEqual(k1.flat(), ['Kind', 1])
    k2 = key.Key('Parent', 42, 'Kind', 1)
    self.assertEqual(k2.flat(), ['Parent', 42, 'Kind', 1])

  def testFlat(self):
    flat = ['Kind', 1]
    pairs = [(flat[i], flat[i+1]) for i in xrange(0, len(flat), 2)]
    k = key.Key(flat=flat)
    self.assertEqual(k.pairs(), pairs)
    self.assertEqual(k.flat(), flat)
    self.assertEqual(k.kind(), 'Kind')

  def testFlatLong(self):
    flat = ['Kind', 1, 'Subkind', 'foobar']
    pairs = [(flat[i], flat[i+1]) for i in xrange(0, len(flat), 2)]
    k = key.Key(flat=flat)
    self.assertEqual(k.pairs(), pairs)
    self.assertEqual(k.flat(), flat)
    self.assertEqual(k.kind(), 'Subkind')

  def testSerialized(self):
    flat = ['Kind', 1, 'Subkind', 'foobar']
    r = entity_pb.Reference()
    r.set_app('_')
    e = r.mutable_path().add_element()
    e.set_type(flat[0])
    e.set_id(flat[1])
    e = r.mutable_path().add_element()
    e.set_type(flat[2])
    e.set_name(flat[3])
    serialized = r.Encode()
    urlsafe = base64.urlsafe_b64encode(r.Encode()).rstrip('=')

    k = key.Key(flat=flat)
    self.assertEqual(k.serialized(), serialized)
    self.assertEqual(k.urlsafe(), urlsafe)
    self.assertEqual(k.reference(), r)

    k = key.Key(urlsafe=urlsafe)
    self.assertEqual(k.serialized(), serialized)
    self.assertEqual(k.urlsafe(), urlsafe)
    self.assertEqual(k.reference(), r)

    k = key.Key(serialized=serialized)
    self.assertEqual(k.serialized(), serialized)
    self.assertEqual(k.urlsafe(), urlsafe)
    self.assertEqual(k.reference(), r)

    k = key.Key(reference=r)
    self.assertTrue(k._reference() is not r)
    self.assertEqual(k.serialized(), serialized)
    self.assertEqual(k.urlsafe(), urlsafe)
    self.assertEqual(k.reference(), r)

  def testRepr(self):
    k = key.Key('Kind', 1L, 'Subkind', 'foobar')
    self.assertEqual(repr(k),
                     "Key('Kind', 1, 'Subkind', 'foobar')")
    self.assertEqual(repr(k), str(k))
    k = key.Key('Kind', 1)
    self.assertEqual(repr(k), "Key('Kind', 1)")

  def testUnicode(self):
    flat_input = [u'Kind\u1234', 1, 'Subkind', u'foobar\u4321']
    flat = [flat_input[0].encode('utf8'), flat_input[1],
            flat_input[2], flat_input[3].encode('utf8')]
    pairs = [(flat[i], flat[i+1]) for i in xrange(0, len(flat), 2)]
    k = key.Key(flat=flat_input)
    self.assertEqual(k.pairs(), pairs)
    self.assertEqual(k.flat(), flat)
    # TODO: test these more thoroughly
    r = k.reference()
    serialized = k.serialized()
    urlsafe = k.urlsafe()
    key.Key(urlsafe=urlsafe.decode('utf8'))
    key.Key(serialized=serialized.decode('utf8'))
    key.Key(reference=r)
    # TODO: this may not make sense -- the protobuf utf8-encodes values
    r = entity_pb.Reference()
    r.set_app('_')
    e = r.mutable_path().add_element()
    e.set_type(flat_input[0])
    e.set_name(flat_input[3])
    k = key.Key(reference=r)
    self.assertEqual(k.reference(), r)

  def testHash(self):
    flat = ['Kind', 1, 'Subkind', 'foobar']
    pairs = [(flat[i], flat[i+1]) for i in xrange(0, len(flat), 2)]
    k = key.Key(flat=flat)
    self.assertEqual(hash(k), hash(tuple(pairs)))

  def testPickling(self):
    flat = ['Kind', 1, 'Subkind', 'foobar']
    pairs = [(flat[i], flat[i+1]) for i in xrange(0, len(flat), 2)]
    k = key.Key(flat=flat)
    for proto in range(pickle.HIGHEST_PROTOCOL + 1):
      s = pickle.dumps(k, protocol=proto)
      kk = pickle.loads(s)
      self.assertEqual(k, kk)

  def testIncomplete(self):
    k = key.Key(flat=['Kind', None])
    self.assertRaises(AssertionError,
                      key.Key, flat=['Kind', None, 'Subkind', 1])
    self.assertRaises(AssertionError, key.Key, flat=['Kind', ()])


def main():
  unittest.main()

if __name__ == '__main__':
  main()
