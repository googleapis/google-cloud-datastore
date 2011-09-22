"""Tests for key.py."""

import base64
import pickle
import unittest

from google.appengine.api import datastore_errors
from google.appengine.datastore import entity_pb

from . import eventloop, key, model, tasklets, test_utils

class KeyTests(test_utils.DatastoreTest):

  def testShort(self):
    k0 = key.Key('Kind', None)
    self.assertEqual(k0.flat(), ('Kind', None))
    k1 = key.Key('Kind', 1)
    self.assertEqual(k1.flat(), ('Kind', 1))
    k2 = key.Key('Parent', 42, 'Kind', 1)
    self.assertEqual(k2.flat(), ('Parent', 42, 'Kind', 1))

  def testFlat(self):
    flat = ('Kind', 1)
    pairs = tuple((flat[i], flat[i+1]) for i in xrange(0, len(flat), 2))
    k = key.Key(flat=flat)
    self.assertEqual(k.pairs(), pairs)
    self.assertEqual(k.flat(), flat)
    self.assertEqual(k.kind(), 'Kind')

  def testFlatLong(self):
    flat = ('Kind', 1, 'Subkind', 'foobar')
    pairs = tuple((flat[i], flat[i+1]) for i in xrange(0, len(flat), 2))
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
    self.assertTrue(k.reference() is not r)
    self.assertEqual(k.serialized(), serialized)
    self.assertEqual(k.urlsafe(), urlsafe)
    self.assertEqual(k.reference(), r)

    k = key.Key(reference=r, app=r.app(), namespace='')
    self.assertTrue(k.reference() is not r)
    self.assertEqual(k.serialized(), serialized)
    self.assertEqual(k.urlsafe(), urlsafe)
    self.assertEqual(k.reference(), r)

    k1 = key.Key('A', 1)
    self.assertEqual(k1.urlsafe(), 'agFfcgcLEgFBGAEM')
    k2 = key.Key(urlsafe=k1.urlsafe())
    self.assertEqual(k1, k2)

  def testId(self):
    k1 = key.Key('Kind', 'foo', app='app1', namespace='ns1')
    self.assertEqual(k1.id(), 'foo')

    k2 = key.Key('Subkind', 42, parent=k1)
    self.assertEqual(k2.id(), 42)

    k3 = key.Key('Subkind', 'bar', parent=k2)
    self.assertEqual(k3.id(), 'bar')

    # incomplete key
    k4 = key.Key('Subkind', None, parent=k3)
    self.assertEqual(k4.id(), None)

  def testStringId(self):
    k1 = key.Key('Kind', 'foo', app='app1', namespace='ns1')
    self.assertEqual(k1.string_id(), 'foo')

    k2 = key.Key('Subkind', 'bar', parent=k1)
    self.assertEqual(k2.string_id(), 'bar')

    k3 = key.Key('Subkind', 42, parent=k2)
    self.assertEqual(k3.string_id(), None)

    # incomplete key
    k4 = key.Key('Subkind', None, parent=k3)
    self.assertEqual(k4.string_id(), None)

  def testIntegerId(self):
    k1 = key.Key('Kind', 42, app='app1', namespace='ns1')
    self.assertEqual(k1.integer_id(), 42)

    k2 = key.Key('Subkind', 43, parent=k1)
    self.assertEqual(k2.integer_id(), 43)

    k3 = key.Key('Subkind', 'foobar', parent=k2)
    self.assertEqual(k3.integer_id(), None)

    # incomplete key
    k4 = key.Key('Subkind', None, parent=k3)
    self.assertEqual(k4.integer_id(), None)

  def testParent(self):
    p = key.Key('Kind', 1, app='app1', namespace='ns1')
    self.assertEqual(p.parent(), None)

    k = key.Key('Subkind', 'foobar', parent=p)
    self.assertEqual(k.flat(), ('Kind', 1, 'Subkind', 'foobar'))
    self.assertEqual(k.parent(), p)

    k = key.Key('Subkind', 'foobar', parent=p,
                app=p.app(), namespace=p.namespace())
    self.assertEqual(k.flat(), ('Kind', 1, 'Subkind', 'foobar'))
    self.assertEqual(k.parent(), p)

  def testRoot(self):
    p = key.Key('Kind', 1, app='app1', namespace='ns1')
    self.assertEqual(p.root(), p)

    k = key.Key('Subkind', 'foobar', parent=p)
    self.assertEqual(k.flat(), ('Kind', 1, 'Subkind', 'foobar'))
    self.assertEqual(k.root(), p)

    k2 = key.Key('Subsubkind', 42, parent=k,
                app=p.app(), namespace=p.namespace())
    self.assertEqual(k2.flat(), ('Kind', 1,
                                 'Subkind', 'foobar',
                                 'Subsubkind', 42))
    self.assertEqual(k2.root(), p)

  def testRepr_Inferior(self):
    k = key.Key('Kind', 1L, 'Subkind', 'foobar')
    self.assertEqual(repr(k),
                     "Key('Kind', 1, 'Subkind', 'foobar')")
    self.assertEqual(repr(k), str(k))

  def testRepr_Toplevel(self):
    k = key.Key('Kind', 1)
    self.assertEqual(repr(k), "Key('Kind', 1)")

  def testRepr_Incomplete(self):
    k = key.Key('Kind', None)
    self.assertEqual(repr(k), "Key('Kind', None)")

  def testRepr_UnicodeKind(self):
    k = key.Key(u'\u1234', 1)
    self.assertEqual(repr(k), "Key('\\xe1\\x88\\xb4', 1)")

  def testRepr_UnicodeId(self):
    k = key.Key('Kind', u'\u1234')
    self.assertEqual(repr(k), "Key('Kind', '\\xe1\\x88\\xb4')")

  def testRepr_App(self):
    k = key.Key('Kind', 1, app='foo')
    self.assertEqual(repr(k), "Key('Kind', 1, app='foo')")

  def testRepr_Namespace(self):
    k = key.Key('Kind', 1, namespace='foo')
    self.assertEqual(repr(k), "Key('Kind', 1, namespace='foo')")

  def testUnicode(self):
    flat_input = (u'Kind\u1234', 1, 'Subkind', u'foobar\u4321')
    flat = (flat_input[0].encode('utf8'), flat_input[1],
            flat_input[2], flat_input[3].encode('utf8'))
    pairs = tuple((flat[i], flat[i+1]) for i in xrange(0, len(flat), 2))
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
    self.assertRaises(datastore_errors.BadArgumentError,
                      key.Key, flat=['Kind', None, 'Subkind', 1])
    self.assertRaises(AssertionError, key.Key, flat=['Kind', ()])

  def testKindFromModel(self):
    from . import model
    class M(model.Model):
      pass
    class N(model.Model):
      @classmethod
      def _get_kind(cls):
        return 'NN'
    k = key.Key(M, 1)
    self.assertEqual(k, key.Key('M', 1))
    k = key.Key('X', 1, N, 2, 'Y', 3)
    self.assertEqual(k, key.Key('X', 1, 'NN', 2, 'Y', 3))

  def testKindFromBadValue(self):
    # TODO: BadArgumentError
    self.assertRaises(Exception, key.Key, 42, 42)
    
  def testPreDeleteHook(self):
    self.counter = 0
    
    class Foo(model.Model):
      @classmethod
      def _pre_delete_hook(cls, ctx, key):
        self.counter += 1
        
    x = Foo()
    self.assertEqual(self.counter, 0,
                     'Delete hook triggered by entity creation')
    x.put()
    self.assertEqual(self.counter, 0, 'Delete hook triggered by entity put')
    x.key.delete()
    self.assertEqual(self.counter, 1,
                     'Delete hook not triggered on key deletion')
    
  def testPostDeleteHook(self):
    self.counter = 0
    
    class Foo(model.Model):
      @classmethod
      def _post_delete_hook(cls, ctx, key):
        self.counter += 1
        
    x = Foo()
    eventloop.get_event_loop().run()
    self.assertEqual(self.counter, 0,
                     'Delete hook triggered by entity creation')
    x.put()
    eventloop.get_event_loop().run()
    self.assertEqual(self.counter, 0, 'Delete hook triggered by entity put')
    x.key.delete()
    self.assertEqual(self.counter, 0,
                     'Delete hook triggered by key deletion before eventloop')
    eventloop.get_event_loop().run()
    self.assertEqual(self.counter, 1,
                     'Delete hook not triggered on key deletion')
    
  def testPreDeleteHookMulti(self):
    self.counter = 0
    
    class Foo(model.Model):
      @classmethod
      def _pre_delete_hook(cls, ctx, key):
        self.counter += 1
        
    entities = [Foo() for _ in range(10)]
    model.put_multi(entities)
    keys = [entity.key for entity in entities]
    model.delete_multi(keys)
    self.assertEqual(self.counter, 10,
                     '%i/10 Delete hooks not triggered on model.delete_multi' %
                     (10 - self.counter))
    
  def testPostDeleteHookMulti(self):
    self.counter = 0
    
    class Foo(model.Model):
      @classmethod
      def _post_delete_hook(cls, ctx, key):
        self.counter += 1
        
    entities = [Foo() for _ in range(10)]
    model.put_multi(entities)
    keys = [entity.key for entity in entities]
    model.delete_multi(keys)
    self.assertEqual(self.counter, 0,
         '%i/10 Delete hooks triggered by model.delete_multi before eventloop' %
         self.counter)
    eventloop.get_event_loop().run()
    self.assertEqual(self.counter, 10,
                     '%i/10 Delete hooks not triggered on model.delete_multi' %
                     (10 - self.counter))
    
  def testMonkeyPatchPreDeleteHook(self):
    original_hook = model.Model._pre_delete_hook
    self.flag = False
    
    class Foo(model.Model):
      @classmethod
      def _pre_delete_hook(cls, ctx, key):
        self.flag = True
    model.Model._pre_delete_hook = Foo._pre_delete_hook
    
    try:
      entity = Foo()
      entity.put()
      entity.key.delete()
      self.assertTrue(self.flag)
    finally:
      model.Model._pre_delete_hook = original_hook
    
  def testMonkeyPatchPostDeleteHook(self):
    original_hook = model.Model._post_delete_hook
    self.flag = False
    
    class Foo(model.Model):
      @classmethod
      def _post_delete_hook(cls, ctx, key):
        self.flag = True
    model.Model._post_delete_hook = Foo._post_delete_hook
    
    try:
      entity = Foo()
      entity.put()
      entity.key.delete()
      eventloop.get_event_loop().run()
      self.assertTrue(self.flag)
    finally:
      model.Model._post_delete_hook = original_hook
      
  def testPreDeleteHookCannotCancelRPC(self):
    class Foo(model.Model):
      @classmethod
      def _pre_delete_hook(*args):
        raise tasklets.Return()
    entity = Foo()
    entity.put()
    self.assertRaises(tasklets.Return, entity.key.delete)
    
  def testPreGetHook(self):
    self.counter = 0
    
    class Foo(model.Model):
      @classmethod
      def _pre_get_hook(cls, ctx, key):
        self.counter += 1
        
    x = Foo()
    self.assertEqual(self.counter, 0, 'Get hook triggered by entity creation')
    x.put()
    self.assertEqual(self.counter, 0, 'Get hook triggered by entity put')
    x.key.get()
    self.assertEqual(self.counter, 1, 'Get hook not triggered on key get')
    
  def testPostGetHook(self):
    self.counter = 0
    
    class Foo(model.Model):
      @classmethod
      def _post_get_hook(cls, ctx, key):
        self.counter += 1
        
    x = Foo()
    eventloop.get_event_loop().run()
    self.assertEqual(self.counter, 0, 'Get hook triggered by entity creation')
    x.put()
    eventloop.get_event_loop().run()
    self.assertEqual(self.counter, 0, 'Get hook triggered by entity put')
    x.key.get()
    self.assertEqual(self.counter, 0,
                     'Get hook triggered by key get before eventloop')
    eventloop.get_event_loop().run()
    self.assertEqual(self.counter, 1,
                     'Get hook not triggered on key get')
    
  def testPreGetHookMulti(self):
    self.counter = 0
    
    class Foo(model.Model):
      @classmethod
      def _pre_get_hook(cls, ctx, key):
        self.counter += 1
        
    entities = [Foo() for _ in range(10)]
    model.put_multi(entities)
    keys = [entity.key for entity in entities]
    model.get_multi(keys)
    self.assertEqual(self.counter, 10,
                     '%i/10 Get hooks not triggered on model.get_multi' %
                     (10 - self.counter))
    
  def testPostGetHookMulti(self):
    self.counter = 0
    
    class Foo(model.Model):
      @classmethod
      def _post_get_hook(cls, ctx, key):
        self.counter += 1
        
    entities = [Foo() for _ in range(10)]
    model.put_multi(entities)
    keys = [entity.key for entity in entities]
    model.get_multi(keys)
    self.assertEqual(self.counter, 0,
         '%i/10 Get hooks triggered by model.get_multi before eventloop' %
         self.counter)
    eventloop.get_event_loop().run()
    self.assertEqual(self.counter, 10,
                     '%i/10 Get hooks not triggered on model.get_multi' %
                     (10 - self.counter))
    
  def testMonkeyPatchPreGetHook(self):
    original_hook = model.Model._pre_get_hook
    self.flag = False
    
    class Foo(model.Model):
      @classmethod
      def _pre_get_hook(cls, ctx, key):
        self.flag = True
    model.Model._pre_get_hook = Foo._pre_get_hook
    
    try:
      entity = Foo()
      entity.put()
      entity.key.get()
      self.assertTrue(self.flag)
    finally:
      model.Model._pre_get_hook = original_hook
    
  def testMonkeyPatchPostGetHook(self):
    original_hook = model.Model._post_get_hook
    self.flag = False
    
    class Foo(model.Model):
      @classmethod
      def _post_get_hook(cls, ctx, key):
        self.flag = True
    model.Model._post_get_hook = Foo._post_get_hook
    
    try:
      entity = Foo()
      entity.put()
      entity.key.get()
      eventloop.get_event_loop().run()
      self.assertTrue(self.flag)
    finally:
      model.Model._post_get_hook = original_hook
      
  def testPreGetHookCannotCancelRPC(self):
    class Foo(model.Model):
      @classmethod
      def _pre_get_hook(*args):
        raise tasklets.Return()
    entity = Foo()
    entity.put()
    self.assertRaises(tasklets.Return, entity.key.get)


def main():
  unittest.main()

if __name__ == '__main__':
  main()
