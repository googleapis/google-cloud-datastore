"""Tests for context.py."""

import logging
import os
import re
import sys
import time
import unittest

from google.appengine.api import memcache
from google.appengine.datastore import datastore_rpc

from ndb import context
from ndb import eventloop
from ndb import model
from ndb import query
from ndb import tasklets
from ndb import test_utils


class MyAutoBatcher(context.AutoBatcher):

  _log = []

  @classmethod
  def reset_log(cls):
    cls._log = []

  def __init__(self, todo_tasklet):
    def wrap(*args):
      self.__class__._log.append(args)
      return todo_tasklet(*args)
    super(MyAutoBatcher, self).__init__(wrap)


class ContextTests(test_utils.DatastoreTest):

  def setUp(self):
    super(ContextTests, self).setUp()
    self.set_up_eventloop()
    MyAutoBatcher.reset_log()
    self.ctx = context.Context(
        conn=model.make_connection(default_model=model.Expando),
        auto_batcher_class=MyAutoBatcher)

  def set_up_eventloop(self):
    if eventloop._EVENT_LOOP_KEY in os.environ:
      del os.environ[eventloop._EVENT_LOOP_KEY]
    self.ev = eventloop.get_event_loop()
    self.log = []

  def testContext_AutoBatcher_Get(self):
    @tasklets.tasklet
    def foo():
      key1 = model.Key(flat=['Foo', 1])
      key2 = model.Key(flat=['Foo', 2])
      key3 = model.Key(flat=['Foo', 3])
      fut1 = self.ctx.get(key1)
      fut2 = self.ctx.get(key2)
      fut3 = self.ctx.get(key3)
      ent1 = yield fut1
      ent2 = yield fut2
      ent3 = yield fut3
      raise tasklets.Return([ent1, ent2, ent3])
    ents = foo().get_result()
    self.assertEqual(ents, [None, None, None])
    self.assertEqual(len(MyAutoBatcher._log), 1)

  @tasklets.tasklet
  def create_entities(self):
    key0 = model.Key(flat=['Foo', None])
    ent1 = model.Model(key=key0)
    ent2 = model.Model(key=key0)
    ent3 = model.Model(key=key0)
    fut1 = self.ctx.put(ent1)
    fut2 = self.ctx.put(ent2)
    fut3 = self.ctx.put(ent3)
    key1 = yield fut1
    key2 = yield fut2
    key3 = yield fut3
    raise tasklets.Return([key1, key2, key3])

  def testContext_AutoBatcher_Put(self):
    keys = self.create_entities().get_result()
    self.assertEqual(len(keys), 3)
    self.assertTrue(None not in keys)
    self.assertEqual(len(MyAutoBatcher._log), 1)

  def testContext_AutoBatcher_Delete(self):
    @tasklets.tasklet
    def foo():
      key1 = model.Key(flat=['Foo', 1])
      key2 = model.Key(flat=['Foo', 2])
      key3 = model.Key(flat=['Foo', 3])
      fut1 = self.ctx.delete(key1)
      fut2 = self.ctx.delete(key2)
      fut3 = self.ctx.delete(key3)
      yield fut1
      yield fut2
      yield fut3
    foo().check_success()
    self.assertEqual(len(MyAutoBatcher._log), 1)

  def testContext_Cache(self):
    @tasklets.tasklet
    def foo():
      key1 = model.Key(flat=('Foo', 1))
      ent1 = model.Expando(key=key1, foo=42, bar='hello')
      key = yield self.ctx.put(ent1)
      self.assertTrue(key1 in self.ctx._cache)  # Whitebox.
      a = yield self.ctx.get(key1)
      b = yield self.ctx.get(key1)
      self.assertTrue(a is b)
      yield self.ctx.delete(key1)
      self.assertTrue(self.ctx._cache[key] is None)  # Whitebox.
      a = yield self.ctx.get(key1)
      self.assertTrue(a is None)
    foo().check_success()

  def testContext_CachePolicy(self):
    def should_cache(key):
      return False
    @tasklets.tasklet
    def foo():
      key1 = model.Key(flat=('Foo', 1))
      ent1 = model.Expando(key=key1, foo=42, bar='hello')
      key = yield self.ctx.put(ent1)
      self.assertTrue(key1 not in self.ctx._cache)  # Whitebox.
      a = yield self.ctx.get(key1)
      b = yield self.ctx.get(key1)
      self.assertTrue(a is not b)
      yield self.ctx.delete(key1)
      self.assertTrue(key not in self.ctx._cache)  # Whitebox.
      a = yield self.ctx.get(key1)
      self.assertTrue(a is None)
    self.ctx.set_cache_policy(should_cache)
    foo().check_success()

  def testContext_CachePolicyDisabledLater(self):
    # If the cache is disabled after an entity is stored in the cache,
    # further get() attempts *must not* return the result stored in cache.

    self.ctx.set_cache_policy(lambda key: True)
    key1 = model.Key(flat=('Foo', 1))
    ent1 = model.Expando(key=key1)
    self.ctx.put(ent1).get_result()

    # get() uses cache
    self.assertTrue(key1 in self.ctx._cache)  # Whitebox.
    self.assertEqual(self.ctx.get(key1).get_result(), ent1)

    # get() uses cache
    self.ctx._cache[key1] = None  # Whitebox.
    self.assertEqual(self.ctx.get(key1).get_result(), None)

    # get() doesn't use cache
    self.ctx.set_cache_policy(lambda key: False)
    self.assertEqual(self.ctx.get(key1).get_result(), ent1)

  def testContext_Memcache(self):
    @tasklets.tasklet
    def foo():
      key1 = model.Key(flat=('Foo', 1))
      key2 = model.Key(flat=('Foo', 2))
      ent1 = model.Expando(key=key1, foo=42, bar='hello')
      ent2 = model.Expando(key=key2, foo=1, bar='world')
      k1, k2 = yield self.ctx.put(ent1), self.ctx.put(ent2)
      self.assertEqual(k1, key1)
      self.assertEqual(k2, key2)
      yield tasklets.sleep(0.01)  # Let other tasklet complete.
      keys = [k1.urlsafe(), k2.urlsafe()]
      results = memcache.get_multi(keys)
      self.assertEqual(
        results,
        {key1.urlsafe(): self.ctx._conn.adapter.entity_to_pb(ent1),
         key2.urlsafe(): self.ctx._conn.adapter.entity_to_pb(ent2)})
    foo().check_success()

  def testContext_CacheQuery(self):
    @tasklets.tasklet
    def foo():
      key1 = model.Key(flat=('Foo', 1))
      key2 = model.Key(flat=('Foo', 2))
      ent1 = model.Expando(key=key1, foo=42, bar='hello')
      ent2 = model.Expando(key=key2, foo=1, bar='world')
      key1a, key2a = yield self.ctx.put(ent1),  self.ctx.put(ent2)
      self.assertTrue(key1 in self.ctx._cache)  # Whitebox.
      self.assertTrue(key2 in self.ctx._cache)  # Whitebox.
      self.assertEqual(key1, key1a)
      self.assertEqual(key2, key2a)
      @tasklets.tasklet
      def callback(ent):
        return ent
      qry = query.Query(kind='Foo')
      results = yield self.ctx.map_query(qry, callback)
      self.assertEqual(results, [ent1, ent2])
      self.assertTrue(results[0] is ent1)
      self.assertTrue(results[1] is ent2)
    foo().check_success()

  def testContext_AllocateIds(self):
    @tasklets.tasklet
    def foo():
      key = model.Key(flat=('Foo', 1))
      lo_hi = yield self.ctx.allocate_ids(key, size=10)
      self.assertEqual(lo_hi, (1, 10))
      lo_hi = yield self.ctx.allocate_ids(key, max=20)
      self.assertEqual(lo_hi, (11, 20))
    foo().check_success()

  def testContext_MapQuery(self):
    @tasklets.tasklet
    def callback(ent):
      return ent.key.flat()[-1]
    @tasklets.tasklet
    def foo():
      yield self.create_entities()
      qry = query.Query(kind='Foo')
      res = yield self.ctx.map_query(qry, callback)
      raise tasklets.Return(res)
    res = foo().get_result()
    self.assertEqual(set(res), set([1, 2, 3]))

  def testContext_MapQuery_NoCallback(self):
    @tasklets.tasklet
    def foo():
      yield self.create_entities()
      qry = query.Query(kind='Foo')
      res = yield self.ctx.map_query(qry, None)
      raise tasklets.Return(res)
    res = foo().get_result()
    self.assertEqual(len(res), 3)
    for i, ent in enumerate(res):
      self.assertTrue(isinstance(ent, model.Model))
      self.assertEqual(ent.key.flat(), ['Foo', i+1])

  def testContext_MapQuery_NonTaskletCallback(self):
    def callback(ent):
      return ent.key.flat()[-1]
    @tasklets.tasklet
    def foo():
      yield self.create_entities()
      qry = query.Query(kind='Foo')
      res = yield self.ctx.map_query(qry, callback)
      raise tasklets.Return(res)
    res = foo().get_result()
    self.assertEqual(res, [1, 2, 3])

  def testContext_MapQuery_CustomFuture(self):
    mfut = tasklets.QueueFuture()
    @tasklets.tasklet
    def callback(ent):
      return ent.key.flat()[-1]
    @tasklets.tasklet
    def foo():
      yield self.create_entities()
      qry = query.Query(kind='Foo')
      res = yield self.ctx.map_query(qry, callback, merge_future=mfut)
      self.assertEqual(res, None)
      vals = set()
      for i in range(3):
        val = yield mfut.getq()
        vals.add(val)
      fail = mfut.getq()
      self.assertRaises(EOFError, fail.get_result)
      raise tasklets.Return(vals)
    res = foo().get_result()
    self.assertEqual(res, set([1, 2, 3]))

  def testContext_MapQuery_KeysOnly(self):
    qo = query.QueryOptions(keys_only=True)
    @tasklets.tasklet
    def callback(key):
      return key.pairs()[-1]
    @tasklets.tasklet
    def foo():
      yield self.create_entities()
      qry = query.Query(kind='Foo')
      res = yield self.ctx.map_query(qry, callback, options=qo)
      raise tasklets.Return(res)
    res = foo().get_result()
    self.assertEqual(set(res), set([('Foo', 1), ('Foo', 2), ('Foo', 3)]))

  def testContext_IterQuery(self):
    @tasklets.tasklet
    def foo():
      yield self.create_entities()
      qry = query.Query(kind='Foo')
      it = self.ctx.iter_query(qry)
      res = []
      while True:
        try:
          ent = yield it.getq()
        except EOFError:
          break
        res.append(ent)
      raise tasklets.Return(res)
    res = foo().get_result()
    self.assertEqual(len(res), 3)
    for i, ent in enumerate(res):
      self.assertTrue(isinstance(ent, model.Model))
      self.assertEqual(ent.key.flat(), ['Foo', i+1])

  def testContext_TransactionFailed(self):
    @tasklets.tasklet
    def foo():
      key = model.Key(flat=('Foo', 1))
      ent = model.Expando(key=key, bar=1)
      yield self.ctx.put(ent)
      @tasklets.tasklet
      def callback():
        ctx = tasklets.get_context()
        self.assertTrue(key not in ctx._cache)  # Whitebox.
        e = yield key.get_async()
        self.assertTrue(key in ctx._cache)  # Whitebox.
        e.bar = 2
        yield e.put_async()
      yield self.ctx.transaction(callback)
      self.assertEqual(self.ctx._cache[key].bar, 2)
    foo().check_success()

  def testContext_GetOrInsert(self):
    # This also tests Context.transaction()
    class Mod(model.Model):
      data = model.StringProperty()
    @tasklets.tasklet
    def foo():
      ent = yield self.ctx.get_or_insert(Mod, 'a', data='hello')
      assert isinstance(ent, Mod)
      ent2 = yield self.ctx.get_or_insert(Mod, 'a', data='hello')
      assert ent2 == ent
    foo().check_success()

  def testContext_GetOrInsertWithParent(self):
    # This also tests Context.transaction()
    class Mod(model.Model):
      data = model.StringProperty()
    @tasklets.tasklet
    def foo():
      parent = model.Key(flat=('Foo', 1))
      ent = yield self.ctx.get_or_insert(Mod, 'a', parent=parent, data='hello')
      assert isinstance(ent, Mod)
      ent2 = yield self.ctx.get_or_insert(Mod, 'a', parent=parent, data='hello')
      assert ent2 == ent
    foo().check_success()

  def testAddContextDecorator(self):
    class Demo(object):
      @context.toplevel
      def method(self, arg):
        return (tasklets.get_context(), arg)
    a = Demo()
    old_ctx = tasklets.get_context()
    ctx, arg = a.method(42)
    self.assertTrue(isinstance(ctx, context.Context))
    self.assertEqual(arg, 42)
    self.assertTrue(ctx is not old_ctx)

  def testDefaultContextTransaction(self):
    @tasklets.synctasklet
    def outer():
      ctx1 = tasklets.get_context()
      @tasklets.tasklet
      def inner():
        ctx2 = tasklets.get_context()
        self.assertTrue(ctx1 is not ctx2)
        self.assertTrue(isinstance(ctx2._conn,
                                   datastore_rpc.TransactionalConnection))
        return 42
      a = yield tasklets.get_context().transaction(inner)
      ctx1a = tasklets.get_context()
      self.assertTrue(ctx1 is ctx1a)
      raise tasklets.Return(a)
    b = outer()
    self.assertEqual(b, 42)

  def testExplicitTransactionClearsDefaultContext(self):
    old_ctx = tasklets.get_context()
    @tasklets.synctasklet
    def outer():
      ctx1 = tasklets.get_context()
      @tasklets.tasklet
      def inner():
        ctx = tasklets.get_context()
        self.assertTrue(ctx is not ctx1)
        key = model.Key('Account', 1)
        ent = yield key.get_async()
        self.assertTrue(tasklets.get_context() is ctx)
        self.assertTrue(ent is None)
        raise tasklets.Return(42)
      fut = ctx1.transaction(inner)
      self.assertEqual(tasklets.get_context(), ctx1)
      val = yield fut
      self.assertEqual(tasklets.get_context(), ctx1)
      raise tasklets.Return(val)
    val = outer()
    self.assertEqual(val, 42)
    self.assertTrue(tasklets.get_context() is old_ctx)

  def testKindError(self):
    ctx = context.Context()
    # If the cache is enabled, attempts to retrieve the object we just put will
    # be satisfied from the cache, so the adapter we're testing will never get
    # called.
    ctx.set_cache_policy(lambda key: False)
    @tasklets.tasklet
    def foo():
      # Foo class is declared in query_test, so let's get a unusual class name.
      key1 = model.Key(flat=('ThisModelClassDoesntExist', 1))
      ent1 = model.Expando(key=key1, foo=42, bar='hello')
      key = yield ctx.put(ent1)
      a = yield ctx.get(key1)
    self.assertRaises(model.KindError, foo().check_success)


def main():
  ##logging.basicConfig(level=logging.INFO)
  unittest.main()


if __name__ == '__main__':
  main()
