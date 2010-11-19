"""Tests for context.py."""

import os
import re
import sys
import time
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_file_stub
from google.appengine.api import memcache
from google.appengine.api.memcache import memcache_stub

from core import datastore_rpc
from core import datastore_query

from ndb import context
from ndb import eventloop
from ndb import model
from ndb import tasks


class MyAutoBatcher(context.AutoBatcher):

  _log = []

  @classmethod
  def reset_log(cls):
    cls._log = []

  def __init__(self, todo_task):
    def wrap(*args):
      self.__class__._log.append(args)
      return todo_task(*args)
    super(MyAutoBatcher, self).__init__(wrap)


class ContextTests(unittest.TestCase):

  def setUp(self):
    self.set_up_eventloop()
    self.set_up_stubs()
    MyAutoBatcher.reset_log()
    self.ctx = context.Context(auto_batcher_class=MyAutoBatcher)

  def set_up_eventloop(self):
    if eventloop._EVENT_LOOP_KEY in os.environ:
      del os.environ[eventloop._EVENT_LOOP_KEY]
    self.ev = eventloop.get_event_loop()
    self.log = []

  def set_up_stubs(self):
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    ds_stub = datastore_file_stub.DatastoreFileStub('_', None)
    apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', ds_stub)
    mc_stub = memcache_stub.MemcacheServiceStub()
    apiproxy_stub_map.apiproxy.RegisterStub('memcache', mc_stub)

  def testContext_AutoBatcher_Get(self):
    @tasks.task
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
      raise tasks.Return([ent1, ent2, ent3])
    ents = foo().get_result()
    self.assertEqual(ents, [None, None, None])
    self.assertEqual(len(MyAutoBatcher._log), 1)

  @tasks.task
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
    raise tasks.Return([key1, key2, key3])

  def testContext_AutoBatcher_Put(self):
    keys = self.create_entities().get_result()
    self.assertEqual(len(keys), 3)
    self.assertTrue(None not in keys)
    self.assertEqual(len(MyAutoBatcher._log), 1)

  def testContext_AutoBatcher_Delete(self):
    @tasks.task
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
    @tasks.task
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
    @tasks.task
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

  def testContext_Memcache(self):
    @tasks.task
    def foo():
      key1 = model.Key(flat=('Foo', 1))
      key2 = model.Key(flat=('Foo', 2))
      ent1 = model.Expando(key=key1, foo=42, bar='hello')
      ent2 = model.Expando(key=key2, foo=1, bar='world')
      k1, k2 = yield self.ctx.put(ent1), self.ctx.put(ent2)
      self.assertEqual(k1, key1)
      self.assertEqual(k2, key2)
      yield tasks.sleep(0.01)  # Let other task complete.
      keys = [k1.urlsafe(), k2.urlsafe()]
      results = memcache.get_multi(keys)
      self.assertEqual(
        results,
        {key1.urlsafe(): self.ctx._conn.adapter.entity_to_pb(ent1),
         key2.urlsafe(): self.ctx._conn.adapter.entity_to_pb(ent2)})
    foo().check_success()

  def testContext_CacheQuery(self):
    @tasks.task
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
      @tasks.task
      def callback(ent):
        return ent
      query = datastore_query.Query(app='_', kind='Foo')
      results = yield self.ctx.map_query(query, callback)
      self.assertEqual(results, [ent1, ent2])
      self.assertTrue(results[0] is ent1)
      self.assertTrue(results[1] is ent2)
    foo().check_success()

  def testContext_AllocateIds(self):
    @tasks.task
    def foo():
      key = model.Key(flat=('Foo', 1))
      lo_hi = yield self.ctx.allocate_ids(key, size=10)
      self.assertEqual(lo_hi, (1, 10))
      lo_hi = yield self.ctx.allocate_ids(key, max=20)
      self.assertEqual(lo_hi, (11, 20))
    foo().check_success()

  def testContext_MapQuery(self):
    @tasks.task
    def callback(ent):
      return ent.key.flat()[-1]
    @tasks.task
    def foo():
      yield self.create_entities()
      query = datastore_query.Query(app='_', kind='Foo')
      res = yield self.ctx.map_query(query, callback)
      raise tasks.Return(res)
    res = foo().get_result()
    self.assertEqual(set(res), set([1, 2, 3]))

  def testContext_MapQuery_NoCallback(self):
    @tasks.task
    def foo():
      yield self.create_entities()
      query = datastore_query.Query(app='_', kind='Foo')
      res = yield self.ctx.map_query(query, None)
      raise tasks.Return(res)
    res = foo().get_result()
    self.assertEqual(len(res), 3)
    for i, ent in enumerate(res):
      self.assertTrue(isinstance(ent, model.Model))
      self.assertEqual(ent.key.flat(), ['Foo', i+1])

  def testContext_MapQuery_NonTaskCallback(self):
    @tasks.task
    def callback(ent):
      return ent.key.flat()[-1]
    @tasks.task
    def foo():
      yield self.create_entities()
      query = datastore_query.Query(app='_', kind='Foo')
      res = yield self.ctx.map_query(query, callback)
      raise tasks.Return(res)
    res = foo().get_result()
    self.assertEqual(res, [1, 2, 3])

  def testContext_TransactionFailed(self):
    @tasks.task
    def foo():
      key = model.Key(flat=('Foo', 1))
      ent = model.Expando(key=key, bar=1)
      yield self.ctx.put(ent)
      @tasks.task
      def callback(ctx):
        self.assertTrue(key not in ctx._cache)  # Whitebox.
        e = yield ctx.get(key)
        self.assertTrue(key in ctx._cache)  # Whitebox.
        e.bar = 2
        yield ctx.put(e)
      yield self.ctx.transaction(callback)
      self.assertEqual(self.ctx._cache[key].bar, 2)
    foo().check_success()

  def testContext_GetOrInsert(self):
    # This also tests Context.transaction()
    class Mod(model.Model):
      data = model.StringProperty()
    @tasks.task
    def foo():
      ent = yield self.ctx.get_or_insert(Mod, 'a', data='hello')
      assert isinstance(ent, Mod)
      ent2 = yield self.ctx.get_or_insert(Mod, 'a', data='hello')
      assert ent2 == ent
    foo().check_success()

  def testAddContextDecorator(self):
    class Demo(object):
      @context.toplevel
      def method(self, arg):
        return (self.ctx, arg)
    a = Demo()
    self.assertFalse(hasattr(a, 'ctx'))
    ctx, arg = a.method(42)
    self.assertTrue(isinstance(ctx, context.Context))
    self.assertEqual(arg, 42)


def main():
  unittest.main()


if __name__ == '__main__':
  main()
