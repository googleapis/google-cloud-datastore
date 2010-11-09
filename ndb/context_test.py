"""Tests for context.py."""

import os
import re
import sys
import time
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_file_stub

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

  def __init__(self, method, options=None):
    super(MyAutoBatcher, self).__init__(method, options)

  def _rpc_callback(self, *args):
    self._log.append(args)
    super(MyAutoBatcher, self)._rpc_callback(*args)


class TaskTests(unittest.TestCase):

  def setUp(self):
    self.set_up_eventloop()
    self.set_up_datastore()
    MyAutoBatcher.reset_log()
    self.ctx = context.Context(auto_batcher_class=MyAutoBatcher)

  def set_up_eventloop(self):
    if eventloop._EVENT_LOOP_KEY in os.environ:
      del os.environ[eventloop._EVENT_LOOP_KEY]
    self.ev = eventloop.get_event_loop()
    self.log = []

  def set_up_datastore(self):
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    stub = datastore_file_stub.DatastoreFileStub('_', None)
    apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', stub)

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
      self.assertTrue(key1 in self.ctx._cache)
      a = yield self.ctx.get(key1)
      b = yield self.ctx.get(key1)
      self.assertTrue(a is b)
    foo().check_success()

  def testContext_MapQuery(self):
    @tasks.task
    def callback(ent):
      return list(ent.key.flat())[-1]
    @tasks.task
    def foo():
      yield self.create_entities()
      query = datastore_query.Query(app='_', kind='Foo')
      fut1, fut2 = self.ctx.map_query(query, callback)
      res1 = yield fut1  # This is a list of Futures
      res1 = yield res1  # Turn it into a list of results
      res2 = yield fut2
      raise tasks.Return([res1, res2])
    res1, res2 = foo().get_result()
    self.assertEqual(res1, [1, 2, 3])
    self.assertEqual(res2, 3)

  def testContext_MapQuery_NonTaskCallback(self):
    def callback(ent):
      return list(ent.key.flat())[-1]
    @tasks.task
    def foo():
      yield self.create_entities()
      query = datastore_query.Query(app='_', kind='Foo')
      fut1, fut2 = self.ctx.map_query(query, callback)
      res1 = yield fut1  # This is a list of values
      res2 = yield fut2
      raise tasks.Return([res1, res2])
    res1, res2 = foo().get_result()
    self.assertEqual(res1, [1, 2, 3])
    self.assertEqual(res2, 3)

  def testContext_GetOrInsert(self):
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
      @context.add_context
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
