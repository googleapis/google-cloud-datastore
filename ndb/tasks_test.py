"""Tests for tasks.py."""

import os
import time
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_file_stub

from core import datastore_rpc

from ndb import eventloop
from ndb import tasks

class TaskTests(unittest.TestCase):

  def setUp(self):
    if eventloop._EVENT_LOOP_KEY in os.environ:
      del os.environ[eventloop._EVENT_LOOP_KEY]
    self.ev = eventloop.get_event_loop()
    self.log = []

  def universal_callback(self, *args):
    self.log.append(args)

  def testFuture_Constructor(self):
    f = tasks.Future()
    self.assertEqual(f.result, None)
    self.assertEqual(f.exception, None)
    self.assertEqual(f.callbacks, [])

  def testFuture_Done_State(self):
    f = tasks.Future()
    self.assertFalse(f.done())
    self.assertEqual(f.state, f.RUNNING)
    f.set_result(42)
    self.assertTrue(f.done())
    self.assertEqual(f.state, f.FINISHING)

  def testFuture_SetResult(self):
    f = tasks.Future()
    f.set_result(42)
    self.assertEqual(f.result, 42)
    self.assertEqual(f.exception, None)
    self.assertEqual(f.get_result(), 42)

  def testFuture_SetException(self):
    f = tasks.Future()
    err = RuntimeError(42)
    f.set_exception(err)
    self.assertEqual(f._done, True)
    self.assertEqual(f.exception, err)
    self.assertEqual(f.result, None)
    self.assertEqual(f.get_exception(), err)
    self.assertRaises(RuntimeError, f.get_result)

  def testFuture_AddDoneCallback_SetResult(self):
    f = tasks.Future()
    f.add_done_callback(self.universal_callback)
    self.assertEqual(self.log, [])  # Nothing happened yet.
    f.set_result(42)
    self.assertEqual(self.log, [(f,)])

  def testFuture_SetResult_AddDoneCallback(self):
    f = tasks.Future()
    f.set_result(42)
    self.assertEqual(f.result, 42)
    f.add_done_callback(self.universal_callback)
    self.assertEqual(self.log, [(f,)])

  def testFuture_AddDoneCallback_SetException(self):
    f = tasks.Future()
    f.add_done_callback(self.universal_callback)
    f.set_exception(RuntimeError(42))
    self.assertEqual(self.log, [(f,)])
    self.assertEqual(f._done, True)

  def create_futures(self):
    self.futs = []
    for i in range(5):
      f = tasks.Future()
      f.add_done_callback(self.universal_callback)
      def wake(fut, result):
        fut.set_result(result)
      self.ev.queue_task(i*0.01, wake, f, i)
      self.futs.append(f)
    return set(self.futs)

  def testFuture_WaitAny(self):
    self.assertEqual(tasks.Future.wait_any([]), None)
    todo = self.create_futures()
    while todo:
      f = tasks.Future.wait_any(todo)
      todo.remove(f)
    self.assertEqual(self.log, [(f,) for f in self.futs])

  def testFuture_WaitAll(self):
    todo = self.create_futures()
    tasks.Future.wait_all(todo)
    self.assertEqual(self.log, [(f,) for f in self.futs])

  def testGetValue(self):
      r0 = tasks.Return()
      r1 = tasks.Return(42)
      r2 = tasks.Return(42, 'hello')
      r3 = tasks.Return((1, 2, 3))
      self.assertEqual(tasks.get_value(r0), None)
      self.assertEqual(tasks.get_value(r1), 42)
      self.assertEqual(tasks.get_value(r2), (42, 'hello'))
      self.assertEqual(tasks.get_value(r3), (1, 2, 3))

  def testBasicTasks(self):
    @tasks.task
    def t1():
      a = yield t2(3)
      b = yield t3(2)
      raise tasks.Return(a + b)
    @tasks.task
    def t2(n):
      raise tasks.Return(n)
    @tasks.task
    def t3(n):
      return n
    x = t1()
    self.assertTrue(isinstance(x, tasks.Future))
    y = x.get_result()
    self.assertEqual(y, 5)

def main():
  unittest.main()

if __name__ == '__main__':
  main()
