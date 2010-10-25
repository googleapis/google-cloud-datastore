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
    self.assertEqual(f.done, False)
    self.assertEqual(f.result, None)
    self.assertEqual(f.exception, None)
    self.assertEqual(f.callbacks, [])

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
    self.assertEqual(f.done, True)
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
    self.assertEqual(f.done, True)

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
