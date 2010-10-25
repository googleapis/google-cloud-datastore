"""Tests for task.py."""

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

  def testFuture(self):
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
