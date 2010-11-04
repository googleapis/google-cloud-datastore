"""Tests for eventloop.py."""

import os
import time
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_file_stub

from core import datastore_rpc

from ndb import eventloop

class EventLoopTests(unittest.TestCase):

  def setUp(self):
    if eventloop._EVENT_LOOP_KEY in os.environ:
      del os.environ[eventloop._EVENT_LOOP_KEY]
    self.ev = eventloop.get_event_loop()

  def testQueueTask(self):
    def f(): return 1
    def g(): return 2
    def h(): return 3
    t_before = time.time()
    eventloop.queue_task(1, f, 42, 'hello', a=1, b=2)
    eventloop.queue_task(3, h, c=3, d=4)
    eventloop.queue_task(2, g, 100, 'abc')
    t_after = time.time()
    self.assertEqual(len(self.ev.queue), 3)
    [(t1, f1, a1, k1), (t2, f2, a2, k2), (t3, f3, a3, k3)] = self.ev.queue
    self.assertTrue(t1 < t2)
    self.assertTrue(t2 < t3)
    self.assertTrue(abs(t1 - (t_before + 1)) < t_after - t_before)
    self.assertTrue(abs(t2 - (t_before + 2)) < t_after - t_before)
    self.assertTrue(abs(t3 - (t_before + 3)) < t_after - t_before)
    self.assertEqual(f1, f)
    self.assertEqual(f2, g)
    self.assertEqual(f3, h)
    self.assertEqual(a1, (42, 'hello'))
    self.assertEqual(a2, (100, 'abc'))
    self.assertEqual(a3, ())
    self.assertEqual(k1, {'a': 1, 'b': 2})
    self.assertEqual(k2, {})
    self.assertEqual(k3, {'c': 3, 'd': 4})

  def testRun(self):
    record = []
    def foo(arg):
      record.append(arg)
    eventloop.queue_task(0.2, foo, 42)
    eventloop.queue_task(0.1, foo, arg='hello')
    eventloop.run()
    self.assertEqual(record, ['hello', 42])

  def testRunWithRpcs(self):
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    stub = datastore_file_stub.DatastoreFileStub('_', None)
    apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', stub)
    record = []
    def foo(arg):
      record.append(arg)
    eventloop.queue_task(0.1, foo, 42)
    conn = datastore_rpc.Connection()
    config = datastore_rpc.Configuration(on_completion=foo)
    rpc = conn.async_get(config, [])
    self.assertEqual(len(rpc.rpcs), 1)
    eventloop.queue_rpc(rpc)
    eventloop.run()
    self.assertEqual(record, [rpc.rpcs[0], 42])
    self.assertEqual(rpc.state, 2)  # TODO: Use apiproxy_rpc.RPC.FINISHING.

def main():
  unittest.main()

if __name__ == '__main__':
  main()
