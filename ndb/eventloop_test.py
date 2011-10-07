"""Tests for eventloop.py."""

import os
import time
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.datastore import datastore_rpc

from . import eventloop, test_utils

class EventLoopTests(test_utils.NDBTest):

  def setUp(self):
    super(EventLoopTests, self).setUp()
    if eventloop._EVENT_LOOP_KEY in os.environ:
      del os.environ[eventloop._EVENT_LOOP_KEY]
    self.ev = eventloop.get_event_loop()

  def testQueueTasklet(self):
    def f(unused_number, unused_string, unused_a, unused_b): return 1
    def g(unused_number, unused_string): return 2
    def h(unused_c, unused_d): return 3
    t_before = time.time()
    eventloop.queue_call(1, f, 42, 'hello', unused_a=1, unused_b=2)
    eventloop.queue_call(3, h, unused_c=3, unused_d=4)
    eventloop.queue_call(2, g, 100, 'abc')
    t_after = time.time()
    self.assertEqual(len(self.ev.queue), 3)
    [(t1, f1, a1, k1), (t2, f2, a2, k2), (t3, f3, a3, k3)] = self.ev.queue
    self.assertTrue(t1 < t2)
    self.assertTrue(t2 < t3)
    self.assertTrue(abs(t1 - (t_before + 1)) <= t_after - t_before)
    self.assertTrue(abs(t2 - (t_before + 2)) <= t_after - t_before)
    self.assertTrue(abs(t3 - (t_before + 3)) <= t_after - t_before)
    self.assertEqual(f1, f)
    self.assertEqual(f2, g)
    self.assertEqual(f3, h)
    self.assertEqual(a1, (42, 'hello'))
    self.assertEqual(a2, (100, 'abc'))
    self.assertEqual(a3, ())
    self.assertEqual(k1, {'unused_a': 1, 'unused_b': 2})
    self.assertEqual(k2, {})
    self.assertEqual(k3, {'unused_c': 3, 'unused_d': 4})
    # Delete queued events (they would fail or take a long time).
    ev = eventloop.get_event_loop()
    ev.queue = []
    ev.rpcs = {}

  def testFifoOrderForEventsWithDelayNone(self):
    order = []
    def foo(arg): order.append(arg)

    eventloop.queue_call(None, foo, 2)
    eventloop.queue_call(None, foo, 1)

    self.assertEqual(len(self.ev.queue), 2)
    [(_t1, _f1, a1, _k1), (_t2, _f2, a2, _k2)] = self.ev.queue
    self.assertEqual(a1, (2,))  # first event should has arg = 2
    self.assertEqual(a2, (1,))  # second event should  has arg = 1

    eventloop.run()
    # test that events are executed in FIFO order, not sort order
    self.assertEqual(order, [2, 1])

  def testRun(self):
    record = []
    def foo(arg):
      record.append(arg)
    eventloop.queue_call(0.2, foo, 42)
    eventloop.queue_call(0.1, foo, arg='hello')
    eventloop.run()
    self.assertEqual(record, ['hello', 42])

  def testRunWithRpcs(self):
    record = []
    def foo(arg):
      record.append(arg)
    eventloop.queue_call(0.1, foo, 42)
    config = datastore_rpc.Configuration(on_completion=foo)
    rpc = self.conn.async_get(config, [])
    if not isinstance(rpc, apiproxy_stub_map.UserRPC):
      self.assertEqual(len(rpc.rpcs), 1)
      rpc = rpc.rpcs[0]
    eventloop.queue_rpc(rpc)
    eventloop.run()
    self.assertEqual(record, [rpc, 42])
    self.assertEqual(rpc.state, 2)  # TODO: Use apiproxy_rpc.RPC.FINISHING.

def main():
  unittest.main()

if __name__ == '__main__':
  main()
