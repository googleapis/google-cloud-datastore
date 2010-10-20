"""Tests for eventloop.py."""

import time
import unittest

from ndb import eventloop

class EventLoopTests(unittest.TestCase):

  def setUp(self):
    self.ev = eventloop.EventLoop()

  def testQueueTask(self):
    def f(): return 1
    def g(): return 2
    def h(): return 3
    t_before = time.time()
    self.ev.queue_task(1, f, 42, 'hello', a=1, b=2)
    self.ev.queue_task(3, h, c=3, d=4)
    self.ev.queue_task(2, g, 100, 'abc')
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
    self.ev.queue_task(0.2, foo, 42)
    self.ev.queue_task(0.1, foo, arg='hello')
    self.ev.run()
    self.assertEqual(record, ['hello', 42])

def main():
  unittest.main()

if __name__ == '__main__':
  main()
