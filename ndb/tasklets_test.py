"""Tests for tasklets.py."""

import os
import re
import random
import sys
import time
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_file_stub

from google.appengine.datastore import datastore_rpc

from ndb import eventloop
from ndb import model
from ndb import test_utils
from ndb import tasklets
from ndb.tasklets import Future, tasklet

class TaskletTests(test_utils.DatastoreTest):

  def setUp(self):
    super(TaskletTests, self).setUp()
    if eventloop._EVENT_LOOP_KEY in os.environ:
      del os.environ[eventloop._EVENT_LOOP_KEY]
    if tasklets._CONTEXT_KEY in os.environ:
      del os.environ[tasklets._CONTEXT_KEY]
    self.ev = eventloop.get_event_loop()
    self.log = []

  def universal_callback(self, *args):
    self.log.append(args)

  def testFuture_Constructor(self):
    f = tasklets.Future()
    self.assertEqual(f._result, None)
    self.assertEqual(f._exception, None)
    self.assertEqual(f._callbacks, [])

  def testFuture_Repr(self):
    f = tasklets.Future()
    prefix = (r'<Future [\da-f]+ created by '
              r'testFuture_Repr\(tasklets_test.py:\d+\) ')
    self.assertTrue(re.match(prefix + r'pending>$', repr(f)), repr(f))
    f.set_result('abc')
    self.assertTrue(re.match(prefix + r'result \'abc\'>$', repr(f)), repr(f))
    f = tasklets.Future()
    f.set_exception(RuntimeError('abc'))
    self.assertTrue(re.match(prefix + r'exception RuntimeError: abc>$',
                             repr(f)),
                    repr(f))

  def testFuture_Done_State(self):
    f = tasklets.Future()
    self.assertFalse(f.done())
    self.assertEqual(f.state, f.RUNNING)
    f.set_result(42)
    self.assertTrue(f.done())
    self.assertEqual(f.state, f.FINISHING)

  def testFuture_SetResult(self):
    f = tasklets.Future()
    f.set_result(42)
    self.assertEqual(f._result, 42)
    self.assertEqual(f._exception, None)
    self.assertEqual(f.get_result(), 42)

  def testFuture_SetException(self):
    f = tasklets.Future()
    err = RuntimeError(42)
    f.set_exception(err)
    self.assertEqual(f.done(), True)
    self.assertEqual(f._exception, err)
    self.assertEqual(f._result, None)
    self.assertEqual(f.get_exception(), err)
    self.assertRaises(RuntimeError, f.get_result)

  def testFuture_AddDoneCallback_SetResult(self):
    f = tasklets.Future()
    f.add_callback(self.universal_callback, f)
    self.assertEqual(self.log, [])  # Nothing happened yet.
    f.set_result(42)
    eventloop.run()
    self.assertEqual(self.log, [(f,)])

  def testFuture_SetResult_AddDoneCallback(self):
    f = tasklets.Future()
    f.set_result(42)
    self.assertEqual(f.get_result(), 42)
    f.add_callback(self.universal_callback, f)
    eventloop.run()
    self.assertEqual(self.log, [(f,)])

  def testFuture_AddDoneCallback_SetException(self):
    f = tasklets.Future()
    f.add_callback(self.universal_callback, f)
    f.set_exception(RuntimeError(42))
    eventloop.run()
    self.assertEqual(self.log, [(f,)])
    self.assertEqual(f.done(), True)

  def create_futures(self):
    self.futs = []
    for i in range(5):
      f = tasklets.Future()
      f.add_callback(self.universal_callback, f)
      def wake(fut, result):
        fut.set_result(result)
      self.ev.queue_call(i*0.01, wake, f, i)
      self.futs.append(f)
    return set(self.futs)

  def testFuture_WaitAny(self):
    self.assertEqual(tasklets.Future.wait_any([]), None)
    todo = self.create_futures()
    while todo:
      f = tasklets.Future.wait_any(todo)
      todo.remove(f)
    eventloop.run()
    self.assertEqual(self.log, [(f,) for f in self.futs])

  def testFuture_WaitAll(self):
    todo = self.create_futures()
    tasklets.Future.wait_all(todo)
    self.assertEqual(self.log, [(f,) for f in self.futs])

  def testSleep(self):
    log = []
    @tasklets.tasklet
    def foo():
      log.append(time.time())
      yield tasklets.sleep(0.1)
      log.append(time.time())
    foo()
    eventloop.run()
    t0, t1 = log
    dt = t1-t0
    self.assertAlmostEqual(dt, 0.1, places=2)

  def testMultiFuture(self):
    @tasklets.tasklet
    def foo(dt):
      yield tasklets.sleep(dt)
      raise tasklets.Return('foo-%s' % dt)
    @tasklets.tasklet
    def bar(n):
      for i in range(n):
        yield tasklets.sleep(0.01)
      raise tasklets.Return('bar-%d' % n)
    bar5 = bar(5)
    futs = [foo(0.05), foo(0.01), foo(0.03), bar(3), bar5, bar5]
    mfut = tasklets.MultiFuture()
    for fut in futs:
      mfut.add_dependent(fut)
    mfut.complete()
    results = mfut.get_result()
    self.assertEqual(set(results),
                     set(['foo-0.01', 'foo-0.03', 'foo-0.05',
                          'bar-3', 'bar-5']))

  def testMultiFuture_PreCompleted(self):
    @tasklets.tasklet
    def foo():
      yield tasklets.sleep(0.01)
      raise tasklets.Return(42)
    mfut = tasklets.MultiFuture()
    dep = foo()
    dep.wait()
    mfut.add_dependent(dep)
    mfut.complete()
    eventloop.run()
    self.assertTrue(mfut.done())
    self.assertEqual(mfut.get_result(), [42])

  def testQueueFuture(self):
    q = tasklets.QueueFuture()
    @tasklets.tasklet
    def produce_one(i):
      yield tasklets.sleep(i * 0.01)
      raise tasklets.Return(i)
    @tasklets.tasklet
    def producer():
      q.putq(0)
      for i in range(1, 10):
        q.add_dependent(produce_one(i))
      q.complete()
    @tasklets.tasklet
    def consumer():
      for i in range(10):
        val = yield q.getq()
        self.assertEqual(val, i)
      yield q
      self.assertRaises(EOFError, q.getq().get_result)
    @tasklets.tasklet
    def foo():
      yield producer(), consumer()
    foo().get_result()

  def testSerialQueueFuture(self):
    q = tasklets.SerialQueueFuture()
    @tasklets.tasklet
    def produce_one(i):
      yield tasklets.sleep(random.randrange(10) * 0.01)
      raise tasklets.Return(i)
    @tasklets.tasklet
    def producer():
      for i in range(10):
        q.add_dependent(produce_one(i))
      q.complete()
    @tasklets.tasklet
    def consumer():
      for i in range(10):
        val = yield q.getq()
        self.assertEqual(val, i)
      yield q
      self.assertRaises(EOFError, q.getq().get_result)
      yield q
    @tasklets.synctasklet
    def foo():
      yield producer(), consumer()
    foo()

  def testReducerFuture(self):
    @tasklets.tasklet
    def sum_tasklet(arg):
      yield tasklets.sleep(0.01)
      raise tasklets.Return(sum(arg))
    @tasklets.tasklet
    def produce_one(i):
      yield tasklets.sleep(i * 0.01)
      raise tasklets.Return(i)
    @tasklets.tasklet
    def producer():
      for i in range(10):
        q.add_dependent(produce_one(i))
      q.complete()
    @tasklets.tasklet
    def consumer():
      total = yield q
      self.assertEqual(total, sum(range(10)))
    @tasklets.tasklet
    def foo():
      yield producer(), consumer()
    q = tasklets.ReducingFuture(sum_tasklet, batch_size=3)
    foo().get_result()
    q = tasklets.ReducingFuture(sum, batch_size=3)
    foo().get_result()

  def testGetReturnValue(self):
      r0 = tasklets.Return()
      r1 = tasklets.Return(42)
      r2 = tasklets.Return(42, 'hello')
      r3 = tasklets.Return((1, 2, 3))
      self.assertEqual(tasklets.get_return_value(r0), None)
      self.assertEqual(tasklets.get_return_value(r1), 42)
      self.assertEqual(tasklets.get_return_value(r2), (42, 'hello'))
      self.assertEqual(tasklets.get_return_value(r3), (1, 2, 3))

  def testTasklets_Basic(self):
    @tasklets.tasklet
    def t1():
      a = yield t2(3)
      b = yield t3(2)
      raise tasklets.Return(a + b)
    @tasklets.tasklet
    def t2(n):
      raise tasklets.Return(n)
    @tasklets.tasklet
    def t3(n):
      return n
    x = t1()
    self.assertTrue(isinstance(x, tasklets.Future))
    y = x.get_result()
    self.assertEqual(y, 5)

  def testTasklets_Raising(self):
    @tasklets.tasklet
    def t1():
      f = t2(True)
      try:
        a = yield f
      except RuntimeError, err:
        self.assertEqual(f.get_exception(), err)
        raise tasklets.Return(str(err))
    @tasklets.tasklet
    def t2(error):
      if error:
        raise RuntimeError('hello')
      else:
        yield tasklets.Future()
    x = t1()
    y = x.get_result()
    self.assertEqual(y, 'hello')

  def testTasklets_YieldRpcs(self):
    @tasklets.tasklet
    def main_tasklet():
      rpc1 = self.conn.async_get(None, [])
      rpc2 = self.conn.async_put(None, [])
      res1 = yield rpc1
      res2 = yield rpc2
      raise tasklets.Return(res1, res2)
    f = main_tasklet()
    result = f.get_result()
    self.assertEqual(result, ([], []))

  def testTasklet_YieldTuple(self):
    @tasklets.tasklet
    def fib(n):
      if n <= 1:
        raise tasklets.Return(n)
      a, b = yield fib(n - 1), fib(n - 2)
      # print 'fib(%r) = %r + %r = %r' % (n, a, b, a + b)
      self.assertTrue(a >= b, (a, b))
      raise tasklets.Return(a + b)
    fut = fib(10)
    val = fut.get_result()
    self.assertEqual(val, 55)

class TracebackTests(unittest.TestCase):
  """Checks that errors result in reasonable tracebacks."""

  def testBasicError(self):
    frames = [sys._getframe()]
    @tasklets.tasklet
    def level3():
      frames.append(sys._getframe())
      raise RuntimeError('hello')
      yield
    @tasklets.tasklet
    def level2():
      frames.append(sys._getframe())
      yield level3()
    @tasklets.tasklet
    def level1():
      frames.append(sys._getframe())
      yield level2()
    @tasklets.tasklet
    def level0():
      frames.append(sys._getframe())
      yield level1()
    fut = level0()
    try:
      fut.check_success()
    except RuntimeError, err:
      _, _, tb = sys.exc_info()
      self.assertEqual(str(err), 'hello')
      tbframes = []
      while tb is not None:
        # It's okay if some _help_tasklet_along frames are present.
        if tb.tb_frame.f_code.co_name != '_help_tasklet_along':
          tbframes.append(tb.tb_frame)
        tb = tb.tb_next
      self.assertEqual(frames, tbframes)
    else:
      self.fail('Expected RuntimeError not raised')


def main():
  unittest.main()

if __name__ == '__main__':
  main()
