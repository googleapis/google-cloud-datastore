"""Tests for tasks.py."""

import os
import re
import sys
import time
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_file_stub

from core import datastore_rpc

from ndb import eventloop
from ndb import tasks
from ndb import model
from ndb.tasks import Future, task

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
    self.assertEqual(f._result, None)
    self.assertEqual(f._exception, None)
    self.assertEqual(f._callbacks, [])

  def testFuture_Repr(self):
    f = tasks.Future()
    prefix = (r'<Future [\da-f]+ created by '
              r'testFuture_Repr\(tasks_test.py:\d+\) ')
    self.assertTrue(re.match(prefix + r'pending>$', repr(f)), repr(f))
    f.set_result('abc')
    self.assertTrue(re.match(prefix + r'result \'abc\'>$', repr(f)), repr(f))
    f = tasks.Future()
    f.set_exception(RuntimeError('abc'))
    self.assertTrue(re.match(prefix + r'exception RuntimeError: abc>$',
                             repr(f)),
                    repr(f))

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
    self.assertEqual(f._result, 42)
    self.assertEqual(f._exception, None)
    self.assertEqual(f.get_result(), 42)

  def testFuture_SetException(self):
    f = tasks.Future()
    err = RuntimeError(42)
    f.set_exception(err)
    self.assertEqual(f.done(), True)
    self.assertEqual(f._exception, err)
    self.assertEqual(f._result, None)
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
    self.assertEqual(f.get_result(), 42)
    f.add_done_callback(self.universal_callback)
    self.assertEqual(self.log, [(f,)])

  def testFuture_AddDoneCallback_SetException(self):
    f = tasks.Future()
    f.add_done_callback(self.universal_callback)
    f.set_exception(RuntimeError(42))
    self.assertEqual(self.log, [(f,)])
    self.assertEqual(f.done(), True)

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

  def testSleep(self):
    log = []
    @tasks.task
    def foo():
      log.append(time.time())
      yield tasks.sleep(0.1)
      log.append(time.time())
    foo()
    eventloop.run()
    t0, t1 = log
    dt = t1-t0
    self.assertAlmostEqual(dt, 0.1, places=2)

  def testMultiFuture(self):
    @tasks.task
    def foo(dt):
      yield tasks.sleep(dt)
      raise tasks.Return('foo-%s' % dt)
    @tasks.task
    def bar(n):
      for i in range(n):
        yield tasks.sleep(0.01)
      raise tasks.Return('bar-%d' % n)
    bar5 = bar(5)
    futs = [foo(0.05), foo(0.01), foo(0.03), bar(3), bar5, bar5]
    mfut = tasks.MultiFuture()
    for fut in futs:
      mfut.add_dependent(fut)
    mfut.complete()
    completed = mfut.get_result()
    self.assertEqual(set(r.get_result() for r in completed),
                     set(['foo-0.01', 'foo-0.03', 'foo-0.05',
                          'bar-3', 'bar-5']))

  def testMultiFuture_Reducer(self):
    @tasks.task
    def foo(i):
      yield tasks.sleep(0.001)
      raise tasks.Return(i)
    def reducer(result, fut):
      assert fut.done()
      val = fut.get_result()
      return result + val
    mfut = tasks.MultiFuture(reducer, 0)
    for i in range(10):
      mfut.add_dependent(foo(i))
    mfut.complete()
    val = mfut.get_result()
    self.assertEqual(val, sum(range(10)))

  def testMultiFuture_PreCompleted(self):
    @tasks.task
    def foo():
      yield tasks.sleep(0.01)
    mfut = tasks.MultiFuture()
    dep = foo()
    dep.wait()
    mfut.add_dependent(dep)
    mfut.complete()
    self.assertTrue(mfut.done())
    self.assertEqual(mfut.get_result(), [dep])

  def testGetReturnValue(self):
      r0 = tasks.Return()
      r1 = tasks.Return(42)
      r2 = tasks.Return(42, 'hello')
      r3 = tasks.Return((1, 2, 3))
      self.assertEqual(tasks.get_return_value(r0), None)
      self.assertEqual(tasks.get_return_value(r1), 42)
      self.assertEqual(tasks.get_return_value(r2), (42, 'hello'))
      self.assertEqual(tasks.get_return_value(r3), (1, 2, 3))

  def testTasks_Basic(self):
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

  def testTasks_Raising(self):
    @tasks.task
    def t1():
      f = t2(True)
      try:
        a = yield f
      except RuntimeError, err:
        self.assertEqual(f.get_exception(), err)
        raise tasks.Return(str(err))
    @tasks.task
    def t2(error):
      if error:
        raise RuntimeError('hello')
      else:
        yield tasks.Future()
    x = t1()
    y = x.get_result()
    self.assertEqual(y, 'hello')

  def set_up_datastore(self):
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    stub = datastore_file_stub.DatastoreFileStub('_', None)
    apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', stub)

  def testTasks_YieldRpcs(self):
    self.set_up_datastore()
    conn = model.conn
    @tasks.task
    def main_task():
      rpc1 = conn.async_get(None, [])
      rpc2 = conn.async_put(None, [])
      res1 = yield rpc1
      res2 = yield rpc2
      raise tasks.Return(res1, res2)
    f = main_task()
    result = f.get_result()
    self.assertEqual(result, ([], []))

  def testTask_YieldTuple(self):
    @tasks.task
    def fib(n):
      if n <= 1:
        raise tasks.Return(n)
      a, b = yield fib(n - 1), fib(n - 2)
      # print 'fib(%r) = %r + %r = %r' % (n, a, b, a + b)
      raise tasks.Return(a + b)
    fut = fib(10)
    val = fut.get_result()
    self.assertEqual(val, 55)

class TracebackTests(unittest.TestCase):
  """Checks that errors result in reasonable tracebacks."""

  def testBasicError(self):
    frames = [sys._getframe()]
    @tasks.task
    def level3():
      frames.append(sys._getframe())
      raise RuntimeError('hello')
      yield
    @tasks.task
    def level2():
      frames.append(sys._getframe())
      yield level3()
    @tasks.task
    def level1():
      frames.append(sys._getframe())
      yield level2()
    @tasks.task
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
        # It's okay if some help_task_along frames are present.
        if tb.tb_frame.f_code.co_name != 'help_task_along':
          tbframes.append(tb.tb_frame)
        tb = tb.tb_next
      self.assertEqual(frames, tbframes)
    else:
      self.fail('Expected RuntimeError not raised')


def main():
  unittest.main()

if __name__ == '__main__':
  main()
