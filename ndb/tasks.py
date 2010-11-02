"""A task decorator.

Tasks are a way to write concurrently running functions without
threads; tasks are executed by an event loop and can suspend
themselves blocking for I/O or some other operation using a yield
statement.  The notion of a blocking operation is abstracted into the
Future class, but a task may also yield an RPC in order to wait for
that RPC to complete.

The @task decorator wraps generator function so that when it is
called, a Future is returned while the generator is executed by the
event loop.  For example:

  @task
  def foo():
    a = yield <some Future>
    c = yield <another Future>
    raise Return(a + b)

  def main():
    f = foo()
    x = f.get_result()
    print x

Note that blocking until the Future's result is available using
get_result() is somewhat inefficient (though not vastly -- it is not
busy-waiting).  In most cases such code should be rewritten as a task
instead:

  @task
  def main_task():
    f = foo()
    x = yield f
    print x

Tasks can be scheduled using the event loop:

  def main():
    eventloop.queue_task(0, main_task)  # Calls foo after 0 seconds delay
    eventloop.run()  # Run until no tasks left to do

As a special feature, if the wrapped function is not a generator
function, its return value is returned via the Future.  This makes the
following two equivalent:

  @task
  def foo():
    return 42

  @task
  def foo():
    if False: yield  # The presence of 'yield' makes foo a generator
    raise StopIteration(42)  # Or, after PEP 380, return 42

This feature (inspired by Monocle) is handy in case you are
implementing an interface that expects tasks but you have no need to
suspend -- there's no need to insert a dummy yield in order to make
the task into a generator.
"""

import logging
import os
import sys
import types

from google.appengine.api.apiproxy_stub_map import UserRPC
from google.appengine.api.apiproxy_rpc import RPC

from core import datastore_rpc
from ndb import eventloop

def is_generator(obj):
  """Helper to test for a generator object.

  NOTE: This tests for the (iterable) object returned by calling a
  generator function, not for a generator function.
  """
  return isinstance(obj, types.GeneratorType)

class Future(object):
  """A Future has 0 or more callbacks.

  The callbacks will be called when the result is ready.

  NOTE: This is somewhat inspired but not conformant to the Future interface
  defined by PEP 3148.  It is also inspired (and tries to be somewhat
  compatible with) the App Engine specific UserRPC and MultiRpc classes.
  """

  # Constants for state property.
  IDLE = RPC.IDLE  # Not yet running (unused)
  RUNNING = RPC.RUNNING  # Not yet completed.
  FINISHING = RPC.FINISHING  # Completed.

  # XXX Add docstrings to all methods.  Separate PEP 3148 API from RPC API.

  def __init__(self):
    # TODO: Make done a method, to match PEP 3148?
    self._done = False
    self._result = None
    self._exception = None
    self._traceback = None
    self._callbacks = []
    frame = sys._getframe(1)
    code = frame.f_code
    self._lineno = frame.f_lineno
    self._filename = code.co_filename
    self._funcname = code.co_name

  def __repr__(self):
    if self._done:
      if self._exception is not None:
        state = 'exception %s: %s' % (self._exception.__class__.__name__,
                                   self._exception)
      else:
        state = 'result %r' % self._result
    else:
      state = 'pending'
    return '<%s %x created by %s(%s:%s) %s>' % (
      self.__class__.__name__, id(self),
      self._funcname, os.path.basename(self._filename),
      self._lineno, state)

  def add_done_callback(self, callback):
    if self._done:
      callback(self)
    else:
      self._callbacks.append(callback)

  def set_result(self, result):
    assert not self._done
    self._result = result
    self._done = True
    for callback in self._callbacks:
      callback(self)  # TODO: What if it raises an exception?

  def set_exception(self, exc, tb=None):
    assert isinstance(exc, BaseException)
    assert not self._done
    self._exception = exc
    self._traceback = tb
    self._done = True
    for callback in self._callbacks:
      callback(self)

  def done(self):
    return self._done

  @property
  def state(self):
    # This is just for compatibility with UserRPC and MultiRpc.
    # A Future is considered running as soon as it is created.
    if self._done:
      return self.FINISHING
    else:
      return self.RUNNING

  def wait(self):
    if self._done:
      return
    ev = eventloop.get_event_loop()
    while not self._done:
      ev.run1()

  def get_exception(self):
    self.wait()
    return self._exception

  def get_traceback(self):
    self.wait()
    return self._traceback

  def check_success(self):
    self.wait()
    if self._exception is not None:
      raise self._exception.__class__, self._exception, self._traceback

  def get_result(self):
    self.check_success()
    return self._result

  @classmethod
  def wait_any(cls, futures):
    # TODO: Flatten MultiRpcs.
    all = set(futures)
    ev = eventloop.get_event_loop()
    while all:
      for f in all:
        if f.state == cls.FINISHING:
          return f
      ev.run1()
    return None

  @classmethod
  def wait_all(cls, futures):
    # TODO: Flatten MultiRpcs.
    all = set(futures)
    ev = eventloop.get_event_loop()
    while all:
      all = set(f for f in all if f.state == cls.RUNNING)
      ev.run1()

def sleep(dt):
  """Public function to sleep some time.

  Example:
    yield tasks.sleep(0.5)  # Sleep for half a sec.
  """
  fut = Future()
  eventloop.queue_task(dt, fut.set_result, None)
  return fut

# Alias for StopIteration used to mark return values.
# To use this, raise Return(<your return value>).  The semantics
# are exactly the same as raise StopIteration(<your return value>)
# but using Return clarifies that you are intending this to be the
# return value of a task.
Return = StopIteration

def get_return_value(err):
  # XXX Docstring
  if not err.args:
    result = None
  elif len(err.args) == 1:
    result = err.args[0]
  else:
    result = err.args
  return result

def task(func):
  # XXX Docstring

  def task_wrapper(*args, **kwds):
    # XXX Docstring
    fut = Future()
    try:
      result = func(*args, **kwds)
    except StopIteration, err:
      result = get_return_value(err)
    if is_generator(result):
      eventloop.queue_task(0, help_task_along, result, fut)
    else:
      fut.set_result(result)
    return fut

  return task_wrapper

def help_task_along(gen, fut, val=None, exc=None, tb=None):
  # XXX Docstring
  try:
    if exc is not None:
      value = gen.throw(exc.__class__, exc, tb)
    else:
      value = gen.send(val)

  except StopIteration, err:
    result = get_return_value(err)
    fut.set_result(result)
    return

  except Exception, err:
    _, _, tb = sys.exc_info()
    logging.exception('help_task_along: task raised an exception')
    fut.set_exception(err, tb)
    return

  else:
    if isinstance(value, datastore_rpc.MultiRpc):
      # TODO: Tail recursion if the RPC is already complete.
      if len(value.rpcs) == 1:
        value = value.rpcs[0]
        # Fall through to next isinstance test.
      else:
        assert False  # TODO: Support MultiRpc using MultiFuture.
    if isinstance(value, UserRPC):
      # TODO: Tail recursion if the RPC is already complete.
      eventloop.queue_rpc(value, on_rpc_completion, value, gen, fut)
      return
    if isinstance(value, Future):
      # TODO: Tail recursion if the Future is already done.
      value.add_done_callback(
          lambda val: on_future_completion(val, gen, fut))
      return
    if is_generator(value):
      assert False  # TODO: emulate PEP 380 here?
    assert False  # A task shouldn't yield plain values.

def on_rpc_completion(rpc, gen, fut):
  try:
    result = rpc.get_result()
  except Exception, err:
    _, _, tb = sys.exc_info()
    help_task_along(gen, fut, exc=err, tb=tb)
  else:
    help_task_along(gen, fut, result)

def on_future_completion(future, gen, fut):
  exc = future.get_exception()
  if exc is not None:
    help_task_along(gen, fut, exc=exc, tb=future.get_traceback())
  else:
    val = future.get_result()  # This better not raise an exception.
    help_task_along(gen, fut, val)

# TODO: Rework the following into documentation.

# A task/coroutine/generator can yield the following things:
# - Another task/coroutine/generator; this is entirely equivalent to
#   "for x in g: yield x"; this is handled entirely by the @task wrapper.
#   (Actually, not.  @task returns a function that when called returns
#   a Future.  You can use the pep380 module's @gwrap decorator to support
#   yielding bare generators though.)
# - An RPC (or MultiRpc); the task will be resumed when this completes.
#   This does not use the RPC's callback mechanism.
# - A Future; the task will be resumed when the Future is done.
#   This uses the Future's callback mechanism.

# A Future can be used in several ways:
# - Yield it from a task; see above.
# - Check (poll) its status via f.done.
# - Call its wait() method, perhaps indirectly via check_success()
#   or get_result().  This invokes the event loop.
# - Call the Future.wait_any() or Future.wait_all() method.
#   This is waits for any or all Futures and RPCs in the argument list.

# XXX HIRO XXX

# - A task is a (generator) function decorated with @task.

# - Calling a task schedules the function for execution and returns a Future.

# - A function implementing a task may:
#   = yield a Future; this waits for the Future which returns f.get_result();
#   = yield an RPC; this waits for the RPC and then returns rpc.get_result();
#   = raise Return(result); this sets the outer Future's result;
#   = raise StopIteration or return; this sets the outer Future's result;
#   = raise another exception: this sets the outer Future's exception.

# - If a function implementing a task is not a generator it will be
#   immediately executed to completion and the task wrapper will
#   return a Future that is already done.  (XXX Alternative behavior:
#   it schedules the call to be run by the event loop.)

# - Code not running in a task can call f.get_result() or f.wait() on
#   a future.  This is implemented by a simple loop like the following:

#     while not self.done:
#       eventloop.run1()

# - Here eventloop.run1() runs one "atomic" part of the event loop:
#   = either it calls one immediately ready callback;
#   = or it waits for the first RPC to complete;
#   = or it sleeps until the first callback should be ready;
#   = or it raises an exception indicating all queues are empty.

# - It is possible but suboptimal to call rpc.get_result() or
#   rpc.wait() directly on an RPC object since this will not allow
#   other callbacks to run as they become ready.  Wrapping an RPC in a
#   Future will take care of this issue.

# - The important insight is that when a generator function
#   implementing a task yields, raises or returns, there is always a
#   wrapper that catches this event and either turns it into a
#   callback sent to the event loop, or sets the result or exception
#   for the task's Future.
