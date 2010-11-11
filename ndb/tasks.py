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

Calling a task automatically schedules it with the event loop:

  def main():
    f = main_task()
    eventloop.run()  # Run until no tasks left to do
    assert f.done()

As a special feature, if the wrapped function is not a generator
function, its return value is returned via the Future.  This makes the
following two equivalent:

  @task
  def foo():
    return 42

  @task
  def foo():
    if False: yield  # The presence of 'yield' makes foo a generator
    raise Return(42)  # Or, after PEP 380, return 42

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
from ndb import eventloop, utils

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
  # TODO: Trim the API; there are too many ways to do the same thing.
  # TODO: Compare to Monocle's much simpler Callback class.

  # Constants for state property.
  IDLE = RPC.IDLE  # Not yet running (unused)
  RUNNING = RPC.RUNNING  # Not yet completed.
  FINISHING = RPC.FINISHING  # Completed.

  _all_pending = set()  # Set of all pending Future instances.

  # XXX Add docstrings to all methods.  Separate PEP 3148 API from RPC API.

  def __init__(self, info=None):
    # TODO: Make done a method, to match PEP 3148?
    __ndb_debug__ = 'SKIP'  # Hide this frame from self._where
    self._info = info  # Info from the caller about this Future's purpose.
    self._done = False
    self._result = None
    self._exception = None
    self._traceback = None
    self._callbacks = []
    self._where = utils.get_stack()
    logging.debug('_all_pending: add %s', self)
    self._all_pending.add(self)

  # TODO: Add a __del__ that complains if neither get_exception() nor
  # check_success() was ever called?  What if it's not even done?

  def __repr__(self):
    if self._done:
      if self._exception is not None:
        state = 'exception %s: %s' % (self._exception.__class__.__name__,
                                   self._exception)
      else:
        state = 'result %r' % (self._result,)
    else:
      state = 'pending'
    line = '?'
    for line in self._where:
      if 'ndb/tasks.py' not in line:
        break
    if self._info:
      line += ' for %s;' % self._info
    return '<%s %x created by %s %s>' % (
      self.__class__.__name__, id(self), line, state)

  def dump(self):
    return '%s\nCreated by %s' % (self, '\n called by '.join(self._where))

  @classmethod
  def clear_all_pending(cls):
    if cls._all_pending:
      logging.info('_all_pending: clear %s', cls._all_pending)
    else:
      logging.debug('_all_pending: clear no-op')
    cls._all_pending.clear()

  @classmethod
  def dump_all_pending(cls, verbose=False):
    all = []
    for fut in cls._all_pending:
      if verbose:
        line = fut.dump()
      else:
        line = str(fut)
      all.append(line)
    return '\n'.join(all)

  def add_callback(self, callback, *args, **kwds):
    if self._done:
      eventloop.queue_task(None, callback, *args, **kwds)
    else:
      self._callbacks.append((callback, args, kwds))

  def set_result(self, result):
    assert not self._done
    self._result = result
    self._done = True
    logging.debug('_all_pending: remove successful %s', self)
    self._all_pending.remove(self)
    for callback, args, kwds  in self._callbacks:
      eventloop.queue_task(None, callback, *args, **kwds)

  def set_exception(self, exc, tb=None):
    assert isinstance(exc, BaseException)
    assert not self._done
    self._exception = exc
    self._traceback = tb
    self._done = True
    if self in self._all_pending:
      logging.debug('_all_pending: remove failing %s', self)
      self._all_pending.remove(self)
    else:
      logging.debug('_all_pending: not found %s', self)
    for callback, args, kwds in self._callbacks:
      eventloop.queue_task(None, callback, *args, **kwds)

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
      if not ev.run1():
        logging.info('Deadlock in %s', self)
        logging.info('All pending Futures:\n%s', self.dump_all_pending())
        if logging.getLogger().level <= logging.DEBUG:
          logging.debug('All pending Futures (verbose):\n%s',
                        self.dump_all_pending(verbose=True))
        self.set_exception(RuntimeError('Deadlock waiting for %s' % self))

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

  # TODO: s/fut/self/

  def _help_task_along(fut, gen, val=None, exc=None, tb=None):
    # XXX Docstring
    info = utils.gen_info(gen)
    __ndb_debug__ = info
    try:
      if exc is not None:
        logging.debug('Throwing %s(%s) into %s',
                      exc.__class__.__name__, exc, info)
        value = gen.throw(exc.__class__, exc, tb)
      else:
        logging.debug('Sending %r to %s', val, info)
        value = gen.send(val)

    except StopIteration, err:
      result = get_return_value(err)
      logging.debug('%s returned %r', info, result)
      fut.set_result(result)
      return

    except Exception, err:
      _, _, tb = sys.exc_info()
      logging.debug('%s raised %s(%s)', info, err.__class__.__name__, err)
      fut.set_exception(err, tb)
      return

    else:
      logging.debug('%s yielded %r', info, value)
      if isinstance(value, datastore_rpc.MultiRpc):
        # TODO: Tail recursion if the RPC is already complete.
        if len(value.rpcs) == 1:
          value = value.rpcs[0]
          # Fall through to next isinstance test.
        else:
          assert False  # TODO: Support MultiRpc using MultiFuture.
      if isinstance(value, UserRPC):
        # TODO: Tail recursion if the RPC is already complete.
        eventloop.queue_rpc(value, fut._on_rpc_completion, value, gen)
        return
      if isinstance(value, Future):
        # TODO: Tail recursion if the Future is already done.
        value.add_callback(fut._on_future_completion, value, gen)
        return
      if isinstance(value, (tuple, list)):
        # Arrange for yield to return a list of results (not Futures).
        # Since the subfutures may not finish in the given order, we
        # keep track of the indexes separately.  (We can't store the
        # indexes on the Futures because the same Future may be involved
        # in multiple yields simultaneously.)
        # TODO: Maybe not use MultiFuture, or do it another way?
        indexes = {}
        for index, subfuture in enumerate(value):
          indexes[subfuture] = index
        def reducer(state, subfuture):
          # TODO: If any of the Futures has an exception, things go bad.
          state[indexes[subfuture]] = subfuture.get_result()
          return state
        mfut = MultiFuture('yield %d items' % len(value), reducer, [None] * len(value))
        for subfuture in value:
          mfut.add_dependent(subfuture)
        mfut.complete()
        mfut.add_callback(fut._on_future_completion, mfut, gen)
        return
      if is_generator(value):
        assert False  # TODO: emulate PEP 380 here?
      assert False  # A task shouldn't yield plain values.

  def _on_rpc_completion(fut, rpc, gen):
    try:
      result = rpc.get_result()
    except Exception, err:
      _, _, tb = sys.exc_info()
      fut._help_task_along(gen, exc=err, tb=tb)
    else:
      fut._help_task_along(gen, result)

  def _on_future_completion(fut, future, gen):
    exc = future.get_exception()
    if exc is not None:
      fut._help_task_along(gen, exc=exc, tb=future.get_traceback())
    else:
      val = future.get_result()  # This better not raise an exception.
      fut._help_task_along(gen, val)

def sleep(dt):
  """Public function to sleep some time.

  Example:
    yield tasks.sleep(0.5)  # Sleep for half a sec.
  """
  fut = Future('sleep(%.3f)' % dt)
  eventloop.queue_task(dt, fut.set_result, None)
  return fut

class MultiFuture(Future):
  """A Future that depends on multiple other Futures.

  The protocol from the caller's POV is:

    mf = MultiFuture()
    mf.add_dependent(<some other Future>)
    mf.add_dependent(<some other Future>)
      .
      . (More mf.add_dependent() calls)
      .
    mf.complete()  # No more dependents will be added.
      .
      . (Time passes)
      .
    completed = mf.get_result()

  Now, completed is a list of all dependent Futures in the order in
  which they completed.  (TODO: Is this the best result value?)

  Adding the same dependent multiple times is a no-op.

  Callbacks can be added at any point.

  From a dependent Future POV, there's nothing to be done: a callback
  is automatically added to each dependent Future which will signal
  its completion to the MultiFuture.
  """

  # TODO: Make this return the list of dependents in the order ADDED.
  # TODO: Do we really need reducer and initial?

  def __init__(self, info=None, reducer=None, initial=None):
    self._full = False
    self._dependents = set()
    super(MultiFuture, self).__init__(info)
    self._reducer = reducer
    self._result = initial
    if reducer is None:
      assert initial is None, initial  # If no reducer, can't use initial.
      self._result = []

  def __repr__(self):
    # TODO: This may be invoked before __init__() returns,
    # from Future.__init__().  Beware.
    line = super(MultiFuture, self).__repr__()
    if self._full:
      line = line[:1] + 'Full ' + line[1:]
    lines = [line]
    for fut in self._dependents:
      lines.append(repr(fut))
    return '\n waiting for '.join(lines)

  # TODO: Rename this method?  (But to what?)
  def complete(self):
    assert not self._full
    self._full = True
    if not self._dependents:
      self.set_result(self._result)

  def add_dependent(self, fut):
    assert isinstance(fut, Future)
    assert not self._full
    if fut not in self._dependents:
      self._dependents.add(fut)
      fut.add_callback(self.signal_dependent_done, fut)

  def signal_dependent_done(self, fut):
    self.process_value(fut)
    self._dependents.remove(fut)
    if self._full and not self._dependents:
      self.set_result(self._result)

  def process_value(self, val):
    # Typically, val is a Future; but this can also be called directly.
    if self._reducer is None:
      self._result.append(val)
    else:
      self._result = self._reducer(self._result, val)

# Alias for StopIteration used to mark return values.
# To use this, raise Return(<your return value>).  The semantics
# are exactly the same as raise StopIteration(<your return value>)
# but using Return clarifies that you are intending this to be the
# return value of a task.
# TODO: According to Monocle authors Steve and Greg Hazel, Twisted
# used an exception to signal a return value from a generator early
# on, and they found out it was error-prone.  Should I worry?
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

  @utils.wrapping(func)
  def task_wrapper(*args, **kwds):
    # XXX Docstring

    # TODO: make most of this a public function so you can take a bare
    # generator and turn it into a task dynamically.  (Monocle has
    # this I believe.)
    # __ndb_debug__ = utils.func_info(func)
    fut = Future('task %s' % utils.func_info(func))
    try:
      result = func(*args, **kwds)
    except StopIteration, err:
      # Just in case the function is not a generator but still uses
      # the "raise Return(...)" idiom, we'll extract the return value.
      result = get_return_value(err)
    if is_generator(result):
      eventloop.queue_task(None, fut._help_task_along, result)
    else:
      fut.set_result(result)
    return fut

  return task_wrapper

def taskify(func):
  """Decorator to run a function as a task when called.

  Use this to wrap a request handler function that will be called by
  some web application framework (e.g. a Django view function or a
  webapp.RequestHandler.get method).
  """
  @utils.wrapping(func)
  def taskify_wrapper(*args):
    __ndb_debug__ = utils.func_info(func)
    taskfunc = task(func)
    return taskfunc(*args).get_result()
  return taskify_wrapper

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
