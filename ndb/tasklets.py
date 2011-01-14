"""A tasklet decorator.

Tasklets are a way to write concurrently running functions without
threads; tasklets are executed by an event loop and can suspend
themselves blocking for I/O or some other operation using a yield
statement.  The notion of a blocking operation is abstracted into the
Future class, but a tasklet may also yield an RPC in order to wait for
that RPC to complete.

The @tasklet decorator wraps generator function so that when it is
called, a Future is returned while the generator is executed by the
event loop.  For example:

  @tasklet
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
busy-waiting).  In most cases such code should be rewritten as a tasklet
instead:

  @tasklet
  def main_tasklet():
    f = foo()
    x = yield f
    print x

Calling a tasklet automatically schedules it with the event loop:

  def main():
    f = main_tasklet()
    eventloop.run()  # Run until no tasklets left to do
    assert f.done()

As a special feature, if the wrapped function is not a generator
function, its return value is returned via the Future.  This makes the
following two equivalent:

  @tasklet
  def foo():
    return 42

  @tasklet
  def foo():
    if False: yield  # The presence of 'yield' makes foo a generator
    raise Return(42)  # Or, after PEP 380, return 42

This feature (inspired by Monocle) is handy in case you are
implementing an interface that expects tasklets but you have no need to
suspend -- there's no need to insert a dummy yield in order to make
the tasklet into a generator.
"""

import collections
import logging
import os
import sys
import types

from google.appengine.api.apiproxy_stub_map import UserRPC
from google.appengine.api.apiproxy_rpc import RPC

from google.appengine.datastore import datastore_rpc
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

  _geninfo = None  # Extra info about suspended generator.

  def __init__(self, info=None):
    # TODO: Make done a method, to match PEP 3148?
    __ndb_debug__ = 'SKIP'  # Hide this frame from self._where
    self._info = info  # Info from the caller about this Future's purpose.
    self._where = utils.get_stack()
    self._context = None
    self._reset()

  def _reset(self):
    self._done = False
    self._result = None
    self._exception = None
    self._traceback = None
    self._callbacks = []
    logging.debug('_all_pending: add %s', self)
    self._all_pending.add(self)
    self._next = None  # Links suspended Futures together in a stack.

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
      if 'ndb/tasklets.py' not in line:
        break
    if self._info:
      line += ' for %s;' % self._info
    if self._geninfo:
      line += ' %s;' % self._geninfo
    return '<%s %x created by %s %s>' % (
      self.__class__.__name__, id(self), line, state)

  def dump(self):
    return '%s\nCreated by %s' % (self.dump_stack(),
                                  '\n called by '.join(self._where))

  def dump_stack(self):
    lines = []
    fut = self
    while fut is not None:
      lines.append(str(fut))
      fut = fut._next
    return '\n waiting for '.join(lines)

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
        line = fut.dump() + ('\n' + '-'*40)
      else:
        line = fut.dump_stack()
      all.append(line)
    return '\n'.join(all)

  def add_callback(self, callback, *args, **kwds):
    if self._done:
      eventloop.queue_call(None, callback, *args, **kwds)
    else:
      self._callbacks.append((callback, args, kwds))

  def set_result(self, result):
    assert not self._done
    self._result = result
    self._done = True
    logging.debug('_all_pending: remove successful %s', self)
    self._all_pending.remove(self)
    for callback, args, kwds  in self._callbacks:
      eventloop.queue_call(None, callback, *args, **kwds)

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
      eventloop.queue_call(None, callback, *args, **kwds)

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

  def _help_tasklet_along(self, gen, val=None, exc=None, tb=None):
    # XXX Docstring
    info = utils.gen_info(gen)
    __ndb_debug__ = info
    try:
      save_context = get_context()
      try:
        set_context(self._context)
        if exc is not None:
          logging.debug('Throwing %s(%s) into %s',
                        exc.__class__.__name__, exc, info)
          value = gen.throw(exc.__class__, exc, tb)
        else:
          logging.debug('Sending %r to %s', val, info)
          value = gen.send(val)
          self._context = get_context()
      finally:
        set_context(save_context)

    except StopIteration, err:
      result = get_return_value(err)
      logging.debug('%s returned %r', info, result)
      self.set_result(result)
      return

    except Exception, err:
      _, _, tb = sys.exc_info()
      logging.warning('%s raised %s(%s)',
                      info, err.__class__.__name__, err,
                      exc_info=(logging.getLogger().level <= logging.INFO))
      self.set_exception(err, tb)
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
        eventloop.queue_rpc(value, self._on_rpc_completion, value, gen)
        return
      if isinstance(value, Future):
        # TODO: Tail recursion if the Future is already done.
        assert not self._next, self._next
        self._next = value
        self._geninfo = utils.gen_info(gen)
        logging.debug('%s is now blocked waiting for %s', self, value)
        value.add_callback(self._on_future_completion, value, gen)
        return
      if isinstance(value, (tuple, list)):
        # Arrange for yield to return a list of results (not Futures).
        info = 'multi-yield from ' + utils.gen_info(gen)
        mfut = MultiFuture(info)
        for subfuture in value:
          mfut.add_dependent(subfuture)
        mfut.complete()
        mfut.add_callback(self._on_future_completion, mfut, gen)
        return
      if is_generator(value):
        assert False  # TODO: emulate PEP 380 here?
      assert False  # A tasklet shouldn't yield plain values.

  def _on_rpc_completion(self, rpc, gen):
    try:
      result = rpc.get_result()
    except Exception, err:
      _, _, tb = sys.exc_info()
      self._help_tasklet_along(gen, exc=err, tb=tb)
    else:
      self._help_tasklet_along(gen, result)

  def _on_future_completion(self, future, gen):
    if self._next is future:
      self._next = None
      self._geninfo = None
      logging.debug('%s is no longer blocked waiting for %s', self, future)
    exc = future.get_exception()
    if exc is not None:
      self._help_tasklet_along(gen, exc=exc, tb=future.get_traceback())
    else:
      val = future.get_result()  # This won't raise an exception.
      self._help_tasklet_along(gen, val)

def sleep(dt):
  """Public function to sleep some time.

  Example:
    yield tasklets.sleep(0.5)  # Sleep for half a sec.
  """
  fut = Future('sleep(%.3f)' % dt)
  eventloop.queue_call(dt, fut.set_result, None)
  return fut


class MultiFuture(Future):
  """A Future that depends on multiple other Futures.

  The protocol from the caller's POV is:

    mf = MultiFuture()
    mf.add_dependent(<some other Future>)  -OR- mf.putq(<some value>)
    mf.add_dependent(<some other Future>)  -OR- mf.putq(<some value>)
      .
      . (More mf.add_dependent() and/or mf.putq() calls)
      .
    mf.complete()  # No more dependents will be added.
      .
      . (Time passes)
      .
    results = mf.get_result()

  Now, results is a list of results from all dependent Futures in
  the order in which they were added.

  It is legal to add the same dependent multiple times.

  Callbacks can be added at any point.

  From a dependent Future POV, there's nothing to be done: a callback
  is automatically added to each dependent Future which will signal
  its completion to the MultiFuture.
  """

  def __init__(self, info=None):
    self._full = False
    self._dependents = set()
    self._results = []
    super(MultiFuture, self).__init__(info=info)

  def __repr__(self):
    # TODO: This may be invoked before __init__() returns,
    # from Future.__init__().  Beware.
    line = super(MultiFuture, self).__repr__()
    lines = [line]
    for fut in self._results:
      lines.append(fut.dump_stack().replace('\n', '\n  '))
    return '\n waiting for '.join(lines)

  # TODO: Rename this method?  (But to what?)
  def complete(self):
    assert not self._full
    self._full = True
    if not self._dependents:
      # TODO: How to get multiple exceptions?
      self.set_result([r.get_result() for r in self._results])

  def putq(self, value):
    if isinstance(value, Future):
      fut = value
    else:
      fut = Future()
      fut.set_result(value)
    self.add_dependent(fut)

  def add_dependent(self, fut):
    assert isinstance(fut, Future)
    assert not self._full
    self._results.append(fut)
    if fut not in self._dependents:
      self._dependents.add(fut)
      fut.add_callback(self._signal_dependent_done, fut)

  def _signal_dependent_done(self, fut):
    self._dependents.remove(fut)
    if self._full and not self._dependents:
      # TODO: How to get multiple exceptions?
      self.set_result([r.get_result() for r in self._results])


class QueueFuture(Future):
  """A Queue following the same protocol as MultiFuture.

  However, instead of returning results as a list, it lets you
  retrieve results as soon as they are ready, one at a time, using
  getq().  The Future itself finishes with a result of None when the
  last result is ready (regardless of whether it was retrieved).

  The getq() method returns a Future which blocks until the next
  result is ready, and then returns that result.  Each getq() call
  retrieves one unique result.  Extra getq() calls after the last
  result is already returned return EOFError as their Future's
  exception.  (I.e., q.getq() returns a Future as always, but yieding
  that Future raises EOFError.)

  NOTE: If .getq() is given a default argument, it will be returned as
  the result instead of raising EOFError.  However, other exceptions
  are still passed through.

  NOTE: Values can also be pushed directly via .putq(value).  However
  there is no flow control -- if the producer is faster than the
  consumer, the queue will grow unbounded.
  """
  # TODO: Refactor to share code with MultiFuture.

  # TODO: Kill getq(default) or add it uniformly.
  _RAISE_ERROR = object()  # Marker for getq() default value.

  def __init__(self, info=None):
    self._full = False
    self._dependents = set()
    self._completed = list()
    self._waiting = list()  # List of (Future, default) tuples.
    super(QueueFuture, self).__init__(info=info)

  # TODO: __repr__

  def complete(self):
    assert not self._full
    self._full = True
    if not self._dependents:
      self._mark_finished()

  def putq(self, value):
    if isinstance(value, Future):
      fut = value
    else:
      fut = Future()
      fut.set_result(value)
    self.add_dependent(fut)

  def add_dependent(self, fut):
    assert isinstance(fut, Future)
    assert not self._full
    if fut not in self._dependents:
      self._dependents.add(fut)
      fut.add_callback(self._signal_dependent_done, fut)

  def _signal_dependent_done(self, fut):
    assert fut.done()
    self._dependents.remove(fut)
    exc = fut.get_exception()
    tb = fut.get_traceback()
    val = None
    if exc is None:
      val = fut.get_result()
    if self._waiting:
      waiter, default = self._waiting.pop(0)
      self._pass_result(waiter, exc, tb, val)
    else:
      self._completed.append((exc, tb, val))
    if self._full and not self._dependents:
      self._mark_finished()

  def _mark_finished(self):
    waiting = self._waiting[:]
    del self._waiting[:]
    for waiter, default in waiting:
      self._pass_eof(waiter, default)
    self.set_result(None)

  def getq(self, default=_RAISE_ERROR):
    # The default is only used when EOFError is raised.
    fut = Future()
    if self._completed:
      exc, tb, val = self._completed.pop(0)
      self._pass_result(fut, exc, tb, val)
    elif self._full and not self._dependents:
      self._pass_eof(fut, default)
    else:
      self._waiting.append((fut, default))
    return fut

  def _pass_eof(self, fut, default):
    if default is self._RAISE_ERROR:
      self._pass_result(fut, EOFError('Queue is empty'), None, None)
    else:
      self._pass_result(fut, None, None, default)

  def _pass_result(self, fut, exc, tb, val):
      if exc is not None:
        fut.set_exception(exc, tb)
      else:
        fut.set_result(val)


class SerialQueueFuture(Future):
  """Like QueueFuture but maintains the order of insertion."""

  def __init__(self, info=None):
    self._full = False
    self._queue = collections.deque()
    self._waiting = collections.deque()
    # Invariant: at least one of _queue and _waiting is empty.
    super(SerialQueueFuture, self).__init__(info=info)

  # TODO: __repr__

  def complete(self):
    assert not self._full
    self._full = True
    while self._waiting:
      waiter = self._waiting.popleft()
      waiter.set_exception(EOFError('Queue is empty'))
    if not self._queue:
      self.set_result(None)

  def putq(self, value):
    if isinstance(value, Future):
      fut = value
    else:
      if self._waiting:
        waiter = self._waiting.popleft()
        waiter.set_result(value)
        return
      fut = Future()
      fut.set_result(value)
    self.add_dependent(fut)

  def add_dependent(self, fut):
    assert isinstance(fut, Future)
    assert not self._full
    if self._waiting:
      waiter = self._waiting.popleft()
      # TODO: Transfer errors too.
      fut.add_callback(lambda: waiter.set_result(fut.get_result()))
    else:
      self._queue.append(fut)

  def getq(self):
    if self._queue:
      fut = self._queue.popleft()
    else:
      fut = Future()
      if self._full:
        fut.set_exception(EOFError('Queue is empty'))
      else:
        self._waiting.append(fut)
    if self._full and not self.done():
      self.set_result(None)
    return fut


class ReducingFuture(Future):
  """A Queue following the same protocol as MultiFuture.

  However the result, instead of being a list of results of dependent
  Futures, is computed by calling a 'reducer' tasklet.  The reducer tasklet
  takes a list of values and returns a single value.  It may be called
  multiple times on sublists of values and should behave like
  e.g. sum().

  NOTE: The reducer input values may be reordered compared to the
  order in which they were added to the queue.
  """
  # TODO: Refactor to reuse some code with MultiFuture.

  def __init__(self, reducer, info=None, batch_size=20):
    self._reducer = reducer
    self._batch_size = batch_size
    self._full = False
    self._dependents = set()
    self._completed = list()
    self._queue = list()
    super(ReducingFuture, self).__init__(info=info)

  # TODO: __repr__

  def complete(self):
    assert not self._full
    self._full = True
    if not self._dependents:
      self._mark_finished()

  def putq(self, value):
    if isinstance(value, Future):
      fut = value
    else:
      fut = Future()
      fut.set_result(value)
    self.add_dependent(fut)

  def add_dependent(self, fut):
    assert not self._full
    self._internal_add_dependent(fut)

  def _internal_add_dependent(self, fut):
    assert isinstance(fut, Future)
    if fut not in self._dependents:
      self._dependents.add(fut)
      fut.add_callback(self._signal_dependent_done, fut)

  def _signal_dependent_done(self, fut):
    assert fut.done()
    self._dependents.remove(fut)
    val = fut.get_result()  # TODO: What about exceptions here?
    self._queue.append(val)
    if len(self._queue) >= self._batch_size:
      todo = self._queue[:]
      del self._queue[:]
      nval = self._reducer(todo)  # TODO: What if exception?
      if isinstance(nval, Future):
        self._internal_add_dependent(nval)
      else:
        self._queue.append(nval)
    if self._full and not self._dependents:
      self._mark_finished()

  def _mark_finished(self):
    if not self._queue:
      self.set_result(None)
    elif len(self._queue) == 1:
      self.set_result(self._queue.pop())
    else:
      todo = self._queue[:]
      del self._queue[:]
      nval = self._reducer(todo)  # TODO: What if exception?
      if isinstance(nval, Future):
        self._internal_add_dependent(nval)
      else:
        self.set_result(nval)


# Alias for StopIteration used to mark return values.
# To use this, raise Return(<your return value>).  The semantics
# are exactly the same as raise StopIteration(<your return value>)
# but using Return clarifies that you are intending this to be the
# return value of a tasklet.
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

def tasklet(func):
  # XXX Docstring

  @utils.wrapping(func)
  def tasklet_wrapper(*args, **kwds):
    # XXX Docstring

    # TODO: make most of this a public function so you can take a bare
    # generator and turn it into a tasklet dynamically.  (Monocle has
    # this I believe.)
    # __ndb_debug__ = utils.func_info(func)
    fut = Future('tasklet %s' % utils.func_info(func))
    fut._context = get_context()
    try:
      result = func(*args, **kwds)
    except StopIteration, err:
      # Just in case the function is not a generator but still uses
      # the "raise Return(...)" idiom, we'll extract the return value.
      result = get_return_value(err)
    if is_generator(result):
      eventloop.queue_call(None, fut._help_tasklet_along, result)
    else:
      fut.set_result(result)
    return fut

  return tasklet_wrapper

def synctasklet(func):
  """Decorator to run a function as a tasklet when called.

  Use this to wrap a request handler function that will be called by
  some web application framework (e.g. a Django view function or a
  webapp.RequestHandler.get method).
  """
  @utils.wrapping(func)
  def synctasklet_wrapper(*args):
    __ndb_debug__ = utils.func_info(func)
    taskletfunc = tasklet(func)
    return taskletfunc(*args).get_result()
  return synctasklet_wrapper


_CONTEXT_KEY = '__CONTEXT__'

# TODO: Use thread-local for this.
_context = None

def get_context():
  global _context
  ctx = None
  if os.getenv(_CONTEXT_KEY):
    ctx = _context
  if ctx is None:
    ctx = make_default_context()
    set_context(ctx)
  return ctx

def make_default_context():
  import context  # Late import to deal with circular imports.
  return context.Context()

def set_context(new_context):
  global _context
  os.environ[_CONTEXT_KEY] = '1'
  _context = new_context

# TODO: Rework the following into documentation.

# A tasklet/coroutine/generator can yield the following things:
# - Another tasklet/coroutine/generator; this is entirely equivalent to
#   "for x in g: yield x"; this is handled entirely by the @tasklet wrapper.
#   (Actually, not.  @tasklet returns a function that when called returns
#   a Future.  You can use the pep380 module's @gwrap decorator to support
#   yielding bare generators though.)
# - An RPC (or MultiRpc); the tasklet will be resumed when this completes.
#   This does not use the RPC's callback mechanism.
# - A Future; the tasklet will be resumed when the Future is done.
#   This uses the Future's callback mechanism.

# A Future can be used in several ways:
# - Yield it from a tasklet; see above.
# - Check (poll) its status via f.done.
# - Call its wait() method, perhaps indirectly via check_success()
#   or get_result().  This invokes the event loop.
# - Call the Future.wait_any() or Future.wait_all() method.
#   This is waits for any or all Futures and RPCs in the argument list.

# XXX HIRO XXX

# - A tasklet is a (generator) function decorated with @tasklet.

# - Calling a tasklet schedules the function for execution and returns a Future.

# - A function implementing a tasklet may:
#   = yield a Future; this waits for the Future which returns f.get_result();
#   = yield an RPC; this waits for the RPC and then returns rpc.get_result();
#   = raise Return(result); this sets the outer Future's result;
#   = raise StopIteration or return; this sets the outer Future's result;
#   = raise another exception: this sets the outer Future's exception.

# - If a function implementing a tasklet is not a generator it will be
#   immediately executed to completion and the tasklet wrapper will
#   return a Future that is already done.  (XXX Alternative behavior:
#   it schedules the call to be run by the event loop.)

# - Code not running in a tasklet can call f.get_result() or f.wait() on
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
#   implementing a tasklet yields, raises or returns, there is always a
#   wrapper that catches this event and either turns it into a
#   callback sent to the event loop, or sets the result or exception
#   for the tasklet's Future.
