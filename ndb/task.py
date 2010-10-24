"""A task decorator.

This should wrap a generator, e.g.:

  @task
  def foo():
    yield <some future>
    yield <another future>
    raise StopIteration(42)

Now calling foo() is equivalent to scheduler.schedule(foo()), and returns
a future that will produce 42 once foo() completes.

As a special feature, if foo() is not a generator, it will be run till
completion and the return value returned via a future.  This is to make
the following two equivalent:

  @task
  def foo():
    return 42

  @task
  def foo():
    if False: yield
    raise StopIteration(42)  # Or, after PEP 380, return 42

(This idea taken from Monocle.)

(How does Monocle do the trampoline?  Does it use the Twisted reactor?)
"""

import sys
import types

from google.appengine.api.apiproxy_stub_map import UserRPC

from ndb import eventloop

def is_generator(obj):
  return isinstance(obj, types.GeneratorType)

class Future(object):
  """A Future has 0 or more callbacks.

  The callbacks will be called when the result is ready.

  NOTE: This is somewhat inspired but not conformant to the Future interface
  defined by PEP 3148.  It is also inspired (and tries to be somewhat
  compatible with) the App Engine specific UserRPC and MultiRpc classes.
  """

  def __init__(self):
    self.done = False
    self.result = None
    self.exception = None
    self.callbacks = []

  def add_done_callback(self, callback):
    if self.done:
      callback(self)
    else:
      self.callbacks.append(callback)

  def set_result(self, result):
    assert not self.done
    self.result = result
    self.done = True
    for callback in self.callbacks:
      callback(self)  # TODO: What if it raises an exception?

  def set_exception(self, exc):
    # TODO: What about tracebacks?
    assert isinstance(exc, BaseException)
    assert not self.done
    self.exception = exc
    self.done = True
    for callback in self.callbacks:
      callback(self)

  @property
  def state(self):
    # This is just for compatibility with UserRPC and MultiRpc.
    # A Future is considered running as soon as it is created.
    if self.done:
      return 2  # FINISHING
    else:
      return 1  # RUNNING

  def wait(self):
    if self.done:
      return
    ev = eventloop.get_event_loop()
    while not self.done:
      ev.run1()

  def get_exception(self):
    self.wait()
    return self.exception

  def check_success(self):
    self.wait()
    if self.exception is not None:
      raise self.exception

  def get_result(self):
    self.check_success()
    return self.result

  @classmethod
  def wait_any(cls, futures):
    all = set(futures)
    for f in all:
      if f.state == 2:
        return f
    assert False, 'XXX what to do now?'

  @classmethod
  def wait_all(cls, futures):
    todo = set(futures)
    while todo:
      f = cls.wait_any(todo)
      if f is not None:
        todo.remove(f)

class Return(StopIteration):
  """Trivial StopIteration class, used to mark return values.

  To use this, raise Return(<your return value>).  The semantics
  are exactly the same as raise StopIteration(<your return value>)
  but using Return clarifies that you are intending this to be the
  return value of a coroutine.
  """

def task(func):
  """XXX Docstring"""

  def task_wrapper(*args, **kwds):
    """XXX Docstring"""
    fut = Future()
    try:
      result = func(*args, **kwds)
    except StopIteration, err:
      result = get_value(err)
    if is_generator(result):
      eventloop.queue_task(0, help_task_along, result, fut)
    else:
      fut.set_result(result)
    return fut

  return task_wrapper

def get_value(err):
  if not err.args:
    result = None
  elif len(err.args) == 1:
    result = err.args[0]
  else:
    result = err.args
  return result

def help_task_along(gen, fut, val=None, exc=None):
  """XXX Docstring"""
  try:
    if exc is not None:
      value = gen.throw(exc)
    else:
      value = gen.send(val)

  except StopIteration, err:
    result = get_value(err)
    fut.set_result(result)
    return

  except Exception, err:
    fut.set_exception(err)
    return

  else:
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
      assert False  # TODO: emulate PEP 380 here.
    assert False  # A task shouldn't yield plain values.

def on_rpc_completion(rpc, gen, fut):
  try:
    result = rpc.get_result()
  except Exception, err:
    help_task_along(gen, fut, exc=err)
  else:
    help_task_along(gen, fut, result)

def on_future_completion(future, gen, fut):
  exc = future.get_exception()
  if exc is not None:
    help_task_along(gen, fut, exc=exc)
  else:
    val = future.get_result()  # This better not raise an exception.
    help_task_along(gen, fut, val)

# XXX Where to put the trampoline code?  Yielding a generator ought to
# schedule that generator -- I've got code in the old NDB to do that.
# The important lesson from Monocle is to separate the trampoline code
# from the event loop.  In particular when PEP 380 goes live (alas,
# not before Python 3.3), the trampoline will no longer be necessary,
# but it has no bearing on the event loop.  I hope.

# XXX A task/coroutine/generator can yield the following things:
# - Another task/coroutine/generator; this is entirely equivalent to
#   "for x in g: yield x"; this is handled entirely by the @task wrapper.
# - An RPC (or MultiRpc); it will be resumed when this completes;
#   this does not use the RPC's callback mechanism.
# - A Future; it will be resumed when the Future is done.
#   This adds a callback to the Future which does roughly:
#     def cb(f):
#       assert f.done
#       if f.exception is not None:
#         g.throw(f)
#       else:
#         g.send(f.result)
#   But when this raises an exception, that exception ought to be
#   propagated back into whatever started *this* generator.

# XXX A Future can be used in several ways:
# - Yield it from a task/coroutine/generator; see above.
# - Check (poll) its status via f.done, f.exception, f.result.
# - Call its wait() method, perhaps indirectly via check_success()
#   or get_result().  What does this do?  Invoke the event loop.
# - Call the Future.wait_any() or Future.wait_all() method.
#   This is supposed to wait for all Futures and RPCs in the argument
#   list.  How does it do this?  By invoking the event loop.
#   (Should we bother?)

# XXX I still can't quite get my head around the intricacies of the
# interaction between coroutines, futures, and the event loop, even
# though I've implemented several of these before...  Maybe a starting
# point would be to implement something that transparently does
# *nothing* except converting "yield a generator g" into (almost just)
# "for x in g: yield x" in such a way that this code can simply be
# deleted once PEP 380 exists (and the code should be changed from
# "yield g" to "yield from g").  A tricky detail may be to make it
# easy to debug problems -- tracebacks involving generators generally
# stink.

def gwrap(func):
  """Decorator to emulate PEP 380 behavior.

  Inside a generator function wrapped in @gwrap, 'yield g', where g is
  a generator object, is equivalent to 'for x in g: yield x', except
  that 'yield g' can also return a value, and that value is whatever g
  passed as the argument to StopIteration when it stopped.

  The idea is that once PEP 380 is implemented, you can drop @gwrap,
  replace 'yield g' with 'yield from g' and 'raise Return(x)' with
  'return x', and everything will work exactly the same as before.

  NOTE: This is not quite the same as @task, which offers event loop
  integration.
  """
  def gwrap_wrapper(*args, **kwds):
    """The wrapper function that is actually returned by gwrap()."""
    # Call the wrapped function.  If it is a generator function, this
    # returns a generator object.  If it raises an exception, let it
    # percolate up unchanged.
    gen = func(*args, **kwds)

    # If that didn't return a generator object, pretend it was a
    # generator that yielded no items.
    if not is_generator(gen):
      if gen is None:
        return  # Don't bother creating a Return() if it returned None.
      raise Return(gen)

    # If this is an immediately recursive call to gwrap_wrapper(),
    # yield out the generator to let the outer call handle things.
    if sys._getframe(1).f_code is sys._getframe(0).f_code:
      result = yield gen
      raise Return(result)

    # The following while loop elaborates on "for x in g: yield x":
    #
    # 1. Pass values or exceptions received from yield back into g.
    #    That's just part of a truly transparent wrapper for a generator.
    #
    # 2. When x is a generator, loop over it in turn, using a stack to
    #    avoid excessive recursion.  That's part of emulating PEP 380
    #    so that "yield g" is interpreted as "yield from g" (which
    #    roughly means "for x in g: yield x").
    #
    # 3. Pass values and exceptions up that stack.  This is where my
    #    brain keeps hurting.

    to_send = None  # What to send into the top generator.
    to_throw = None  # What to throw into the top generator.
    stack = [gen]  # Stack of generators.
    while stack:
      # Throw or send something into the current generator.
      gen = stack[-1]
      try:
        if to_throw is not None:
          gen.throw(to_throw)
        else:
          to_yield = gen.send(to_send)

      except StopIteration, err:
        # The generator has no more items.  Pop it off the stack.
        stack.pop()
        if not stack:
          raise  # We're done.

        # Prepare to send this value into the next generator on the stack.
        to_send = None
        if err.args:
          if len(err.args) == 1:
            to_send = err.args[0]
          else:
            to_send = err.args
        to_throw = None
        continue

      except Exception, err:
        # The generator raised an exception.  Pop it off the stack.
        stack.pop()
        if not stack:
          raise  # We're done.

        # Prepare to throw this exception into the next generator on the stack.
        to_send = None
        to_throw = err
        continue

      else:
        # The generator yielded a value.
        to_throw = None
        to_send = None
        if not is_generator(to_yield):
          # It yielded some plain value.  Yield this outwards.
          # Whatever our yield returns or raises will be sent or thrown
          # into the current generator.
          # TODO: support "yield Return(...)" as an alternative for
          # "raise Return(...)"?  Monocle users would like that.
          try:
            # If the yield returns a value, prepare to send that into
            # the current generator.
            to_send = yield to_yield
          except Exception, err:
            # The yield raised an exception.  Prepare to throw it into
            # the current generator.
            to_throw = err

        else:
          # It yielded another generator.  Push it onto the stack.
          # Note that this new generator is (assumed to be) in the
          # "initial" state for generators, meaning that it hasn't
          # executed any code in the generator function's body yet.
          # In this state we may only call gen.next() or gen.send(None),
          # so it's a good thing that to_send and to_throw are None.
          stack.append(to_yield)

  return gwrap_wrapper

# XXX Well that was all very nice and all, but that's not how we're
# going to get integration with the event loop.  But how?

# XXX I've been thinking about it all wrong!  Here's the right
# structure (and it probably resembles Monocle):

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
