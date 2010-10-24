"""A task decorator.

This decorates a generator function so that when it is called, a
Future is returned while the generator is executed by the event loop.
For example:

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
get_result() is somewhat inefficient; in most cases such code should
be rewritten as a task instead:

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

# TODO: Rework the following into documentation.

# A task/coroutine/generator can yield the following things:
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

# A Future can be used in several ways:
# - Yield it from a task/coroutine/generator; see above.
# - Check (poll) its status via f.done, f.exception, f.result.
# - Call its wait() method, perhaps indirectly via check_success()
#   or get_result().  What does this do?  Invoke the event loop.
# - Call the Future.wait_any() or Future.wait_all() method.
#   This is supposed to wait for all Futures and RPCs in the argument
#   list.  How does it do this?  By invoking the event loop.
#   (Should we bother?)

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
