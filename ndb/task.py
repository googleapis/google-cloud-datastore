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

import types

class Future(object):
  """A Future has 0 or more callbacks.

  The callbacks will be called when the result is ready.
  """

  def __init__(self):
    self.done = False
    self.result = None
    self.exception = None
    self.callbacks = []

  def add(self, callback):
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

  def wait(self):
    assert self.done  # TODO: How to wait until set_*() is called?

  def check_success(self):
    self.wait()
    if self.exception is not None:
      raise self.exception

  def get_result(self):
    self.check_success()
    return self.result

class Return(StopIteration):
  pass

def task(func):
  def wrapper(*args, **kwds):
    try:
      result = func(*args, **kwds)
      if isinstance(result, types.GeneratorType):
        XXX
    except Exception:
      # NOTE: Don't catch BaseException or string exceptions or
      # exceptions not deriving from Exception.  BaseException
      # deserves to quit the whole program, and string exceptions
      # shouldn't be used at all (they're deprecated in Python 2.5 and
      # cannot be raised in Python 2.6).  Exceptions not deriving from
      # BaseException are theoretically still allowed, but they are
      # not recommended.
      XXX

# XXX Where to put the trampoline code?  Yielding a generator ought to
# schedule that generator -- I've got code in the old NDB to do that.
# The important lesson from Monocle is to separate the trampoline code
# from the event loop.  In particular when PEP 380 goes live (alas,
# not before Python 3.3), the trampoline will no longer be necessary,
# but it has no bearing on the event loop.  I hope.
