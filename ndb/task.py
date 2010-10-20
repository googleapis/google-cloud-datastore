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

  def __init__(self):
    XXX

  def wait(self):
    XXX

  def check_success(self):
    self.wait()
    XXX  # May raise

  def get_result(self):
    self.check_success()
    return XXX

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
# but it has no bearing on the event loop.

# XXX Where to put the event loop?  Ideally it ought to be independent
# from tasks, futures and generators.  It should be possible to write
# an alternative framework (e.g. one purely based on callbacks, or a
# Monocle stack) that reuses our event loop.  The job of the event
# loop is to keep track of asynchronous App Engine RPCs (in particular
# urlfetch and datastore RPCs), intermingling these with time-based
# events (e.g. call function F in 0.3 seconds) and and "explicit
# wakeups".  The latter are best thought of as Futures; or like
# Twisted Deferreds.  So for RPCs, you can tell the event loop "Here
# are an RPC and a function; when the RPC completes, call the callback
# function with some argument."  For timed calls, you can tell the
# event loop "Here's a delay (or an absolute time?) and a function; at
# the designated time call the function with some argument."  Finally
# for futures I think that while the Future is on hold the event loop
# won't know about it; when it is ready whoever signals it as ready
# must tell the event loop to call the Future's callback ASAP.  But
# how to block, waiting for a Future?  Ideally you should be in a
# generator and yield the Future -- it will be returned to you when it
# is ready using the above mechanism.  But if you aren't in a
# generator we could invoke the nested event loop until this specific
# Future is done.
