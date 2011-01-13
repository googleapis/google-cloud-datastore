"""An event loop.

This event loop should handle both asynchronous App Engine RPC objects
(specifically urlfetch and datastore RPC objects) and arbitrary
callback functions with an optional time delay.

Normally, event loops are singleton objects, though there is no
enforcement of this requirement.

The API here is inspired by Monocle.
"""

import bisect
import logging
import os
import time

from google.appengine.api.apiproxy_rpc import RPC

from google.appengine.datastore import datastore_rpc

IDLE = RPC.IDLE
RUNNING = RPC.RUNNING
FINISHING = RPC.FINISHING

class EventLoop(object):
  """An event loop."""

  # TODO: Use a separate queue for tasklets with delay=None.

  def __init__(self):
    """Constructor."""
    self.queue = []
    self.rpcs = {}

  # TODO: Rename to queue_callback?
  def queue_call(self, delay, callable, *args, **kwds):
    """Schedule a function call at a specific time in the future."""
    if delay is None:
      when = 0
    elif delay < 1e9:
      when = delay + time.time()
    else:
      # Times over a billion seconds are assumed to be absolute.
      when = delay
    bisect.insort(self.queue, (when, callable, args, kwds))

  def queue_rpc(self, rpc, callable=None, *args, **kwds):
    """Schedule an RPC with an optional callback.

    The caller must have previously sent the call to the service.
    The optional callback is called with the remaining arguments.

    NOTE: If the rpc is a MultiRpc, the callback will be called once
    for each sub-RPC.  TODO: Is this a good idea?
    """
    if rpc is None:
      return
    assert rpc.state in (RUNNING, FINISHING), rpc.state
    if isinstance(rpc, datastore_rpc.MultiRpc):
      rpcs = rpc.rpcs
    else:
      rpcs = [rpc]
    for rpc in rpcs:
      self.rpcs[rpc] = (callable, args, kwds)

  # TODO: A way to add a datastore Connection

  def run0(self):
    """Run one item (a callback or an RPC wait_any).

    Returns:
      A time to sleep if something happened (may be 0);
      None if all queues are empty.
    """
    delay = None
    if self.queue:
      delay = self.queue[0][0] - time.time()
      if delay is None or delay <= 0:
        when, callable, args, kwds = self.queue.pop(0)
        logging.debug('event: %s', callable.__name__)
        callable(*args, **kwds)
        # TODO: What if it raises an exception?
        return 0
    if self.rpcs:
      rpc = datastore_rpc.MultiRpc.wait_any(self.rpcs)
      if rpc is not None:
        logging.debug('rpc: %s', rpc.method)
        # Yes, wait_any() may return None even for a non-empty argument.
        # But no, it won't ever return an RPC not in its argument.
        assert rpc in self.rpcs, (rpc, self.rpcs)
        callable, args, kwds = self.rpcs[rpc]
        del self.rpcs[rpc]
        if callable is not None:
          callable(*args, **kwds)
          # TODO: Again, what about exceptions?
      return 0
    return delay

  def run1(self):
    """Run one item (a callback or an RPC wait_any) or sleep.

    Returns:
      True if something happened; False if all queues are empty.
    """
    delay = self.run0()
    if delay is None:
      return False
    if delay > 0:
      time.sleep(delay)
    return True

  def run(self):
    """Run until there's nothing left to do."""
    # TODO: A way to stop running before the queue is empty.
    while True:
      if not self.run1():
        break


_EVENT_LOOP_KEY = '__EVENT_LOOP__'
_event_loop = None

def get_event_loop():
  """Return a singleton EventLoop instance.

  A new singleton is created for each new HTTP request.  We determine
  that we're in a new request by inspecting os.environ, which is reset
  at the start of each request.
  """
  # TODO: Use thread-local storage?
  global _event_loop
  ev = None
  if os.getenv(_EVENT_LOOP_KEY):
    ev = _event_loop
  if ev is None:
    ev = EventLoop()
    _event_loop = ev
    os.environ[_EVENT_LOOP_KEY] = '1'
  return ev

def queue_call(*args, **kwds):
  ev = get_event_loop()
  ev.queue_call(*args, **kwds)

def queue_rpc(rpc, callable=None, *args, **kwds):
  ev = get_event_loop()
  ev.queue_rpc(rpc, callable, *args, **kwds)

def run():
  ev = get_event_loop()
  ev.run()

def run1():
  ev = get_event_loop()
  return ev.run1()

def run0():
  ev = get_event_loop()
  return ev.run0()
