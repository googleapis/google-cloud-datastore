"""An event loop.

This event loop should handle both asynchronous App Engine RPC objects
(specifically urlfetch and datastore RPC objects) and arbitrary
callback functions with an optional time delay.

Normally, event loops are singleton objects, though there is no
enforcement of this requirement.

The API here is inspired by Monocle.
"""

import bisect
import time

from core import datastore_rpc

class EventLoop(object):
  """An event loop."""

  def __init__(self):
    """Constructor."""
    self.queue = []
    self.rpcs = set()

  def queue_task(self, delay, callable, *args, **kwds):
    """Schedule a function call at a specific time in the future."""
    if delay < 1e9:
      when = delay + time.time()
    else:
      # Times over a billion seconds are assumed to be absolute.
      when = delay
    bisect.insort(self.queue, (when, callable, args, kwds))

  def queue_rpc(self, rpc):
    """Schedule an RPC.

    The caller must have previously sent the call to the service.
    Callbacks are to be dealt with by the RPC world.
    """
    assert rpc.state > 0  # TODO: Use apiproxy_rpc.RPC.*.
    if isinstance(rpc, datastore_rpc.MultiRpc):
      self.rpcs.update(rpc.rpcs)
    else:
      self.rpcs.add(rpc)

  # TODO: A way to add a datastore Connection

  def run(self):
    """Run until there's nothing left to do."""
    # TODO: A way to stop running before the queue is empty.
    # TODO: Run until a specific event (or RPC or time?).
    while self.queue or self.rpcs:
      if self.queue:
        delay = self.queue[0][0] - time.time()
        if delay <= 0:
          when, callable, args, kwds = self.queue.pop(0)
          callable(*args, **kwds)
          # TODO: What if it raises an exception?
          # TODO: What if it returns a value other than None?
          continue
      if self.rpcs:
        rpc = datastore_rpc.MultiRpc.wait_any(self.rpcs)
        if rpc is not None:
          # Yes, wait_any() may return None even for a non-empty argument.
          # But no, it won't ever return an RPC not in its argument.
          assert rpc in self.rpcs, (rpc, self.rpcs)
          self.rpcs.remove(rpc)
