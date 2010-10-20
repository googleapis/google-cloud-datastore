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

class EventLoop(object):
  """An event loop."""

  def __init__(self):
    self.queue = []

  def queue_task(self, delay, callable, *args, **kwds):
    if delay < 1e9:
      when = delay + time.time()
    else:
      # Times over a billion seconds are assumed to be absolute.
      when = delay
    bisect.insort(self.queue, (when, callable, args, kwds))

  def run(self):
    # TODO: Handle RPCs.
    # TODO: A way to stop running before the queue is empty.
    # TODO: Run until a specific event.
    while self.queue:
      delay = self.queue[0][0] - time.time()
      if delay > 0:
        time.sleep(delay)
        continue
      when, callable, args, kwds = self.queue.pop(0)
      callable(*args, **kwds)
      # TODO: What if it raises an exception?
      # TODO: What if it returns a value other than None?
