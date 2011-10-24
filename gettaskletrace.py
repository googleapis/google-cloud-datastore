"""Runs an infinite loop to find race conditions in context get_tasklet."""

from google.appengine.ext import testbed

from ndb import model
from ndb import tasklets


def cause_problem():
  tb = testbed.Testbed()
  tb.activate()
  tb.init_datastore_v3_stub()
  tb.init_memcache_stub()
  ctx = tasklets.make_default_context()
  tasklets.set_context(ctx)
  ctx.set_datastore_policy(True)
  ctx.set_cache_policy(False)
  ctx.set_memcache_policy(True)

  @tasklets.tasklet
  def problem_tasklet():
    class Foo(model.Model):
      pass
    key = yield ctx.put(Foo())
    yield ctx.get(key)  # Trigger get_tasklet that does not complete...
    yield ctx.delete(key)  # ... by the time this delete_tasklet starts.
    a = yield ctx.get(key)
    assert a is None, '%r is not None' % a

  problem_tasklet().check_success()
  print 'No problem yet...'
  tb.deactivate()


if __name__ == '__main__':
  while True:
    cause_problem()
