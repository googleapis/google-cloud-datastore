"""Runs an infinite loop to find race conditions in a context get_tasklet."""

import os

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_file_stub
from google.appengine.api.memcache import memcache_stub

from ndb import model, tasklets


def cause_problem():
  APP_ID = '_'
  os.environ['APPLICATION_ID'] = APP_ID
  os.environ['AUTH_DOMAIN'] = 'example.com'

  apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
  ds_stub = datastore_file_stub.DatastoreFileStub(APP_ID, None)
  apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', ds_stub)
  mc_stub = memcache_stub.MemcacheServiceStub()
  apiproxy_stub_map.apiproxy.RegisterStub('memcache', mc_stub)

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


if __name__ == '__main__':
  while True:
    cause_problem()
