"""Test utlities for writing NDB tests.

Useful set of utilities for correctly setting up the appengine testing
environment.  Functions and test-case base classes that configure stubs
and other environment variables.
"""

import os
import logging
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_file_stub
from google.appengine.api import memcache
from google.appengine.api.memcache import memcache_stub
from google.appengine.api import taskqueue
from google.appengine.api.taskqueue import taskqueue_stub

from ndb import model
from ndb import tasklets
from ndb import eventloop


def set_up_basic_stubs(app_id):
  """Set up a basic set of stubs.

  Configures datastore and memcache stubs for testing.

  Args:
    app_id: Application ID to configure stubs with.

  Returns:
    Dictionary mapping stub name to stub.
  """
  apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
  ds_stub = datastore_file_stub.DatastoreFileStub(app_id, None)
  apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', ds_stub)
  mc_stub = memcache_stub.MemcacheServiceStub()
  apiproxy_stub_map.apiproxy.RegisterStub('memcache', mc_stub)
  tq_stub = taskqueue_stub.TaskQueueServiceStub()
  apiproxy_stub_map.apiproxy.RegisterStub('taskqueue', tq_stub)

  return {
    'datastore': ds_stub,
    'memcache': mc_stub,
    'taskqueue': tq_stub,
  }


class DatastoreTest(unittest.TestCase):
  """Base class for tests that actually interact with the (stub) Datastore.

  NOTE: Care must be used when working with model classes using this test
  class.  The kind-map is reset on each iteration.  The general practice
  should be to declare test models in the sub-classes setUp method AFTER
  calling this classes setUp method.
  """

  # Override this in sub-classes to configure alternate application ids.
  APP_ID = '_'

  def setUp(self):
    """Set up test framework.

    Configures basic environment variables, stubs and creates a default
    connection.
    """
    os.environ['APPLICATION_ID'] = self.APP_ID
    # Set the defeault AUTH_DOMAIN, otherwise datastore_file_stub.py
    # can't compare User objects.
    os.environ['AUTH_DOMAIN'] = 'example.com'

    self.set_up_stubs()

    self.conn = model.make_connection()

    self.ResetKindMap()
    self.SetupContextCache()

  def tearDown(self):
    """Tear down test framework."""
    ev = eventloop.get_event_loop()
    stragglers = 0
    while ev.run1():
      stragglers += 1
    if stragglers:
      logging.info('Processed %d straggler events after test completed',
                   stragglers)
    self.ResetKindMap()
    self.datastore_stub.Clear()
    self.memcache_stub.MakeSyncCall('memcache', 'FlushAll',
                                    memcache.MemcacheFlushRequest(),
                                    memcache.MemcacheFlushResponse())
    for q in self.taskqueue_stub.GetQueues():
      self.taskqueue_stub.FlushQueue(q['name'])

  def set_up_stubs(self):
    """Set up basic stubs using classes default application id.

    Set attributes on tests for each stub created.
    """
    for name, value in set_up_basic_stubs(self.APP_ID).iteritems():
      setattr(self, name + '_stub', value)

  def ResetKindMap(self):
    model.Model._reset_kind_map()

  def SetupContextCache(self):
    """Set up the context cache.

    We only need cache active when testing the cache, so the default behavior
    is to disable it to avoid misleading test results. Override this when
    needed.
    """
    from ndb import tasklets
    ctx = tasklets.get_context()
    ctx.set_cache_policy(False)
    ctx.set_memcache_policy(False)
