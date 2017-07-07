#
# Copyright 2008 The ndb Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test utilities for writing NDB tests.

Useful set of utilities for correctly setting up the appengine testing
environment.  Functions and test-case base classes that configure stubs
and other environment variables.
"""

import logging
import shutil
import tempfile
import os

from .google_imports import apiproxy_stub_map
from .google_imports import datastore
from .google_imports import datastore_pbs
from .google_imports import datastore_rpc
from .google_test_imports import cloud_datastore_v1_remote_stub
from .google_test_imports import datastore_stub_util
from .google_test_imports import testbed
from .google_test_imports import unittest

from . import context
from . import model
from . import tasklets
from . import eventloop


class NDBBaseTest(unittest.TestCase):
  """Base class for tests that interact with API stubs or create Models.

  NOTE: Care must be used when working with model classes using this test
  class.  The kind-map is reset on each iteration.  The general practice
  should be to declare test models in the sub-classes setUp method AFTER
  calling this classes setUp method.
  """

  APP_ID = 'ndb-test-app-id'

  def setUp(self):
    """Set up test framework.

    Configures basic environment variables, stubs and creates a default
    connection.
    """
    self.testbed = testbed.Testbed()
    self.testbed.setup_env(overwrite=True, app_id=self.APP_ID)
    self.testbed.activate()

    self.ResetKindMap()
    self.conn = self.MakeConnection()
    self.ctx = self.MakeContext(conn=self.conn)
    tasklets.set_context(self.ctx)

    self._logger = logging.getLogger()
    self._old_log_level = self._logger.getEffectiveLevel()

  def ExpectErrors(self):
    if self.DefaultLogging():
      self._logger.setLevel(logging.CRITICAL)

  def ExpectWarnings(self):
    if self.DefaultLogging():
      self._logger.setLevel(logging.ERROR)

  def DefaultLogging(self):
    return self._old_log_level == logging.WARNING

  def tearDown(self):
    """Tear down test framework."""
    self.testbed.deactivate()
    self._logger.setLevel(self._old_log_level)
    ev = eventloop.get_event_loop()
    stragglers = 0
    while ev.run1():
      stragglers += 1
    if stragglers:
      logging.info('Processed %d straggler events after test completed',
                   stragglers)
    self.ResetKindMap()

  def ResetKindMap(self):
    model.Model._reset_kind_map()

  def MakeContext(self, config=None, auto_batcher_class=context.AutoBatcher,
                  default_model=None, conn=None):
    if not conn:
      conn = self.MakeConnection(config=config,
                                 default_model=default_model)
    ctx = context.Context(
        conn=conn,
        auto_batcher_class=auto_batcher_class,
        config=config)
    # We only need cache active when testing the cache, so the default behavior
    # is to disable it to avoid misleading test results. Override this when
    # needed.
    ctx.set_cache_policy(False)
    ctx.set_memcache_policy(False)
    return ctx


class NDBTest(NDBBaseTest):

  # Set to the module under test to check its __all__ for inconsistencies.
  the_module = None

  def testAllVariableIsConsistent(self):
    if self.the_module is None:
      return
    modname = self.the_module.__name__
    undefined = []
    for name in self.the_module.__all__:
      if not hasattr(self.the_module, name):
        undefined.append(name)
    self.assertFalse(undefined,
                     '%s.__all__ has some names that are not defined: %s' %
                     (modname, undefined))
    module_type = type(self.the_module)
    unlisted = []
    for name in dir(self.the_module):
      if not name.startswith('_'):
        obj = getattr(self.the_module, name)
        if not isinstance(obj, module_type):
          if name not in self.the_module.__all__:
            unlisted.append(name)
    self.assertFalse(unlisted,
                     '%s defines some names that are not in __all__: %s' %
                     (modname, unlisted))

  def HRTest(self):
    ds_stub = self.testbed.get_stub('datastore_v3')
    hrd_policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(
        probability=1)
    ds_stub.SetConsistencyPolicy(hrd_policy)

  def setUp(self):
    super(NDBTest, self).setUp()

    # model_test.IndexTests requries index setup.
    # The legacy datastore_stub requires an gae app root_path for index.yaml
    # before updating datastore index.
    self.app_root_path = tempfile.mkdtemp()

    self.testbed.init_datastore_v3_stub(root_path=self.app_root_path)
    self.testbed.init_memcache_stub()
    self.testbed.init_taskqueue_stub()

  def tearDown(self):
    super(NDBTest, self).tearDown()
    shutil.rmtree(self.app_root_path)

  def MakeConnection(self, *args, **kwargs):
    return model.make_connection(*args, **kwargs)


class NDBCloudDatastoreV1Test(NDBBaseTest):
  """NDB test base that uses a datastore V1 connection."""

  @classmethod
  def setUpClass(cls):
    # Late import so that tests can still run if googledatastore is not
    # available.
    from . import datastore_emulator
    factory = datastore_emulator.DatastoreEmulatorFactory()
    cls.datastore = factory.Create(cls.APP_ID)

  @classmethod
  def tearDownClass(cls):
    cls.datastore.Stop()

  def setUp(self):
    super(NDBCloudDatastoreV1Test, self).setUp()
    # The host doesn't get used since we override the stub, however the
    # existence of this environment variable can break if we try to get
    # credentials.
    os.environ['DATASTORE_EMULATOR_HOST'] = 'localhost:1234'
    self.datastore.Clear()
    stub = cloud_datastore_v1_remote_stub.CloudDatastoreV1RemoteStub(
        self.datastore.GetDatastore())
    apiproxy_stub_map.apiproxy.ReplaceStub(datastore_rpc._CLOUD_DATASTORE_V1,
                                           stub)

  def tearDown(self):
    super(NDBCloudDatastoreV1Test, self).tearDown()
    self.datastore.Clear()

  def HRTest(self):
    pass

  def MakeConnection(self, *args, **kwargs):
    if '_api_version' not in kwargs:
      kwargs['_api_version'] = datastore_rpc._CLOUD_DATASTORE_V1
    if '_id_resolver' not in kwargs:
      kwargs['_id_resolver'] = datastore_pbs.IdResolver([self.APP_ID])
    return model.make_connection(*args, **kwargs)
