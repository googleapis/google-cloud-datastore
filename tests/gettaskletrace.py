#
# Copyright 2008 Google Inc. All Rights Reserved.
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
#

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
