"""Stress test for ndb with Python 2.7 threadsafe."""

import logging
import random
import threading
import time

from google.appengine.api import apiproxy_stub_map
from google.appengine.datastore import datastore_stub_util

from google.appengine.ext import testbed

from ndb import model, tasklets


INSTANCES = 4
RUNS = 10

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class EmptyModel(model.Model):
  pass


@tasklets.tasklet
def workload(id, run):
  key = model.Key(EmptyModel, 1)
  ent = EmptyModel(key=key)
  keys = []

  time.sleep(random.random() / 10)

  def tx1():
    new_key = yield ent.put_async()
    assert key == new_key, (id, run)

  def tx2():
    new_ent = yield key.get_async()
    assert ent == new_ent, (id, run)

  def tx3():
    ents = [EmptyModel() for _ in range(10)]
    futs = [e.put_async() for e in ents]
    keys.extend((yield futs))
    for key, ent in zip(keys, ents):
      assert ent == key.get()

  def tx4():
    yield key.delete_async()
    for key in keys:
      assert (yield key.get_async) is None

  yield [model.transaction_async(tx, xg=True) for tx in tx1, tx2, tx3, tx4]


class Stress(threading.Thread):
  def run(self):
    global cache_policy, memcache_policy, datastore_policy
    ctx = tasklets.get_context()
    ctx.set_cache_policy(cache_policy)
    ctx.set_memcache_policy(memcache_policy)
    ctx.set_datastore_policy(datastore_policy)

    id = threading.current_thread().ident

    try:
      for run in range(1, RUNS + 1):
        workload(id, run).check_success()
    except Exception, e:
      logger.exception('Thread %d run %d raised %s: %s',
                       id, run, e.__class__.__name__, e)
    finally:
      logger.info('Thread %d stopped on run %d', id, run)


def main():
  global cache_policy, memcache_policy, datastore_policy

  # Test every single policy choice
  for cache_policy in (True, False):
    for memcache_policy in (True, False):
      for datastore_policy in (True, False):
        if not (cache_policy or memcache_policy or datastore_policy):
          continue
        logger.info('c: %i mc: %i ds: %i', cache_policy, memcache_policy,
                    datastore_policy)

        tb = testbed.Testbed()
        tb.activate()
        tb.init_datastore_v3_stub()
        tb.init_memcache_stub()
        tb.init_taskqueue_stub()
        datastore_stub = apiproxy_stub_map.apiproxy.GetStub('datastore_v3')
        datastore_stub.SetConsistencyPolicy(
          datastore_stub_util.BaseHighReplicationConsistencyPolicy())

        threads = []
        for _ in range(INSTANCES):
          stress_thread = Stress()
          stress_thread.start()
          threads.append(stress_thread)

        for t in threads:
          t.join()

        tb.deactivate()

if __name__ == '__main__':
  main()
