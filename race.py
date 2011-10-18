"""Race condition tests for ndb."""

import logging

logging.basicConfig(
  level=logging.INFO,
  format='%(threadName)s:%(levelname)s:%(name)s:%(message)s')

import threading
import time

from google.appengine.api import memcache
from google.appengine.ext import testbed

from ndb import context
from ndb import eventloop
from ndb import model
from ndb import tasklets


class CrashTestDummyModel(model.Model):
  name = model.StringProperty()


def memcache_locking_put_then_get():
  # Thanks to Arie Ozarov for finding this.
  def setup_context():
    ctx = tasklets.get_context()
    ctx.set_datastore_policy(True)
    ctx.set_memcache_policy(True)
    ctx.set_cache_policy(False)
    return ctx

  key = model.Key(CrashTestDummyModel, 1)
  keystr = tasklets.get_context()._memcache_prefix + key.urlsafe()

  ent1 = CrashTestDummyModel(key=key, name=u'Brad Roberts')
  ent2 = CrashTestDummyModel(key=key, name=u'Ellen Reid')

  ctx = setup_context()
  # Store an original version of the entity
  # NOTE: Do not wish to store this one value in memcache, turning it off
  ent1.put(use_memcache=False)

  a_lock = threading.Lock()
  class A(threading.Thread):
    def run(self):
      ctx = setup_context()
      fut = ent2.put_async()

      # Get to the point that the lock is written to memcache
      while ctx._memcache.get(keystr) != context._LOCKED:
        eventloop.run0()

      # Wait for B to cause a race condition
      a_lock.acquire()
      fut.get_result()
      eventloop.run()
      a_lock.release()

  # A: write lock to memcache
  a = A()
  a_lock.acquire()
  a.start()
  while memcache.get(keystr) != context._LOCKED:
    time.sleep(0.1)  # Wait for the memcache lock to be set

  # M: evict the lock
  memcache.flush_all()
  assert memcache.get(keystr) is None, 'lock was not evicted'

  # B: read from memcache (it's a miss)
  b = key.get_async()
  while not ctx._get_batcher._running:
    eventloop.run0()

  # B: read from datastore
  while not eventloop.get_event_loop().rpcs:
    eventloop.run0()
  while eventloop.get_event_loop().rpcs:
    eventloop.run0()

  # A: write to datastore
  a_lock.release()
  a.join()

  # B: write to memcache (writes a stale value)
  b.get_result()
  eventloop.run()  # Puts to memcache are still stuck in the eventloop

  pb3 = memcache.get(keystr)
  assert pb3 is not context._LOCKED, 'Received _LOCKED value'
  if pb3 is not None:
    ent3 = ctx._conn.adapter.pb_to_entity(pb3)
    assert ent3 == ent2, 'stale value in memcache; %r != %r' % (ent3, ent2)

  # Finally check the high-level API.
  ent4 = key.get()
  assert ent4 == ent2


TESTS = (memcache_locking_put_then_get,)


def main():
  for test in TESTS:
    tb = testbed.Testbed()
    tb.activate()
    tb.init_datastore_v3_stub()
    tb.init_memcache_stub()
    test()
    tb.deactivate()

if __name__ == '__main__':
  main()
