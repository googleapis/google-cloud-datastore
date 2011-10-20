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


def wait_on_batcher(batcher):
  while not batcher._running:
    eventloop.run0()
  while batcher._running:
    eventloop.run0()


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
      wait_on_batcher(ctx._memcache_set_batcher)

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
  wait_on_batcher(ctx._memcache_get_batcher)

  # B: read from datastore
  wait_on_batcher(ctx._get_batcher)

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


def subverting_aries_fix():
  # Variation by Guido van Rossum.
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

  a_written_to_datastore = False

  a_lock1 = threading.Lock()
  a_lock2 = threading.Lock()
  a_lock3 = threading.Lock()
  class A(threading.Thread):
    def run(self):
      ctx = setup_context()
      fut = ent2.put_async()

      # Get to the point that the lock is written to memcache
      wait_on_batcher(ctx._memcache_set_batcher)

      # Wait for B to cause a race condition
      a_lock2.acquire()
      a_lock1.acquire()
      wait_on_batcher(ctx._put_batcher)
      a_lock2.release()
      a_lock1.release()

      # Wait for C to read from memcache
      a_lock3.acquire()
      fut.check_success()
      a_lock3.release()

  class C(threading.Thread):
    def run(self):
      ctx = setup_context()
      result = key.get()
      assert result == ent2, result
      eventloop.run()

  logging.info('A: write lock to memcache')
  a = A()
  a_lock1.acquire()
  a_lock3.acquire()
  a.start()
  while memcache.get(keystr) != context._LOCKED:
    time.sleep(0.1)  # Wait for the memcache lock to be set

  logging.info('M: evict the lock')
  memcache.flush_all()
  assert memcache.get(keystr) is None, 'lock was not evicted'

  logging.info("B: read from memcache (it's a miss)")
  b = key.get_async()
  wait_on_batcher(ctx._memcache_get_batcher)

  logging.info('B: write lock to memcache')
  wait_on_batcher(ctx._memcache_set_batcher)

  logging.info("B: read the lock back (it's a success)")
  wait_on_batcher(ctx._memcache_get_batcher)

  logging.info('B: read from datastore')
  wait_on_batcher(ctx._get_batcher)

  logging.info('A: write to datastore')
  a_lock1.release()
  a_lock2.acquire()
  a_lock2.release()

  logging.info('B: write to memcache (writes a stale value)')
  b.get_result()
  eventloop.run()  # Puts to memcache are still stuck in the eventloop

  logging.info('C: read from memcache (sees a stale value)')
  c = C()
  c.start()
  c.join()

  logging.info('A: delete from memcache (deletes the stale value!)')
  a_lock3.release()
  a.join()

  pb3 = memcache.get(keystr)
  assert pb3 is not context._LOCKED, 'Received _LOCKED value'
  if pb3 is not None:
    ent3 = ctx._conn.adapter.pb_to_entity(pb3)
    assert ent3 == ent2, 'stale value in memcache; %r != %r' % (ent3, ent2)

  # Finally check the high-level API.
  ent4 = key.get()
  assert ent4 == ent2


TESTS = (
          # memcache_locking_put_then_get,
          subverting_aries_fix,
        )


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
