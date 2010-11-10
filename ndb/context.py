"""Context class."""

import logging
import sys

from google.appengine.api import datastore_errors
from google.appengine.api import memcache

from core import datastore_rpc

from ndb import model, tasks, eventloop

class AutoBatcher(object):

  def __init__(self, todo_task):
    self._todo_task = todo_task  # Task called with list of (future, arg) pairs
    self._todo = []  # List of (future, arg) pairs
    self._running = None  # Currently running task, if any

  def add(self, arg):
    fut = tasks.Future()
    if not self._todo:  # Schedule the callback
      # We use the fact that regular tasks are queued at time None,
      # which puts them at absolute time 0 (i.e. ASAP -- still on a
      # FIFO basis).  Callbacks explicitly scheduled with a delay of 0
      # are only run after all immediately runnable tasks have run.
      eventloop.queue_task(0, self._callback)
    self._todo.append((fut, arg))
    return fut

  def _callback(self, unused=None):
    # The unused argument is for add_done_callback(self._callback) below.
    if not self._todo:
      return
    if self._running is not None:
      # Another callback may still be running.
      if not self._running.done():
        # Wait for it to complete first, then try again.
        self._running.add_done_callback(self._callback)
        return
      self._running = None
    # We cannot postpone the inevitable any longer.
    todo = self._todo
    self._todo = []  # Get ready for the next batch
    logging.info('AutoBatcher(%s): %d items',
                 self._todo_task.__name__, len(todo))
    self._running = self._todo_task(todo)
    # Add a callback to the Future to propagate exceptions,
    # since this Future is not normally checked otherwise.
    self._running.add_done_callback(lambda fut: fut.check_success())

  @tasks.task
  def flush(self):
    while self._running or self._todo:
      if self._running:
        if self._running.done():
          self._running = None
        else:
          yield self._running
      else:
        self._callback()

# TODO: Rename?  To what?  Session???
class Context(object):

  def __init__(self, conn=None, auto_batcher_class=AutoBatcher):
    if conn is None:
      conn = model.conn  # TODO: Get rid of this?
    self._conn = conn
    self._auto_batcher_class = auto_batcher_class
    self._get_batcher = auto_batcher_class(self._get_task)
    self._put_batcher = auto_batcher_class(self._put_task)
    self._delete_batcher = auto_batcher_class(self._delete_task)
    self._cache = {}
    self._cache_policy = None

  @tasks.task
  def flush(self):
    yield (self._get_batcher.flush(),
           self._put_batcher.flush(),
           self._delete_batcher.flush())

  @tasks.task
  def _get_task(self, todo):
    assert todo
    # First check memcache.
    keymap = {}
    for fut, key in todo:
      keymap[key.urlsafe()] = fut, key
    results = memcache.get_multi(keymap.keys())
    for mkey, pb in results.iteritems():
      fut, key = keymap[mkey]
      ent = self._conn.adapter.pb_to_entity(pb)
      fut.set_result(ent)
      del keymap[mkey]
    # Go to the datastore if anything left in keymap.
    if keymap:
      todo = keymap.values()
      keys = [key for (_, key) in todo]
      results = yield self._conn.async_get(None, keys)
      for ent, (fut, _) in zip(results, todo):
        fut.set_result(ent)

  @tasks.task
  def _put_task(self, todo):
    assert todo
    ents = [ent for (_, ent) in todo]
    results = yield self._conn.async_put(None, ents)
    for key, (fut, ent) in zip(results, todo):
      if key != ent.key:
        assert ent.key is None or not list(ent.key.flat())[-1]
        ent.key = key
      fut.set_result(key)
    # Now update memcache.
    # TODO: Could we update memcache *before* calling async_put()?
    mapping = {}
    for _, ent in todo:
      pb = self._conn.adapter.entity_to_pb(ent)
      mapping[ent.key.urlsafe()] = pb
    failures = memcache.set_multi(mapping)
    if failures:
      badkeys = []
      for failure in failures:
        badkeys.append(mapping[failure].key)
      logging.info('memcache failed to set %d out of %d keys: %s',
                   len(failures), len(mapping), badkeys)

  @tasks.task
  def _delete_task(self, todo):
    assert todo
    keys = [key for (_, key) in todo]
    yield self._conn.async_delete(None, keys)
    for fut, _ in todo:
      fut.set_result(None)
    # Now update memcache.
    memkeys = [key.urlsafe() for key in keys]
    memcache.delete_multi(memkeys)
    # The value returned by delete_multi() is pretty much useless, it
    # could be the keys were never cached in the first place.

  def set_cache_policy(self, func):
    self._cache_policy = func

  def should_cache(self, key, entity):
    if self._cache_policy is None:
      return True
    return self._cache_policy(key, entity)

  # TODO: What about conflicting requests to different autobatchers,
  # e.g. task A calls get() on a given key while task B calls
  # delete()?  The outcome is nondeterministic, depending on which
  # autobatcher gets run first.  Maybe we should just flag such
  # conflicts as errors, with an overridable policy to resolve them
  # differently?

  @tasks.task
  def get(self, key):
    if key in self._cache:
      entity = self._cache[key]  # May be None, meaning "doesn't exist".
    else:
      entity = yield self._get_batcher.add(key)
      if self.should_cache(key, entity):
        self._cache[key] = entity
    raise tasks.Return(entity)

  @tasks.task
  def put(self, entity):
    key = yield self._put_batcher.add(entity)
    if entity.key != key:
      logging.info('replacing key %s with %s', entity.key, key)
      entity.key = key
    if self.should_cache(key, entity):
      self._cache[key] = entity
    raise tasks.Return(key)

  @tasks.task
  def delete(self, key):
    yield self._delete_batcher.add(key)
    if key in self._cache:
      self._cache[key] = None

  @tasks.task
  def allocate_ids(self, key, size=None, max=None):
    lo_hi = yield self._conn.async_allocate_ids(None, key, size, max)
    raise tasks.Return(lo_hi)

  def map_query(self, query, callback,
                options=None, reducer=None, initial=None):
    mfut = tasks.MultiFuture(reducer, initial)

    @tasks.task
    def helper():
      rpc = query.run_async(self._conn, options)
      count = 0
      while rpc is not None:
        batch = yield rpc
        rpc = batch.next_batch_async(options)
        for ent in batch.results:
          key = ent.key
          if key in self._cache:
            if self._cache[key] is None:
              # This is a weird case.  Apparently this entity was
              # deleted concurrently with the query.  Let's just
              # pretend he delete happened first.
              logging.info('Conflict: entity %s was deleted', key)
              continue
            # Replace the entity the callback will see with the one
            # from the cache.
            if ent != self._cache[key]:
              logging.info('Conflict: entity %s was modified', key)
            ent = self._cache[key]
          else:
            if self.should_cache(key, ent):
              self._cache[key] = ent
          count += 1
          val = callback(ent)  # TODO: If this raises something, log and ignore
          if isinstance(val, tasks.Future):
            mfut.add_dependent(val)
          else:
            mfut.process_value(val)
      mfut.complete()
      raise tasks.Return(count)

    return mfut, helper()

  @tasks.task
  def transaction(self, callback, retry=3, entity_group=None):
    # Will invoke callback(ctx) one or more times with ctx set to a new,
    # transactional Context.  Returns a Future.  Callback must be a task.
    if entity_group is not None:
      app = entity_group._Key__reference.app()
    else:
      app = model._DefaultAppId()
    yield self.flush()
    for i in range(1 + max(0, retry)):
      transaction = yield self._conn.async_begin_transaction(None, app)
      tconn = datastore_rpc.TransactionalConnection(
        adapter=self._conn.adapter,
        config=self._conn.config,
        transaction=transaction,
        entity_group=entity_group)
      tctx = self.__class__(conn=tconn,
                            auto_batcher_class=self._auto_batcher_class)
      fut = callback(tctx)
      assert isinstance(fut, tasks.Future)
      try:
        try:
          result = yield fut
        finally:
          yield tctx.flush()
      except Exception, err:
        t, e, tb = sys.exc_info()
        yield tconn.async_rollback(None)  # TODO: Don't block???
        raise t, e, tb
      else:
        ok = yield tconn.async_commit(None)
        if ok:
          self._cache.update(tctx._cache)
          raise tasks.Return(result)
    # Out of retries
    raise datastore_errors.TransactionFailedError(
      'The transaction could not be committed. Please try again.')

  @tasks.task
  def get_or_insert(self, model_class, name, parent=None, **kwds):
    assert isinstance(name, basestring) and name
    if parent is None:
      pairs = []
    else:
      pairs = list(parent.pairs())
    pairs.append((model_class.GetKind(), name))
    key = model.Key(pairs=pairs)
    ent = yield self.get(key)
    if ent is None:
      @tasks.task
      def txn(ctx):
        ent = yield ctx.get(key)
        if ent is None:
          ent = model_class(**kwds)  # TODO: Check for forbidden keys
          ent.key = key
          yield ctx.put(ent)
        raise tasks.Return(ent)
      ent = yield self.transaction(txn)
    raise tasks.Return(ent)


# TODO: Is this a good idea?
def add_context(func):
  """Decorator that adds a fresh Context as self.ctx."""
  def add_context_wrapper(self, *args):
    self.ctx = Context()
    return func(self, *args)
  return add_context_wrapper
