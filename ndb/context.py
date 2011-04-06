"""Context class."""

# TODO: Handle things like request size limits.  E.g. what if we've
# batched up 1000 entities to put and now the memcache call fails?

import logging
import sys

from google.appengine.api import datastore_errors
from google.appengine.api import memcache

from google.appengine.datastore import datastore_rpc

import ndb.key
from ndb import model, tasklets, eventloop, utils

class AutoBatcher(object):

  def __init__(self, todo_tasklet):
    # todo_tasklet is a tasklet to be called with list of (future, arg) pairs
    self._todo_tasklet = todo_tasklet
    self._todo = []  # List of (future, arg) pairs
    self._running = None  # Currently running tasklet, if any

  def __repr__(self):
    return '%s(%s)' % (self.__class__.__name__, self._todo_tasklet.__name__)

  def add(self, arg):
    fut = tasklets.Future('%s.add(%s)' % (self, arg))
    if not self._todo:  # Schedule the callback
      # We use the fact that regular tasklets are queued at time None,
      # which puts them at absolute time 0 (i.e. ASAP -- still on a
      # FIFO basis).  Callbacks explicitly scheduled with a delay of 0
      # are only run after all immediately runnable tasklets have run.
      eventloop.queue_call(0, self._autobatcher_callback)
    self._todo.append((fut, arg))
    return fut

  def _autobatcher_callback(self):
    if not self._todo:
      return
    if self._running is not None:
      # Another callback may still be running.
      if not self._running.done():
        # Wait for it to complete first, then try again.
        self._running.add_callback(self._autobatcher_callback)
        return
      self._running = None
    # We cannot postpone the inevitable any longer.
    todo = self._todo
    self._todo = []  # Get ready for the next batch
    logging.info('AutoBatcher(%s): %d items',
                 self._todo_tasklet.__name__, len(todo))
    self._running = self._todo_tasklet(todo)
    # Add a callback to the Future to propagate exceptions,
    # since this Future is not normally checked otherwise.
    self._running.add_callback(self._running.check_success)

  @tasklets.tasklet
  def flush(self):
    while self._running or self._todo:
      if self._running:
        if self._running.done():
          self._running.check_success()
          self._running = None
        else:
          yield self._running
      else:
        self._autobatcher_callback()

# TODO: Rename?  To what?  Session???
class Context(object):

  def __init__(self, conn=None, auto_batcher_class=AutoBatcher):
    if conn is None:
      conn = model.make_connection()
    self._conn = conn
    self._auto_batcher_class = auto_batcher_class
    self._get_batcher = auto_batcher_class(self._get_tasklet)
    self._put_batcher = auto_batcher_class(self._put_tasklet)
    self._delete_batcher = auto_batcher_class(self._delete_tasklet)
    self._cache = {}
    self._cache_policy = lambda key: True
    self._memcache_policy = lambda key: True
    # TODO: Also add a way to compute the memcache expiration time.

  @tasklets.tasklet
  def flush(self):
    yield (self._get_batcher.flush(),
           self._put_batcher.flush(),
           self._delete_batcher.flush())

  @tasklets.tasklet
  def _get_tasklet(self, todo):
    assert todo
    # First check memcache.
    keys = set(key for _, key in todo)
    memkeymap = dict((key, key.urlsafe())
                     for key in keys if self.should_memcache(key))
    if memkeymap:
      results = memcache.get_multi(memkeymap.values())
      leftover = []
##      del todo[1:]  # Uncommenting this creates an interesting bug.
      for fut, key in todo:
        mkey = memkeymap[key]
        if mkey in results:
          pb = results[mkey]
          ent = self._conn.adapter.pb_to_entity(pb)
          fut.set_result(ent)
        else:
          leftover.append((fut, key))
      todo = leftover
    if todo:
      keys = [key for (_, key) in todo]
      # TODO: What if async_get() created a non-trivial MultiRpc?
      results = yield self._conn.async_get(None, keys)
      for ent, (fut, _) in zip(results, todo):
        fut.set_result(ent)

  @tasklets.tasklet
  def _put_tasklet(self, todo):
    assert todo
    # TODO: What if the same entity is being put twice?
    # TODO: What if two entities with the same key are being put?
    # TODO: Clear entities from memcache before starting the write?
    # TODO: Attempt to prevent dogpile effect while keeping cache consistent?
    ents = [ent for (_, ent) in todo]
    results = yield self._conn.async_put(None, ents)
    for key, (fut, ent) in zip(results, todo):
      if key != ent.key:
        if ent.has_complete_key():
          raise datastore_errors.BadKeyError(
              'Entity key differs from the one returned by the datastore. '
              'Expected %r, got %r' % (key, ent.key))
        ent.key = key
      fut.set_result(key)
    # Now update memcache.
    # TODO: Could we update memcache *before* calling async_put()?
    # (Hm, not for new entities but possibly for updated ones.)
    mapping = {}
    for _, ent in todo:
      if self.should_memcache(ent.key):
        pb = self._conn.adapter.entity_to_pb(ent)
        mapping[ent.key.urlsafe()] = pb
    if mapping:
      # TODO: Optionally set the memcache expiration time;
      # maybe configurable based on key (or even entity).
      failures = memcache.set_multi(mapping)
      if failures:
        badkeys = []
        for failure in failures:
          badkeys.append(mapping[failure].key)
        logging.info('memcache failed to set %d out of %d keys: %s',
                     len(failures), len(mapping), badkeys)

  @tasklets.tasklet
  def _delete_tasklet(self, todo):
    assert todo
    keys = set(key for (_, key) in todo)
    yield self._conn.async_delete(None, keys)
    for fut, _ in todo:
      fut.set_result(None)
    # Now update memcache.
    memkeys = [key.urlsafe() for key in keys if self.should_memcache(key)]
    if memkeys:
      memcache.delete_multi(memkeys)
      # The value returned by delete_multi() is pretty much useless, it
      # could be the keys were never cached in the first place.

  def get_cache_policy(self):
    """Returns the current context cache policy.

    Returns:
      A function that accepts a Key instance as argument and returns
      a boolean indicating if it should be cached.
    """
    return self._cache_policy

  def set_cache_policy(self, func):
    """Sets the context cache policy.

    Args:
      func: A function that accepts a Key instance as argument and returns
        a boolean indicating if it should be cached.
    """
    self._cache_policy = func

  def should_cache(self, key):
    """Return whether to use the context cache for this key.

    Args:
      key: Key instance.

    Returns:
      True if the key should be cached, False otherwise.
    """
    return self._cache_policy(key)

  def get_memcache_policy(self):
    """Returns the current memcache policy.

    Returns:
      A function that accepts a Key instance as argument and returns
      a boolean indicating if it should be cached.
    """
    return self._memcache_policy

  def set_memcache_policy(self, func):
    """Sets the memcache policy.

    Args:
      func: A function that accepts a Key instance as argument and returns
        a boolean indicating if it should be cached.
    """
    self._memcache_policy = func

  def should_memcache(self, key):
    """Return whether to use memcache for this key.

    Args:
      key: Key instance.

    Returns:
      True if the key should be cached, False otherwise.
    """
    return self._memcache_policy(key)

  # TODO: What about conflicting requests to different autobatchers,
  # e.g. tasklet A calls get() on a given key while tasklet B calls
  # delete()?  The outcome is nondeterministic, depending on which
  # autobatcher gets run first.  Maybe we should just flag such
  # conflicts as errors, with an overridable policy to resolve them
  # differently?

  @tasklets.tasklet
  def get(self, key):
    """Returns a Model instance given the entity key.

    It will use the context cache if the cache policy for the given
    key is enabled.

    Args:
      key: Key instance.

    Returns:
      A Model instance it the key exists in the datastore; None otherwise.
    """
    should_cache = self.should_cache(key)
    if should_cache and key in self._cache:
      entity = self._cache[key]  # May be None, meaning "doesn't exist".
    else:
      entity = yield self._get_batcher.add(key)
      if should_cache:
        self._cache[key] = entity
    raise tasklets.Return(entity)

  @tasklets.tasklet
  def put(self, entity):
    key = yield self._put_batcher.add(entity)
    if entity.key != key:
      logging.info('replacing key %s with %s', entity.key, key)
      entity.key = key
    # TODO: For updated entities, could we update the cache first?
    if self.should_cache(key):
      # TODO: What if by now the entity is already in the cache?
      self._cache[key] = entity
    raise tasklets.Return(key)

  @tasklets.tasklet
  def delete(self, key):
    yield self._delete_batcher.add(key)
    if key in self._cache:
      self._cache[key] = None

  @tasklets.tasklet
  def allocate_ids(self, key, size=None, max=None):
    lo_hi = yield self._conn.async_allocate_ids(None, key, size, max)
    raise tasklets.Return(lo_hi)

  @datastore_rpc._positional(3)
  def map_query(self, query, callback, options=None, merge_future=None):
    mfut = merge_future
    if mfut is None:
      mfut = tasklets.MultiFuture('map_query')

    @tasklets.tasklet
    def helper():
      inq = tasklets.SerialQueueFuture()
      query.run_to_queue(inq, self._conn, options)
      is_ancestor_query = query.ancestor is not None
      while True:
        try:
          ent = yield inq.getq()
        except EOFError:
          break
        if isinstance(ent, model.Key):
          pass  # It was a keys-only query and ent is really a Key.
        else:
          key = ent.key
          if key in self._cache:
            # Assume the cache is more up to date.
            if self._cache[key] is None:
              # This is a weird case.  Apparently this entity was
              # deleted concurrently with the query.  Let's just
              # pretend the delete happened first.
              logging.info('Conflict: entity %s was deleted', key)
              continue
            # Replace the entity the callback will see with the one
            # from the cache.
            if ent != self._cache[key]:
              logging.info('Conflict: entity %s was modified', key)
            ent = self._cache[key]
          else:
            if is_ancestor_query and self.should_cache(key):
              self._cache[key] = ent
        if callback is None:
          val = ent
        else:
          val = callback(ent)  # TODO: If this raises, log and ignore
        mfut.putq(val)
      mfut.complete()

    helper()
    return mfut

  @datastore_rpc._positional(2)
  def iter_query(self, query, options=None):
    return self.map_query(query, callback=None, options=options,
                          merge_future=tasklets.SerialQueueFuture())

  @tasklets.tasklet
  def transaction(self, callback, retry=3, entity_group=None):
    # Will invoke callback() one or more times with the default
    # context set to a new, transactional Context.  Returns a Future.
    # Callback may be a tasklet.
    if entity_group is not None:
      app = entity_group.app()
    else:
      app = ndb.key._DefaultAppId()
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
      tctx.set_memcache_policy(lambda key: False)
      tasklets.set_context(tctx)
      try:
        try:
          result = callback()
          if isinstance(result, tasklets.Future):
            result = yield result
        finally:
          yield tctx.flush()
      except Exception, err:
        t, e, tb = sys.exc_info()
        yield tconn.async_rollback(None)  # TODO: Don't block???
        raise t, e, tb
      else:
        ok = yield tconn.async_commit(None)
        if ok:
          # TODO: This is questionable when self is transactional.
          self._cache.update(tctx._cache)
          self._flush_memcache(tctx._cache)
          raise tasklets.Return(result)
    # Out of retries
    raise datastore_errors.TransactionFailedError(
      'The transaction could not be committed. Please try again.')

  def flush_cache(self):
    """Clears the in-memory cache.

    NOTE: This does not affect memcache.
    """
    self._cache.clear()

  def _flush_memcache(self, keys):
    keys = set(key for key in keys if self.should_memcache(key))
    if keys:
      memkeys = [key.urlsafe() for key in keys]
      memcache.delete_multi(memkeys)

  @tasklets.tasklet
  def get_or_insert(self, model_class, name,
                    app=None, namespace=None, parent=None,
                    **kwds):
    # TODO: Test the heck out of this, in all sorts of evil scenarios.
    assert isinstance(name, basestring) and name
    key = model.Key(model_class, name,
                    app=app, namespace=namespace, parent=parent)
    # TODO: Can (and should) the cache be trusted here?
    ent = yield self.get(key)
    if ent is None:
      @tasklets.tasklet
      def txn():
        ent = yield key.get_async()
        if ent is None:
          ent = model_class(**kwds)  # TODO: Check for forbidden keys
          ent.key = key
          yield ent.put_async()
        raise tasklets.Return(ent)
      ent = yield self.transaction(txn)
    raise tasklets.Return(ent)


def toplevel(func):
  """A sync tasklet that sets a fresh default Context.

  Use this for toplevel view functions such as
  webapp.RequestHandler.get() or Django view functions.
  """
  @utils.wrapping(func)
  def add_context_wrapper(*args):
    __ndb_debug__ = utils.func_info(func)
    tasklets.Future.clear_all_pending()
    # Reset context; a new one will be created on the first call to
    # get_context().
    tasklets.set_context(None)
    ctx = tasklets.get_context()
    try:
      return tasklets.synctasklet(func)(*args)
    finally:
      eventloop.run()  # Ensure writes are flushed, etc.
  return add_context_wrapper


# Transaction API using the default context.

def transaction(callback):
  return transaction_async(callback).get_result()

def transaction_async(callback):
  return tasklets.get_context().transaction(callback)
