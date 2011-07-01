"""Context class."""

# TODO: Handle things like request size limits.  E.g. what if we've
# batched up 1000 entities to put and now the memcache call fails?

import logging
import sys

from google.appengine.api import datastore  # For taskqueue coordination
from google.appengine.api import datastore_errors
from google.appengine.api import memcache

from google.appengine.datastore import datastore_rpc

import ndb.key
from ndb import model, tasklets, eventloop, utils


class ContextOptions(datastore_rpc.Configuration):
  """Configuration options that may be passed along with get/put/delete."""

  @datastore_rpc.ConfigOption
  def ndb_should_cache(value):
    if not isinstance(value, bool):
      raise datastore_errors.BadArgumentError(
        'ndb_should_cache should be a bool (%r)' % (value,))
    return value

  @datastore_rpc.ConfigOption
  def ndb_should_memcache(value):
    if not isinstance(value, bool):
      raise datastore_errors.BadArgumentError(
        'ndb_should_memcache should be a bool (%r)' % (value,))
    return value


  @datastore_rpc.ConfigOption
  def ndb_memcache_timeout(value):
    if not isinstance(value, (int, long, float)):
      raise datastore_errors.BadArgumentError(
        'ndb_memcache_timeout should be a number (%r)' % (value,))
    return value


def _make_ctx_options(ctx_options):
  """Helper to construct a ContextOptions object from keyword arguents.

  Args:
    ctx_options: a dict of keyword arguments.

  Note that either 'options' or 'config' can be used to pass another
  ContextOptions object, but not both.  If another ContextOptions
  object is given it provides default values.

  Returns:
    A ContextOptions object, or None if ctx_options is empty.
  """
  if not ctx_options:
    return None
  if 'options' in ctx_options:
    # Move 'options' to 'config' since that is what QueryOptions() uses.
    assert 'config' not in ctx_options, ctx_options
    ctx_options['config'] = ctx_options.pop('options')
  return ContextOptions(**ctx_options)


class AutoBatcher(object):

  def __init__(self, todo_tasklet):
    # todo_tasklet is a tasklet to be called with list of (future, arg) pairs
    self._todo_tasklet = todo_tasklet
    self._todo = []  # List of (future, arg) pairs
    self._running = None  # Currently running tasklet, if any

  def __repr__(self):
    return '%s(%s)' % (self.__class__.__name__, self._todo_tasklet.__name__)

  def add(self, arg, options):
    fut = tasklets.Future('%s.add(%s, %s)' % (self, arg, options))
    if not self._todo:  # Schedule the callback
      # We use the fact that regular tasklets are queued at time None,
      # which puts them at absolute time 0 (i.e. ASAP -- still on a
      # FIFO basis).  Callbacks explicitly scheduled with a delay of 0
      # are only run after all immediately runnable tasklets have run.
      eventloop.queue_call(0, self._autobatcher_callback)
    self._todo.append((fut, arg, options))
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
    # TODO: Use logging_debug(), at least if len(todo) == 1.
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


class Context(object):

  def __init__(self, conn=None, auto_batcher_class=AutoBatcher, config=None):
    if conn is None:
      conn = model.make_connection(config)
    else:
      assert config is None  # It wouldn't be used.
    self._conn = conn
    self._auto_batcher_class = auto_batcher_class
    self._get_batcher = auto_batcher_class(self._get_tasklet)
    self._put_batcher = auto_batcher_class(self._put_tasklet)
    self._delete_batcher = auto_batcher_class(self._delete_tasklet)
    self._cache = {}
    self._cache_policy = None
    self._memcache_policy = None
    self._memcache_timeout_policy = None
    self._memcache_prefix = 'NDB:'  # TODO: make this configurable.

  @tasklets.tasklet
  def flush(self):
    yield (self._get_batcher.flush(),
           self._put_batcher.flush(),
           self._delete_batcher.flush())

  @tasklets.tasklet
  def _get_tasklet(self, todo):
    assert todo
    # First check memcache.
    memkeymap = {}
    for fut, key, options in todo:
      if self.should_memcache(key, options):
        memkeymap[key] = key.urlsafe()
    if memkeymap:
      results = memcache.get_multi(memkeymap.values(),
                                   key_prefix=self._memcache_prefix)
      leftover = []
      for fut, key, options in todo:
        mkey = memkeymap.get(key)
        if mkey is not None and mkey in results:
          pb = results[mkey]
          ent = self._conn.adapter.pb_to_entity(pb)
          fut.set_result(ent)
        else:
          leftover.append((fut, key, options))
      todo = leftover
    # Make RPC calls, segregated by ConfigOptions.
    by_options = {}
    for fut, key, options in todo:
      if options in by_options:
        futures, keys = by_options[options]
      else:
        futures, keys = by_options[options] = [], []
      futures.append(fut)
      keys.append(key)
    for options, (futures, keys) in by_options.iteritems():
      results = yield self._conn.async_get(options, keys)
      for result, fut in zip(results, futures):
        fut.set_result(result)

  @tasklets.tasklet
  def _put_tasklet(self, todo):
    assert todo
    # TODO: What if the same entity is being put twice?
    # TODO: What if two entities with the same key are being put?
    # TODO: Clear entities from memcache before starting the write?
    # TODO: Attempt to prevent dogpile effect while keeping cache consistent?
    by_options = {}
    for fut, ent, options in todo:
      if options in by_options:
        futures, entities = by_options[options]
      else:
        futures, entities = by_options[options] = [], []
      futures.append(fut)
      entities.append(ent)
    for options, (futures, entities) in by_options.iteritems():
      keys = yield self._conn.async_put(options, entities)
      for key, fut, ent in zip(keys, futures, entities):
        if key != ent._key:
          if ent._has_complete_key():
            raise datastore_errors.BadKeyError(
                'Entity key differs from the one returned by the datastore. '
                'Expected %r, got %r' % (key, ent._key))
          ent._key = key
        fut.set_result(key)
    # Now update memcache.
    # TODO: Could we update memcache *before* calling async_put()?
    # (Hm, not for new entities but possibly for updated ones.)
    mappings = {}  # Maps timeout value to {urlsafe_key: pb} mapping.
    for _, ent, options in todo:
      if self.should_memcache(ent._key, options):
        pb = self._conn.adapter.entity_to_pb(ent)
        timeout = self.get_memcache_timeout(ent._key, options)
        mapping = mappings.get(timeout)
        if mapping is None:
          mapping = mappings[timeout] = {}
        mapping[ent._key.urlsafe()] = pb
    if mappings:
      # If the timeouts are not uniform, make a separate call for each
      # distinct timeout value.
      for timeout, mapping in mappings.iteritems():
        failures = memcache.set_multi(mapping, time=timeout,
                                      key_prefix=self._memcache_prefix)
        if failures:
          badkeys = []
          for failure in failures:
            badkeys.append(mapping[failure].key)
          logging.info('memcache failed to set %d out of %d keys: %s',
                       len(failures), len(mapping), badkeys)

  @tasklets.tasklet
  def _delete_tasklet(self, todo):
    assert todo
    by_options = {}
    for fut, key, options in todo:
      if options in by_options:
        futures, keys = by_options[options]
      else:
        futures, keys = by_options[options] = [], []
      futures.append(fut)
      keys.append(key)
    for options, (futures, keys) in by_options.iteritems():
      yield self._conn.async_delete(options, keys)
      for fut in futures:
        fut.set_result(None)
    # Now update memcache.
    memkeys = []
    for _, key, options in todo:
      if self.should_memcache(key, options):
        memkeys.append(key.urlsafe())
    if memkeys:
      memcache.delete_multi(memkeys, key_prefix=self._memcache_prefix)
      # The value returned by delete_multi() is pretty much useless, it
      # could be the keys were never cached in the first place.

  def get_cache_policy(self):
    """Return the current context cache policy function.

    Returns:
      A function that accepts a Key instance as argument and returns
      a bool indicating if it should be cached.  May be None.
    """
    return self._cache_policy

  def set_cache_policy(self, func):
    """Set the context cache policy function.

    Args:
      func: A function that accepts a Key instance as argument and returns
        a bool indicating if it should be cached.  May be None.
    """
    self._cache_policy = func

  def should_cache(self, key, options=None):
    """Return whether to use the context cache for this key.

    Args:
      key: Key instance.
      options: ContextOptions instance, or None.

    Returns:
      True if the key should be cached, False otherwise.
    """
    flag = getattr(options, 'ndb_should_cache', None)
    if flag is None and self._cache_policy is not None:
      flag = self._cache_policy(key)
    if flag is None:
      flag = getattr(self._conn.config, 'ndb_should_cache', True)
    return flag

  def get_memcache_policy(self):
    """Return the current memcache policy function.

    Returns:
      A function that accepts a Key instance as argument and returns
      a bool indicating if it should be cached.  May be None.
    """
    return self._memcache_policy

  def set_memcache_policy(self, func):
    """Set the memcache policy function.

    Args:
      func: A function that accepts a Key instance as argument and returns
        a bool indicating if it should be cached.  May be None.
    """
    self._memcache_policy = func

  def set_memcache_timeout_policy(self, func):
    """Set the policy function for memcache timeout (expiration).

    Args:
      func: A function that accepts a key instance as argument and returns
        an integer indicating the desired memcache timeout.  May be None.

    If the function returns 0 it implies the default timeout.
    """
    self._memcache_timeout_policy = func

  def get_memcache_timeout_policy(self):
    """Return the current policy function for memcache timeout (expiration)."""
    return self._memcache_timeout_policy

  def should_memcache(self, key, options=None):
    """Return whether to use memcache for this key.

    Args:
      key: Key instance.
      options: ContextOptions instance, or None.

    Returns:
      True if the key should be cached, False otherwise.
    """
    flag = getattr(options, 'ndb_should_memcache', None)
    if flag is None and self._memcache_policy is not None:
      flag = self._memcache_policy(key)
    if flag is None:
      flag = getattr(self._conn.config, 'ndb_should_memcache', True)
    return flag

  def get_memcache_timeout(self, key, options=None):
    """Return the memcache timeout to use for this key."""
    timeout = getattr(options, 'ndb_memcache_timeout', None)
    if timeout is None and self._memcache_timeout_policy is not None:
      timeout = self._memcache_timeout_policy(key)
    if timeout is None:
      timeout = getattr(self._conn.config, 'ndb_memcache_timeout', 0)
    return timeout

  # TODO: What about conflicting requests to different autobatchers,
  # e.g. tasklet A calls get() on a given key while tasklet B calls
  # delete()?  The outcome is nondeterministic, depending on which
  # autobatcher gets run first.  Maybe we should just flag such
  # conflicts as errors, with an overridable policy to resolve them
  # differently?

  @tasklets.tasklet
  def get(self, key, **ctx_options):
    """Return a Model instance given the entity key.

    It will use the context cache if the cache policy for the given
    key is enabled.

    Args:
      key: Key instance.
      **ctx_options: Context options.

    Returns:
      A Model instance it the key exists in the datastore; None otherwise.
    """
    options = _make_ctx_options(ctx_options)
    should_cache = self.should_cache(key, options)
    if should_cache and key in self._cache:
      entity = self._cache[key]  # May be None, meaning "doesn't exist".
      if entity is None or entity._key == key:
        # If entity's key didn't change later, it is ok. See issue #13.
        raise tasklets.Return(entity)
    entity = yield self._get_batcher.add(key, options)
    if should_cache:
      self._cache[key] = entity
    raise tasklets.Return(entity)

  @tasklets.tasklet
  def put(self, entity, **ctx_options):
    options = _make_ctx_options(ctx_options)
    key = yield self._put_batcher.add(entity, options)
    if entity._key != key:
      logging.info('replacing key %s with %s', entity._key, key)
      entity._key = key
    # TODO: For updated entities, could we update the cache first?
    if self.should_cache(key, options):
      # TODO: What if by now the entity is already in the cache?
      self._cache[key] = entity
    raise tasklets.Return(key)

  @tasklets.tasklet
  def delete(self, key, **ctx_options):
    options = _make_ctx_options(ctx_options)
    yield self._delete_batcher.add(key, options)
    if key in self._cache:
      self._cache[key] = None

  @tasklets.tasklet
  def allocate_ids(self, key, size=None, max=None, **ctx_options):
    options = _make_ctx_options(ctx_options)
    lo_hi = yield self._conn.async_allocate_ids(options, key, size, max)
    raise tasklets.Return(lo_hi)

  @datastore_rpc._positional(3)
  def map_query(self, query, callback, options=None, merge_future=None):
    mfut = merge_future
    if mfut is None:
      mfut = tasklets.MultiFuture('map_query')

    @tasklets.tasklet
    def helper():
      try:
        inq = tasklets.SerialQueueFuture()
        query.run_to_queue(inq, self._conn, options)
        is_ancestor_query = query.ancestor is not None
        while True:
          try:
            batch, i, ent = yield inq.getq()
          except EOFError:
            break
          if isinstance(ent, model.Key):
            pass  # It was a keys-only query and ent is really a Key.
          else:
            key = ent._key
            if key in self._cache:
              hit = self._cache[key]
              if hit is not None and hit.key != key:
                # The cached entry has been mutated to have a different key.
                # That's a false hit.  Get rid of it.  See issue #13.
                del self._cache[key]
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
              # Cache the entity only if this is an ancestor query;
              # non-ancestor queries may return stale results, since in
              # the HRD these queries are "eventually consistent".
              # TODO: Shouldn't we check this before considering cache hits?
              if is_ancestor_query and self.should_cache(key, options):
                self._cache[key] = ent
          if callback is None:
            val = ent
          else:
            # TODO: If the callback raises, log and ignore.
            if options is not None and options.produce_cursors:
              val = callback(batch, i, ent)
            else:
              val = callback(ent)
          mfut.putq(val)
      except Exception, err:
        _, _, tb = sys.exc_info()
        mfut.set_exception(err, tb)
        raise
      else:
        mfut.complete()

    helper()
    return mfut

  @datastore_rpc._positional(2)
  def iter_query(self, query, callback=None, options=None):
    return self.map_query(query, callback=callback, options=options,
                          merge_future=tasklets.SerialQueueFuture())

  @tasklets.tasklet
  def transaction(self, callback, retry=3, entity_group=None, **ctx_options):
    # Will invoke callback() one or more times with the default
    # context set to a new, transactional Context.  Returns a Future.
    # Callback may be a tasklet.
    options = _make_ctx_options(ctx_options)
    if entity_group is not None:
      app = entity_group.app()
    else:
      app = ndb.key._DefaultAppId()
    yield self.flush()
    for i in range(1 + max(0, retry)):
      transaction = yield self._conn.async_begin_transaction(options, app)
      tconn = datastore_rpc.TransactionalConnection(
        adapter=self._conn.adapter,
        config=self._conn.config,
        transaction=transaction,
        entity_group=entity_group)
      tctx = self.__class__(conn=tconn,
                            auto_batcher_class=self._auto_batcher_class)
      tctx.set_memcache_policy(lambda key: False)
      tasklets.set_context(tctx)
      old_ds_conn = datastore._GetConnection()
      try:
        datastore._SetConnection(tconn)  # For taskqueue coordination
        try:
          try:
            result = callback()
            if isinstance(result, tasklets.Future):
              result = yield result
          finally:
            yield tctx.flush()
        except Exception, err:
          t, e, tb = sys.exc_info()
          yield tconn.async_rollback(options)  # TODO: Don't block???
          if issubclass(t, datastore_errors.Rollback):
            return
          else:
            raise t, e, tb
        else:
          ok = yield tconn.async_commit(options)
          if ok:
            # TODO: This is questionable when self is transactional.
            self._cache.update(tctx._cache)
            self._clear_memcache(tctx._cache)
            raise tasklets.Return(result)
      finally:
        datastore._SetConnection(old_ds_conn)

    # Out of retries
    raise datastore_errors.TransactionFailedError(
      'The transaction could not be committed. Please try again.')

  def in_transaction(self):
    """Return whether a transaction is currently active."""
    return isinstance(self._conn, datastore_rpc.TransactionalConnection)

  def clear_cache(self):
    """Clears the in-memory cache.

    NOTE: This does not affect memcache.
    """
    self._cache.clear()

  # Backwards compatible alias.
  flush_cache = clear_cache  # TODO: Remove this after one release.

  def _clear_memcache(self, keys):
    keys = set(key for key in keys if self.should_memcache(key))
    if keys:
      memkeys = [key.urlsafe() for key in keys]
      memcache.delete_multi(memkeys, key_prefix=self._memcache_prefix)

  @tasklets.tasklet
  def get_or_insert(self, model_class, name,
                    app=None, namespace=None, parent=None,
                    context_options=None,
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
        ent = yield key.get_async(options=context_options)
        if ent is None:
          ent = model_class(**kwds)  # TODO: Check for forbidden keys
          ent._key = key
          yield ent.put_async(options=context_options)
        raise tasklets.Return(ent)
      ent = yield self.transaction(txn)
    raise tasklets.Return(ent)


def toplevel(func):
  """A sync tasklet that sets a fresh default Context.

  Use this for toplevel view functions such as
  webapp.RequestHandler.get() or Django view functions.
  """
  @utils.wrapping(func)
  def add_context_wrapper(*args, **kwds):
    __ndb_debug__ = utils.func_info(func)
    tasklets.Future.clear_all_pending()
    # Reset context; a new one will be created on the first call to
    # get_context().
    tasklets.set_context(None)
    ctx = tasklets.get_context()
    try:
      return tasklets.synctasklet(func)(*args, **kwds)
    finally:
      eventloop.run()  # Ensure writes are flushed, etc.
  return add_context_wrapper
