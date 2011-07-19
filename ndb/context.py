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

_LOCK_TIME = 32  # Time to lock out memcache.add() after datastore.put().


class ContextOptions(datastore_rpc.Configuration):
  """Configuration options that may be passed along with get/put/delete."""

  # TODO: Remove this method once datastore_rpc.Configuration implements it.
  def __hash__(self):
    return hash(frozenset(self._values.iteritems()))

  @datastore_rpc.ConfigOption
  def use_cache(value):
    if not isinstance(value, bool):
      raise datastore_errors.BadArgumentError(
        'use_cache should be a bool (%r)' % (value,))
    return value

  @datastore_rpc.ConfigOption
  def use_memcache(value):
    if not isinstance(value, bool):
      raise datastore_errors.BadArgumentError(
        'use_memcache should be a bool (%r)' % (value,))
    return value

  @datastore_rpc.ConfigOption
  def use_datastore(value):
    if not isinstance(value, bool):
      raise datastore_errors.BadArgumentError(
        'use_datastore should be a bool (%r)' % (value,))
    return value

  @datastore_rpc.ConfigOption
  def memcache_timeout(value):
    if not isinstance(value, (int, long)):
      raise datastore_errors.BadArgumentError(
        'memcache_timeout should be an integer (%r)' % (value,))
    return value


# For backwards compatibility, translate these option names.
_OPTION_TRANSLATIONS = {
  'options': 'config',
  'ndb_should_cache': 'use_cache',
  'ndb_should_memcache': 'use_memcache',
  'ndb_memcache_timeout': 'memcache_timeout',
}


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
  for key in list(ctx_options):
    translation = _OPTION_TRANSLATIONS.get(key)
    if translation:
      assert translation not in ctx_options, (key, translation)
      if key.startswith('ndb_'):
        logging.warning('Context option %s is deprecated; use %s instead',
                        key, translation)
      ctx_options[translation] = ctx_options.pop(key)
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

  # TODO: Set proper namespace for memcache.

  _memcache_prefix = 'NDB:'  # TODO: Might make this configurable.

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
      if self._use_memcache(key, options):
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
    # Segregate things by ConfigOptions.
    by_options = {}
    for fut, key, options in todo:
      if options in by_options:
        futures, keys = by_options[options]
      else:
        futures, keys = by_options[options] = [], []
      futures.append(fut)
      keys.append(key)
    # Make the RPC calls.
    mappings = {}  # Maps timeout value to {urlsafe_key: pb} mapping.
    for options, (futures, keys) in by_options.iteritems():
      datastore_futures = []
      datastore_keys = []
      for fut, key in zip(futures, keys):
        if self._use_datastore(key, options):
          datastore_keys.append(key)
          datastore_futures.append(fut)
        else:
          fut.set_result(None)
      if datastore_keys:
        entities = yield self._conn.async_get(options, datastore_keys)
        for ent, fut, key in zip(entities, datastore_futures, datastore_keys):
          fut.set_result(ent)
          if ent is not None and self._use_memcache(key, options):
            pb = self._conn.adapter.entity_to_pb(ent)
            timeout = self._get_memcache_timeout(key, options)
            mapping = mappings.get(timeout)
            if mapping is None:
              mapping = mappings[timeout] = {}
            mapping[ent._key.urlsafe()] = pb
    if mappings:
      # If the timeouts are not uniform, make a separate call for each
      # distinct timeout value.
      for timeout, mapping in mappings.iteritems():
        # Use add, not set.  This is a no-op within _LOCK_TIME seconds
        # of the delete done by the most recent write.
        memcache.add_multi(mapping, time=timeout,
                           key_prefix=self._memcache_prefix)

  @tasklets.tasklet
  def _put_tasklet(self, todo):
    assert todo
    # TODO: What if the same entity is being put twice?
    # TODO: What if two entities with the same key are being put?
    by_options = {}
    delete_keys = []  # For memcache.delete_multi().
    mappings = {}  # For memcache.set_multi(), segregated by timeout.
    for fut, ent, options in todo:
      if ent._has_complete_key():
        if self._use_memcache(ent._key, options):
          if self._use_datastore(ent._key, options):
            delete_keys.append(ent._key.urlsafe())
          else:
            pb = self._conn.adapter.entity_to_pb(ent)
            timeout = self._get_memcache_timeout(ent._key, options)
            mapping = mappings.get(timeout)
            if mapping is None:
              mapping = mappings[timeout] = {}
            mapping[ent._key.urlsafe()] = pb
      else:
        key = ent._key
        if key is None:
          # Create a dummy Key to call _use_datastore().
          key = model.Key(ent.__class__, None)
        if not self._use_datastore(key, options):
          raise datastore_errors.BadKeyError(
              'Cannot put incomplete key when use_datastore=False.')
      if options in by_options:
        futures, entities = by_options[options]
      else:
        futures, entities = by_options[options] = [], []
      futures.append(fut)
      entities.append(ent)
    if delete_keys:  # Pre-emptively delete from memcache.
      memcache.delete_multi(delete_keys, seconds=_LOCK_TIME,
                            key_prefix=self._memcache_prefix)
    if mappings:  # Write to memcache (only if use_datastore=False).
      # If the timeouts are not uniform, make a separate call for each
      # distinct timeout value.
      for timeout, mapping in mappings.iteritems():
        # Use add, not set.  This is a no-op within _LOCK_TIME seconds
        # of the delete done by the most recent write.
        memcache.add_multi(mapping, time=timeout,
                           key_prefix=self._memcache_prefix)
    for options, (futures, entities) in by_options.iteritems():
      datastore_futures = []
      datastore_entities = []
      for fut, ent in zip(futures, entities):
        key = ent._key
        if key is None:
          # Pass a dummy Key to _use_datastore().
          key = model.Key(ent.__class__, None)
        if self._use_datastore(key, options):
          datastore_futures.append(fut)
          datastore_entities.append(ent)
        else:
          # TODO: If ent._key is None, this is really lame.
          fut.set_result(ent._key)
      if datastore_entities:
        keys = yield self._conn.async_put(options, datastore_entities)
        for key, fut, ent in zip(keys, datastore_futures, datastore_entities):
          if key != ent._key:
            if ent._has_complete_key():
              raise datastore_errors.BadKeyError(
                  'Entity key differs from the one returned by the datastore. '
                  'Expected %r, got %r' % (key, ent._key))
            ent._key = key
          fut.set_result(key)

  @tasklets.tasklet
  def _delete_tasklet(self, todo):
    assert todo
    by_options = {}
    delete_keys = []  # For memcache.delete_multi()
    for fut, key, options in todo:
      if self._use_memcache(key, options):
        delete_keys.append(key.urlsafe())
      if options in by_options:
        futures, keys = by_options[options]
      else:
        futures, keys = by_options[options] = [], []
      futures.append(fut)
      keys.append(key)
    if delete_keys:  # Pre-emptively delete from memcache.
      memcache.delete_multi(delete_keys, seconds=_LOCK_TIME,
                            key_prefix=self._memcache_prefix)
    for options, (futures, keys) in by_options.iteritems():
      datastore_keys = []
      for key in keys:
        if self._use_datastore(key, options):
          datastore_keys.append(key)
      if datastore_keys:
        yield self._conn.async_delete(options, datastore_keys)
      for fut in futures:
        fut.set_result(None)

  # TODO: Unify the policy docstrings (they're getting too verbose).

  # All the policy functions may also:
  # - be a constant of the right type (instead of a function);
  # - return None (instead of a value of the right type);
  # - be None (instead of a function or constant).

  # Model classes may define class variables or class methods
  # _use_{cache,memcache,datastore} or _memcache_timeout to set the
  # default policy of that type for that class.

  @staticmethod
  def default_cache_policy(key):
    """Default cache policy.

    This defers to _use_cache on the Model class.

    Args:
      key: Key instance.

    Returns:
      A bool or None.
    """
    flag = None
    if key is not None:
      modelclass = model.Model._kind_map.get(key.kind())
      if modelclass is not None:
        policy = getattr(modelclass, '_use_cache', None)
        if policy is not None:
          if isinstance(policy, bool):
            flag = policy
          else:
            flag = policy(key)
    return flag

  _cache_policy = default_cache_policy

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
    if func is None:
      func = self.default_cache_policy
    elif isinstance(func, bool):
      func = lambda key, flag=func: flag
    self._cache_policy = func

  def _use_cache(self, key, options=None):
    """Return whether to use the context cache for this key.

    Args:
      key: Key instance.
      options: ContextOptions instance, or None.

    Returns:
      True if the key should be cached, False otherwise.
    """
    flag = ContextOptions.use_cache(options)
    if flag is None:
      flag = self._cache_policy(key)
    if flag is None:
      flag = ContextOptions.use_cache(self._conn.config)
    if flag is None:
      flag = True
    return flag

  @staticmethod
  def default_memcache_policy(key):
    """Default memcache policy.

    This defers to _use_memcache on the Model class.

    Args:
      key: Key instance.

    Returns:
      A bool or None.
    """
    flag = None
    if key is not None:
      modelclass = model.Model._kind_map.get(key.kind())
      if modelclass is not None:
        policy = getattr(modelclass, '_use_memcache', None)
        if policy is not None:
          if isinstance(policy, bool):
            flag = policy
          else:
            flag = policy(key)
    return flag

  _memcache_policy = default_memcache_policy

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
    if func is None:
      func = self.default_memcache_policy
    elif isinstance(func, bool):
      func = lambda key, flag=func: flag
    self._memcache_policy = func

  def _use_memcache(self, key, options=None):
    """Return whether to use memcache for this key.

    Args:
      key: Key instance.
      options: ContextOptions instance, or None.

    Returns:
      True if the key should be cached in memcache, False otherwise.
    """
    flag = ContextOptions.use_memcache(options)
    if flag is None:
      flag = self._memcache_policy(key)
    if flag is None:
      flag = ContextOptions.use_memcache(self._conn.config)
    if flag is None:
      flag = True
    return flag

  @staticmethod
  def default_datastore_policy(key):
    """Default datastore policy.

    This defers to _use_datastore on the Model class.

    Args:
      key: Key instance.

    Returns:
      A bool or None.
    """
    flag = None
    if key is not None:
      modelclass = model.Model._kind_map.get(key.kind())
      if modelclass is not None:
        policy = getattr(modelclass, '_use_datastore', None)
        if policy is not None:
          if isinstance(policy, bool):
            flag = policy
          else:
            flag = policy(key)
    return flag

  _datastore_policy = default_datastore_policy

  def get_datastore_policy(self):
    """Return the current context datastore policy function.

    Returns:
      A function that accepts a Key instance as argument and returns
      a bool indicating if it should use the datastore.  May be None.
    """
    return self._datastore_policy

  def set_datastore_policy(self, func):
    """Set the context datastore policy function.

    Args:
      func: A function that accepts a Key instance as argument and returns
        a bool indicating if it should use the datastore.  May be None.
    """
    if func is None:
      func = self.default_datastore_policy
    elif isinstance(func, bool):
      func = lambda key, flag=func: flag
    self._datastore_policy = func

  def _use_datastore(self, key, options=None):
    """Return whether to use the datastore for this key.

    Args:
      key: Key instance.
      options: ContextOptions instance, or None.

    Returns:
      True if the datastore should be used, False otherwise.
    """
    flag = ContextOptions.use_datastore(options)
    if flag is None:
      flag = self._datastore_policy(key)
    if flag is None:
      flag = ContextOptions.use_datastore(self._conn.config)
    if flag is None:
      flag = True
    return flag

  @staticmethod
  def default_memcache_timeout_policy(key):
    """Default memcache timeout policy.

    This defers to _memcache_timeout on the Model class.

    Args:
      key: Key instance.

    Returns:
      Memcache timeout to use (integer), or None.
    """
    timeout = None
    if key is not None:
      modelclass = model.Model._kind_map.get(key.kind())
      if modelclass is not None:
        policy = getattr(modelclass, '_memcache_timeout', None)
        if policy is not None:
          if isinstance(policy, (int, long)):
            timeout = policy
          else:
            timeout = policy(key)
    return timeout

  _memcache_timeout_policy = default_memcache_timeout_policy

  def set_memcache_timeout_policy(self, func):
    """Set the policy function for memcache timeout (expiration).

    Args:
      func: A function that accepts a key instance as argument and returns
        an integer indicating the desired memcache timeout.  May be None.

    If the function returns 0 it implies the default timeout.
    """
    if func is None:
      func = self.default_memcache_timeout_policy
    elif isinstance(func, (int, long)):
      func = lambda key, flag=func: flag
    self._memcache_timeout_policy = func

  def get_memcache_timeout_policy(self):
    """Return the current policy function for memcache timeout (expiration)."""
    return self._memcache_timeout_policy

  def _get_memcache_timeout(self, key, options=None):
    """Return the memcache timeout (expiration) for this key."""
    timeout = ContextOptions.memcache_timeout(options)
    if timeout is None:
      timeout = self._memcache_timeout_policy(key)
    if timeout is None:
      timeout = ContextOptions.memcache_timeout(self._conn.config)
    if timeout is None:
      timeout = 0
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
    use_cache = self._use_cache(key, options)
    if use_cache and key in self._cache:
      entity = self._cache[key]  # May be None, meaning "doesn't exist".
      if entity is None or entity._key == key:
        # If entity's key didn't change later, it is ok. See issue #13.
        raise tasklets.Return(entity)
    entity = yield self._get_batcher.add(key, options)
    if use_cache:
      self._cache[key] = entity
    raise tasklets.Return(entity)

  @tasklets.tasklet
  def put(self, entity, **ctx_options):
    options = _make_ctx_options(ctx_options)
    key = yield self._put_batcher.add(entity, options)
    if key is not None:
      if entity._key != key:
        logging.info('replacing key %s with %s', entity._key, key)
        entity._key = key
      # TODO: For updated entities, could we update the cache first?
      if self._use_cache(key, options):
        # TODO: What if by now the entity is already in the cache?
        self._cache[key] = entity
    raise tasklets.Return(key)

  @tasklets.tasklet
  def delete(self, key, **ctx_options):
    options = _make_ctx_options(ctx_options)
    yield self._delete_batcher.add(key, options)
    if self._use_cache(key, options) and key in self._cache:
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
              if is_ancestor_query and self._use_cache(key, options):
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
      tctx.set_memcache_policy(False)
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
    keys = set(key for key in keys if self._use_memcache(key))
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
    tasklets._state.clear_all_pending()
    # Reset context; a new one will be created on the first call to
    # get_context().
    tasklets.set_context(None)
    ctx = tasklets.get_context()
    try:
      return tasklets.synctasklet(func)(*args, **kwds)
    finally:
      eventloop.run()  # Ensure writes are flushed, etc.
  return add_context_wrapper
