"""Context class."""

# TODO: Handle things like request size limits.  E.g. what if we've
# batched up 1000 entities to put and now the memcache call fails?

import logging
import sys

from google.appengine.api import datastore_errors
from google.appengine.api import memcache

from google.appengine.datastore import datastore_rpc

from ndb import model, tasks, eventloop, utils

class AutoBatcher(object):

  def __init__(self, todo_task):
    self._todo_task = todo_task  # Task called with list of (future, arg) pairs
    self._todo = []  # List of (future, arg) pairs
    self._running = None  # Currently running task, if any

  def __repr__(self):
    return '%s(%s)' % (self.__class__.__name__, self._todo_task.__name__)

  def add(self, arg):
    fut = tasks.Future('%s.add(%s)' % (self, arg))
    if not self._todo:  # Schedule the callback
      # We use the fact that regular tasks are queued at time None,
      # which puts them at absolute time 0 (i.e. ASAP -- still on a
      # FIFO basis).  Callbacks explicitly scheduled with a delay of 0
      # are only run after all immediately runnable tasks have run.
      eventloop.queue_task(0, self._autobatcher_callback)
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
                 self._todo_task.__name__, len(todo))
    self._running = self._todo_task(todo)
    # Add a callback to the Future to propagate exceptions,
    # since this Future is not normally checked otherwise.
    self._running.add_callback(self._running.check_success)

  @tasks.task
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
      conn = model.conn  # TODO: Get rid of this?
    self._conn = conn
    self._auto_batcher_class = auto_batcher_class
    self._get_batcher = auto_batcher_class(self._get_task)
    self._put_batcher = auto_batcher_class(self._put_task)
    self._delete_batcher = auto_batcher_class(self._delete_task)
    self._cache = {}
    self._cache_policy = None
    self._memcache_policy = None
    # TODO: Also add a way to compute the memcache expiration time.

  @tasks.task
  def flush(self):
    yield (self._get_batcher.flush(),
           self._put_batcher.flush(),
           self._delete_batcher.flush())

  @tasks.task
  def _get_task(self, todo):
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

  @tasks.task
  def _put_task(self, todo):
    assert todo
    # TODO: What if the same entity is being put twice?
    # TODO: What if two entities with the same key are being put?
    # TODO: Clear entities from memcache before starting the write?
    # TODO: Attempt to prevent dogpile effect while keeping cache consistent?
    ents = [ent for (_, ent) in todo]
    results = yield self._conn.async_put(None, ents)
    for key, (fut, ent) in zip(results, todo):
      if key != ent.key:
        assert ent.key is None or not list(ent.key.flat())[-1]
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

  @tasks.task
  def _delete_task(self, todo):
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

  def set_cache_policy(self, func):
    self._cache_policy = func

  def should_cache(self, key):
    # TODO: Don't need this, set_cache_policy() could substitute a lambda.
    if self._cache_policy is None:
      return True
    return self._cache_policy(key)

  def set_memcache_policy(self, func):
    self._memcache_policy = func

  def should_memcache(self, key):
    # TODO: Don't need this, set_memcache_policy() could substitute a lambda.
    if self._memcache_policy is None:
      return True
    return self._memcache_policy(key)

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
      if self.should_cache(key):
        self._cache[key] = entity
    raise tasks.Return(entity)

  @tasks.task
  def put(self, entity):
    key = yield self._put_batcher.add(entity)
    if entity.key != key:
      logging.info('replacing key %s with %s', entity.key, key)
      entity.key = key
    # TODO: For updated entities, could we update the cache first?
    if self.should_cache(key):
      # TODO: What if by now the entity is already in the cache?
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

  @datastore_rpc._positional(3)
  def map_query(self, query, callback, options=None, merge_future=None):
    mfut = merge_future
    if mfut is None:
      mfut = tasks.MultiFuture('map_query')

    @tasks.task
    def helper():
      inq = tasks.SerialQueueFuture()
      query.run_to_queue(inq, self._conn, options)
      while True:
        try:
          ent = yield inq.getq()
        except EOFError:
          break
        key = ent.key
        if key in self._cache:
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
          if self.should_cache(key):
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
                          merge_future=tasks.SerialQueueFuture())

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
      tctx.set_memcache_policy(lambda key: False)
      set_default_context(None)
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
          # TODO: This is questionable when self is transactional.
          self._cache.update(tctx._cache)
          self._flush_memcache(tctx._cache)
          raise tasks.Return(result)
    # Out of retries
    raise datastore_errors.TransactionFailedError(
      'The transaction could not be committed. Please try again.')

  def _flush_memcache(self, keys):
    keys = set(key for key in keys if self.should_memcache(key))
    if keys:
      memkeys = [key.urlsafe() for key in keys]
      memcache.delete_multi(memkeys)

  @tasks.task
  def get_or_insert(self, model_class, name, parent=None, **kwds):
    # TODO: Test the heck out of this, in all sorts of evil scenarios.
    assert isinstance(name, basestring) and name
    if parent is None:
      pairs = []
    else:
      pairs = list(parent.pairs())
    pairs.append((model_class.GetKind(), name))
    key = model.Key(pairs=pairs)
    # TODO: Can (and should) the cache be trusted here?
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


def toplevel(func):
  """Decorator that adds a fresh Context as self.ctx *and* taskifies it."""
  @utils.wrapping(func)
  def add_context_wrapper(self, *args):
    __ndb_debug__ = utils.func_info(func)
    tasks.Future.clear_all_pending()
    self.ctx = Context()
    return tasks.taskify(func)(self, *args)
  return add_context_wrapper


# TODO: Use thread-local for this.
_default_context = None

def get_default_context():
  return _default_context

def set_default_context(new_context):
  assert (new_context is None or
          isinstance(new_context, Context)), repr(new_context)
  global _default_context
  _default_context = new_context


# TODO: Rename to something less cute.
class MagicFuture(tasks.Future):
  """A Future that keeps track of a default Context for its task."""

  def __init__(self, info, default_context):
    assert (default_context is None or
            isinstance(default_context, Context)), repr(default_context)
    super(MagicFuture, self).__init__(info)
    self.default_context = default_context

  def _help_task_along(self, gen, val=None, exc=None, tb=None):
    save_context = get_default_context()
    try:
      set_default_context(self.default_context)
      super(MagicFuture, self)._help_task_along(gen, val=val, exc=exc, tb=tb)
    finally:
      set_default_context(save_context)


def task(func):
  """Decorator like @tasks.task that maintains a default Context."""

  @utils.wrapping(func)
  def context_task_wrapper(*args, **kwds):

    # TODO: make most of this a public function so you can take a bare
    # generator and turn it into a task dynamically.  (Monocle has
    # this I believe.)
    # __ndb_debug__ = utils.func_info(func)
    fut = MagicFuture('context.task %s' % utils.func_info(func),
                      get_default_context())
    try:
      result = func(*args, **kwds)
    except StopIteration, err:
      # Just in case the function is not a generator but still uses
      # the "raise Return(...)" idiom, we'll extract the return value.
      result = get_return_value(err)
    if tasks.is_generator(result):
      eventloop.queue_task(None, fut._help_task_along, result)
    else:
      fut.set_result(result)
    return fut

  return context_task_wrapper


def taskify(func):
  # TODOL Update docstring?
  """Decorator to run a function as a task when called.

  Use this to wrap a request handler function that will be called by
  some web application framework (e.g. a Django view function or a
  webapp.RequestHandler.get method).
  """
  @utils.wrapping(func)
  def context_taskify_wrapper(*args):
    __ndb_debug__ = utils.func_info(func)
    tasks.Future.clear_all_pending()
    set_default_context(Context())
    taskfunc = task(func)
    return taskfunc(*args).get_result()
  return context_taskify_wrapper


# Functions using the default context.

def get(*args, **kwds):
  return get_default_context().get(*args, **kwds)

def put(*args, **kwds):
  return get_default_context().put(*args, **kwds)

def delete(*args, **kwds):
  return get_default_context().delete(*args, **kwds)

def allocate_ids(*args, **kwds):
  return get_default_context().allocate_ids(*args, **kwds)

def map_query(*args, **kwds):
  return get_default_context().map_query(*args, **kwds)

def iter_query(*args, **kwds):
  return get_default_context().iter_query(*args, **kwds)

def transaction(callback, *args, **kwds):
  def callback_wrapper(ctx):
    # TODO: Is this right?
    save_context = get_default_context()
    try:
      set_default_context(ctx)
      return callback()
    finally:
      set_default_context(save_context)
  return get_default_context().transaction(callback_wrapper, *args, **kwds)

def get_or_insert(*args, **kwds):
  return get_default_context().get_or_insert(*args, **kwds)

# TODO: Add flush() and cache policy API?
