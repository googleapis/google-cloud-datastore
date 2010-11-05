"""Context class."""

import logging

from ndb import model, tasks, eventloop

class AutoBatcher(object):

  def __init__(self, method, options=None):
    self._todo = []
    self._method = method  # conn.async_get, conn.async_put, conn.async_delete
    self._options = options  # datastore_rpc.Configuration

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

  def _callback(self):
    # We cannot postpone the inevitable any longer.
    assert self._todo
    args = [arg for (fut, arg) in self._todo]
    logging.info('AutoBatcher(%s): %d items', self._method.__name__, len(args))
    rpc = self._method(self._options, args)
    eventloop.queue_rpc(rpc, self._rpc_callback, rpc, self._todo)
    self._todo = []  # Get ready for the next batch

  def _rpc_callback(self, rpc, todo):
    values = rpc.get_result()  # TODO: What if it raises?
    if values is None:  # For delete
      for fut, arg in todo:
        fut.set_result(None)
    else:
      for (fut, arg), val in zip(todo, values):
        fut.set_result(val)

class Context(object):

  def __init__(self, conn=None, auto_batcher_class=AutoBatcher):
    if conn is None:
      conn = model.conn  # TODO: Get rid of this?
    self._conn = conn
    self._auto_batcher_class = auto_batcher_class
    self._get_batcher = auto_batcher_class(self._conn.async_get)
    self._put_batcher = auto_batcher_class(self._conn.async_put)
    self._delete_batcher = auto_batcher_class(self._conn.async_delete)

  def get(self, key):
      return self._get_batcher.add(key)

  def put(self, ent):
      return self._put_batcher.add(ent)

  def delete(self, key):
      return self._delete_batcher.add(key)

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
          count += 1
          val = callback(ent)  # TODO: If this raises something, log and ignore
          if isinstance(val, tasks.Future):
            mfut.add_dependent(val)
          else:
            mfut.process_value(val)
      mfut.complete()
      raise tasks.Return(count)

    return mfut, helper()

  # TODO: allocate_ids().
  
  def async_transaction(self, callback, retry=3):
    # Will invoke callback(ctx) one or more times with ctx set to a new,
    # transactional Context.  Returns a Future.  Callback must be a task.
    @tasks.task
    def try_several_times():
      for i in range(1 + max(0, retry)):
        tconn = self._conn.new_transaction()
        tctx = self.__class__(conn=tconn,
                              auto_batcher_class=self._auto_batcher_class)
        fut = callback(tctx)
        assert isinstance(fut, tasks.Future)
        try:
          result = yield fut
        except Exception, err:
          yield tconn.async_rollback(None)  # TODO: Don't block???
          raise
        else:
          ok = yield tconn.async_commit(None)
          if ok:
            raise tasks.Return(result)
      # Out of retries
      raise RuntimeError('Transaction retried too many times')  # XXX
    return try_several_times()

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
      fut = self.async_transaction(txn)
      ent = yield fut
    raise tasks.Return(ent)
    

# TODO: Is this a good idea?
def add_context(func):
  """Decorator that adds a fresh Context as self.ctx."""
  def add_context_wrapper(self, *args):
    self.ctx = Context()
    return func(self, *args)
  return add_context_wrapper
