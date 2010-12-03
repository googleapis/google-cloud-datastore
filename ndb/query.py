"""Higher-level Query wrapper."""

import heapq

from google.appengine.api import datastore_errors
from google.appengine.api import datastore_types
from google.appengine.datastore import datastore_query
from google.appengine.datastore import datastore_rpc

from ndb import model


ASC = datastore_query.PropertyOrder.ASCENDING
DESC = datastore_query.PropertyOrder.DESCENDING

_AND = datastore_query.CompositeFilter.AND

_OPS = {
  '__eq': '=',
##  '__ne': '!=',
  '__lt': '<',
  '__le': '<=',
  '__gt': '>',
  '__ge': '>=',
##  '__in': 'in',
  }


def make_filter(name, opsymbol, value):
  return datastore_query.make_filter(name, opsymbol, value)


def conjunction(preds):
  return datastore_query.CompositeFilter(_AND, preds)


class Query(object):

  # TODO: Add an all() or select() class method to Model that returns
  # a Query instance.

  @datastore_rpc._positional(1)
  def __init__(self, kind=None, ancestor=None, filter=None, order=None):
    """A wrapper for Query."""
    self.__kind = kind
    self.__ancestor = ancestor
    self.__filter = filter
    self.__order = order
    self.__query = None

  def _get_query(self, connection):
    if self.__query is not None:
      return self.__query
    kind = self.__kind
    ancestor = self.__ancestor
    filter = self.__filter
    order = self.__order
    if ancestor is not None:
      ancestor = model.conn.adapter.key_to_pb(ancestor)
    self.__query = datastore_query.Query(kind=kind, ancestor=ancestor,
                                         filter_predicate=filter,
                                         order=order)
    return self.__query

  def run_async(self, connection, options=None):
    return self._get_query(connection).run_async(connection, options)

  def run(self, connection, options=None):
    return self._get_query(connection).run(connection, options)

  # NOTE: This is an iterating generator, not a coroutine!
  def iterate(self, connection, options=None):
    for batch in self.run(connection, options):
      for result in batch.results:
        yield result

  @property
  def kind(self):
    return self.__kind

  @property
  def ancestor(self):
    return self.__ancestor

  @property
  def filter(self):
    return self.__filter

  @property
  def order(self):
    return self.__order

  def where(self, **kwds):
    # NOTE: Filters specified this way are not ordered; to force
    # ordered filters, use q.filter(...).filter(...).
    # TODO: What about renamed properties?  The kwd should be the
    # Python name, but the Query should use the datastore name.  We'd
    # need the actual Model class to suport this though, or at least
    # the actual Property instance.
    if not kwds:
      return self
    preds = []
    f = self.filter
    if f:
      preds.append(f)
    for key, value in kwds.iteritems():
      for opname, opsymbol in _OPS.iteritems():
        if key.endswith(opname):
          name = key[:-len(opname)]
          pred = make_filter(name, opsymbol, value)
          preds.append(pred)
          break
      else:
        if '__' not in key:
          pred = make_filter(name, '=', value)
        else:
          assert False, 'No valid operator (%r)' % key  # TODO: proper exc.
    if len(preds) == 1:
      pred = preds[0]
    else:
      pred = conjunction(preds)
    return self.__class__(kind=self.kind, ancestor=self.ancestor,
                          order=self.order, filter=pred)

  # TODO: Add or_where() -- client-side query merging.

  def order_by(self, *args, **kwds):
    # q.order(prop1=ASC).order(prop2=DESC)
    # or q.order('prop1', ('prop2', DESC))
    # TODO: Again with the renamed properties.
    if not args and not kwds:
      return self
    orders = []
    o = self.order
    if o:
      orders.append(o)
    for arg in args:
      if isinstance(arg, tuple):
        propname, direction = arg
        assert direction in (ASC, DESC), direction
      else:
        propname = arg
        direction = ASC
      orders.append(datastore_query.PropertyOrder(propname, direction))
    if len(orders) == 1:
      order = orders[0]
    else:
      order = datastore_query.CompositeOrder(orders)
    return self.__class__(kind=self.kind, ancestor=self.ancestor,
                          filter=self.filter, order=order)


class _SubQueryIteratorState(object):
  # Helper class for MultiQuery.

  def __init__(self, entity, iterator, orders):
    self.entity = entity
    self.iterator = iterator
    self.orders = orders

  def __cmp__(self, other):
    assert isinstance(other, _SubQueryIteratorState)
    assert self.orders == other.orders
    our_entity = self.entity
    their_entity = other.entity
    # TODO: Renamed properties again.
    for propname, direction in self.orders:
      our_value = getattr(our_entity, propname, None)
      their_value = getattr(their_entity, propname, None)
      # NOTE: Repeated properties sort by lowest value when in
      # ascending order and highest value when in descending order.
      # TODO: Use min_max_value_cache as datastore.py does?
      if direction == ASC:
        func = min
      else:
        func = max
      if isinstance(our_value, list):
        our_value = func(our_value)
      if isinstance(their_value, list):
        their_value = func(their_value)
      flag = cmp(our_value, their_value)
      if direction == DESC:
        flag = -flag
      if flag:
        return flag
    # All considered properties are equal; compare by key (ascending).
    # TODO: Comparison between ints and strings is arbitrary.
    return cmp(our_entity.key.pairs(), their_entity.key.pairs())


class MultiQuery(object):

  # This is not created by the user directly, but implicitly by using
  # a where() call with an __in or __ne operator.  In the future
  # or_where() can also use this.  Note that some options must be
  # interpreted by MultiQuery instead of passed to the underlying
  # Query's run_async() methode, e.g. offset (though not necessarily
  # limit, and I'm not sure about cursors).

  def __init__(self, subqueries, orders=()):
    assert isinstance(subqueries, list), subqueries
    assert all(isinstance(subq, Query) for subq in subqueries), subqueries
    self.__subqueries = subqueries
    self.__orders = orders

  # TODO: Implement equivalents to run() and run_async().  The latter
  # is needed so we can use this with map_query().

  # NOTE: This is an iterating generator, not a coroutine!
  def iterate(self, connection, options=None):
    # Create a list of (first-entity, subquery-iterator) tuples.
    # TODO: Use the specified sort order.
    state = []
    for subq in self.__subqueries:
      subit = subq.iterate(connection)
      try:
        ent = subit.next()
      except StopIteration:
        # An empty subquery can't contribute, so just skip it.
        continue
      state.append(_SubQueryIteratorState(ent, subit, self.__orders))

    # Now turn it into a sorted heap.  The heapq module claims that
    # calling heapify() is more efficient than calling heappush() for
    # each item.
    heapq.heapify(state)

    # Repeatedly yield the lowest entity from the state vector,
    # filtering duplicates.  This is essentially a multi-way merge
    # sort.  One would think it should be possible to filter
    # duplicates simply by dropping other entities already in the
    # state vector that are equal to the lowest entity, but because of
    # the weird sorting of repeated properties, we have to explicitly
    # keep a set of all keys, so we can remove later occurrences.
    # Yes, this means that the output may not be sorted correctly.
    # Too bad.  (I suppose you can do this in constant memory bounded
    # by the maximum number of entries in relevant repeated
    # properties, but I'm too lazy for now.  And yes, all this means
    # MultiQuery is a bit of a toy.  But where it works, it beats
    # expecting the user to do this themselves.)
    keys_seen = set()
    while state:
      item = heapq.heappop(state)
      ent = item.entity
      if ent.key not in keys_seen:
        keys_seen.add(ent.key)
        yield ent
      try:
        item.entity = item.iterator.next()
      except StopIteration:
        # The subquery is exhausted, so just forget about it.
        continue
      heapq.heappush(state, item)
