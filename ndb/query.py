"""Higher-level Query wrapper."""

import heapq

from google.appengine.api import datastore_errors
from google.appengine.api import datastore_types
from google.appengine.datastore import datastore_query
from google.appengine.datastore import datastore_rpc
from google.appengine.ext import gql

from ndb import context
from ndb import model
from ndb import tasklets


ASC = datastore_query.PropertyOrder.ASCENDING
DESC = datastore_query.PropertyOrder.DESCENDING

_AND = datastore_query.CompositeFilter.AND

_OPS = {
  '__eq': '=',
  '__ne': '!=',
  '__lt': '<',
  '__le': '<=',
  '__gt': '>',
  '__ge': '>=',
  '__in': 'in',
  }


class Binding(object):

  def __init__(self, value=None, key=None):
    self.value = value
    self.key = key

  def __repr__(self):
    return '%s(%r, %r)' % (self.__class__.__name__, self.value, self.key)

  def __eq__(self, other):
    if not isinstance(other, Binding):
      return NotImplemented
    return self.value == other.value and self.key == other.key

  def resolve(self):
    value = self.value
    assert not isinstance(value, Binding)
    return value


class Node(object):

  def __new__(cls):
    assert cls is not None
    return super(Node, cls).__new__(cls)

  def __eq__(self, other):
    return NotImplemented

  def __ne__(self, other):
    eq = self.__eq__(other)
    if eq is NotImplemented:
      eq = not eq
    return eq

  def __unordered(self, other):
    raise TypeError('Nodes cannot be ordered')
  __le__ = __lt__ = __ge__ = __gt__ = __unordered

  def _to_filter(self, bindings):
    raise NotImplementedError

  def resolve(self):
    raise NotImplementedError


class FalseNode(Node):

  def __new__(cls):
    return super(Node, cls).__new__(cls)

  def __eq__(self, other):
    if not isinstane(other, FalseNode):
      return NotImplemented
    return True

  def _to_filter(self, bindings):
    # TODO: Or use make_filter(name, '=', []) ?
    raise ValueError('Cannot convert FalseNode to predicate')

  def resolve(self):
    return self


class FilterNode(Node):

  def __new__(cls, name, opsymbol, value):
    if opsymbol == '!=':
      n1 = FilterNode(name, '<', value)
      n2 = FilterNode(name, '>', value)
      return DisjunctionNode([n1, n2])
    if opsymbol == 'in' and not isinstance(value, Binding):
      assert isinstance(value, (list, tuple, set, frozenset)), value
      nodes = [FilterNode(name, '=', v) for v in value]
      if not nodes:
        return FalseNode()
      if len(nodes) == 1:
        return nodes[0]
      return DisjunctionNode(nodes)
    self = super(FilterNode, cls).__new__(cls)
    self.__name = name
    self.__opsymbol = opsymbol
    self.__value = value
    return self

  def _sort_key(self):
    return (self.__name, self.__opsymbol, self.__value)

  def __repr__(self):
    return '%s(%r, %r, %r)' % (self.__class__.__name__,
                               self.__name, self.__opsymbol, self.__value)

  def __eq__(self, other):
    if not isinstance(other, FilterNode):
      return NotImplemented
    return (self.__name == other.__name and
            self.__opsymbol == other.__opsymbol and
            self.__value == other.__value)

  def __lt__(self, other):
    if not isinstance(other, FilterNode):
      return NotImplemented
    return self._sort_key() < other._sort_key()
    

  def _to_filter(self, bindings):
    assert self.__opsymbol not in ('!=', 'in'), self.__opsymbol
    value = self.__value
    if isinstance(value, Binding):
      bindings[value.key] = value
      value = value.resolve()
    return datastore_query.make_filter(self.__name, self.__opsymbol, value)

  def resolve(self):
    if self.__opsymbol == 'in':
      assert isinstance(self.__value, Binding)
      return FilterNode(self.__name, self.__opsymbol, self.__value.resolve())
    else:
      return self


class ConjunctionNode(Node):
  # AND

  def __new__(cls, nodes):
    assert nodes
    if len(nodes) == 1:
      return nodes[0]
    clauses = [[]]  # Outer: Disjunction; inner: Conjunction.
    # TODO: Remove duplicates?
    for node in nodes:
      assert isinstance(node, Node), node
      if isinstance(node, DisjunctionNode):
        # Apply the distributive law: (X or Y) and (A or B) becomes
        # (X and A) or (X and B) or (Y and A) or (Y and B).
        new_clauses = []
        for clause in clauses:
          for subnode in node:
            new_clause = clause + [subnode]
            new_clauses.append(new_clause)
        clauses = new_clauses
      elif isinstance(node, ConjunctionNode):
        # Apply half of the distributive law: (X or Y) and A becomes
        # (X and A) or (Y and A).
        for clause in clauses:
          clause.extend(node.__nodes)
      else:
        # Ditto.
        for clause in clauses:
          clause.append(node)
    if not clauses:
      return FalseNode()
    if len(clauses) > 1:
      return DisjunctionNode([ConjunctionNode(clause) for clause in clauses])
    self = super(ConjunctionNode, cls).__new__(cls)
    self.__nodes = clauses[0]
    return self

  def __iter__(self):
    return iter(self.__nodes)

  def __repr__(self):
    return '%s(%r)' % (self.__class__.__name__, self.__nodes)

  def __eq__(self, other):
    if not isinstance(other, ConjunctionNode):
      return NotImplemented
    return self.__nodes == other.__nodes

  def _to_filter(self, bindings):
    filters = [node._to_filter(bindings) for node in self.__nodes]
    return datastore_query.CompositeFilter(_AND, filters)

  def resolve(self):
    nodes = [node.resolve() for node in self.__nodes]
    if nodes == self.__nodes:
      return self
    return ConjunctionNode(nodes)


class DisjunctionNode(Node):
  # OR

  def __new__(cls, nodes):
    assert nodes
    if len(nodes) == 1:
      return nodes[0]
    self = super(DisjunctionNode, cls).__new__(cls)
    self.__nodes = []
    # TODO: Remove duplicates?
    for node in nodes:
      assert isinstance(node, Node), node
      if isinstance(node, DisjunctionNode):
        self.__nodes.extend(node.__nodes)
      else:
        self.__nodes.append(node)
    return self

  def __iter__(self):
    return iter(self.__nodes)

  def __repr__(self):
    return '%s(%r)' % (self.__class__.__name__, self.__nodes)

  def __eq__(self, other):
    if not isinstance(other, DisjunctionNode):
      return NotImplemented
    return self.__nodes == other.__nodes

  def resolve(self):
    nodes = [node.resolve() for node in self.__nodes]
    if nodes == self.__nodes:
      return self
    return DisjunctionNode(nodes)


def _args_to_val(func, args, bindings):
  vals = []
  for arg in args:
    if isinstance(arg, (int, long, basestring)):
      if arg in bindings:
        val = bindings[arg]
      else:
        val = Binding(None, arg)
        bindings[arg] = val
    elif isinstance(arg, gql.Literal):
      val = arg.Get()
    else:
      assert False, 'Unexpected arg (%r)' % arg
    vals.append(val)
  if func == 'nop':
    assert len(vals) == 1
    return vals[0]
  if func == 'list':
    return vals
  if func == 'key':
    if len(vals) == 1 and isinstance(vals[0], basestring):
      return model.Key(urlsafe=vals[0])
    assert False, 'Unexpected key args (%r)' % (vals,)
  assert False, 'Unexpected func (%r)' % func


def parse_gql(query_string):
  """Parse a GQL query string.

  Args:
    query_string: Full GQL query, e.g. 'SELECT * FROM Kind WHERE prop = 1'.

  Returns:
    A tuple (query, options, bindings) where query is a Query instance,
    options a datastore_query.QueryOptions instance, and bindings a dict
    mapping integers and strings to Binding instances.
  """
  gql_qry = gql.GQL(query_string)
  ancestor = None
  flt = gql_qry.filters()
  bindings = {}
  filters = []
  for ((name, op), values) in flt.iteritems():
    op = op.lower()
    if op == 'is' and name == gql.GQL._GQL__ANCESTOR:
      assert len(values) == 1
      [(func, args)] = values
      ancestor = _args_to_val(func, args, bindings)
      continue
    assert op in _OPS.values()
    for (func, args) in values:
      val = _args_to_val(func, args, bindings)
      filters.append(FilterNode(name, op, val))
  if filters:
    filters.sort()  # For predictable tests.
    filter = ConjunctionNode(filters)
  else:
    filter = None
  order = gql_qry.orderings() or None
  qry = Query(kind=gql_qry._entity,
              ancestor=ancestor,
              filter=filter,
              order=order)
  offset = gql_qry.offset()
  if offset < 0:
    offset = None
  limit = gql_qry.limit()
  if limit < 0:
    limit = None
  options = datastore_query.QueryOptions(offset=offset, limit=limit)
  return qry, options, bindings


class Query(object):

  @datastore_rpc._positional(1)
  def __init__(self, kind=None, ancestor=None, filter=None, order=None):
    if ancestor is not None and not isinstance(ancestor, Binding):
      lastid = ancestor.pairs()[-1][1]
      assert lastid, 'ancestor cannot be an incomplete key'
    self.__kind = kind  # String
    self.__ancestor = ancestor  # Key
    self.__filter = filter  # Node subclass
    self.__order = order  # List/tuple of (propname, direction)
    self.__query = None  # Cache for datastore_query.Query instance

  def _get_query(self, connection):
    if self.__query is not None:
      return self.__query
    kind = self.__kind
    ancestor = self.__ancestor
    bindings = {}
    if isinstance(ancestor, Binding):
      bindings[ancestor.key] = ancestor
      ancestor = ancestor.resolve()
    filter = self.__filter
    order = self.__order
    if ancestor is not None:
      ancestor = model.conn.adapter.key_to_pb(ancestor)
    if filter is not None:
      filter = filter._to_filter(bindings)
    if order:
      order = [datastore_query.PropertyOrder(*o) for o in order]
      if len(order) == 1:
        order = order[0]
      else:
        order = datastore_query.CompositeOrder(order)
    dsqry = datastore_query.Query(kind=kind, ancestor=ancestor,
                                  filter_predicate=filter, order=order)
    if not bindings:
      self.__query = dsqry
    return dsqry

  @tasklets.tasklet
  def run_to_queue(self, queue, conn, options=None):
    """Run this query, putting entities into the given queue."""
    multiquery = self._maybe_multi_query()
    if multiquery is not None:
      multiquery.run_to_queue(queue, conn, options=options)  # No return value.
      return
    rpc = self._get_query(conn).run_async(conn, options)
    while rpc is not None:
      batch = yield rpc
      rpc = batch.next_batch_async(options)
      for ent in batch.results:
        queue.putq(ent)
    queue.complete()

  def _maybe_multi_query(self):
    filter = self.__filter
    if filter is not None:
      filter = filter.resolve()
      if isinstance(filter, DisjunctionNode):
        # Switch to a MultiQuery.
        subqueries = []
        for subfilter in filter:
          subquery = Query(kind=self.__kind, ancestor=self.__ancestor,
                           filter=subfilter, order=self.__order)
          subqueries.append(subquery)
        return MultiQuery(subqueries, order=self.__order)
    return None

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
          pred = FilterNode(name, opsymbol, value)
          preds.append(pred)
          break
      else:
        if '__' not in key:
          pred = FilterNode(key, '=', value)
          preds.append(pred)
        else:
          assert False, 'No valid operator (%r)' % key  # TODO: proper exc.
    if not preds:
      pred = None
    elif len(preds) == 1:
      pred = preds[0]
    else:
      pred = ConjunctionNode(preds)
    return self.__class__(kind=self.kind, ancestor=self.ancestor,
                          order=self.order, filter=pred)

  # TODO: Add or_where() -- client-side query merging.

  def order_by(self, *args):
    # q.order_by('prop1', ('prop2', DESC))
    # TODO: Again with the renamed properties.
    if not args:
      return self
    order = []
    o = self.order
    if o:
      order.extend(o)
    for arg in args:
      if isinstance(arg, tuple):
        propname, direction = arg
        assert direction in (ASC, DESC), direction
      else:
        propname = arg
        direction = ASC
      order.append((propname, direction))
    return self.__class__(kind=self.kind, ancestor=self.ancestor,
                          filter=self.filter, order=order)

  def order_by_desc(self, *args):
    # q.order_by_desc('prop1', 'prop2') is equivalent to
    # q.order_by(('prop1', DESC), ('prop2', DESC)).
    order = []
    for arg in args:
      assert isinstance(arg, basestring)
      order.append((arg, DESC))
    return self.order_by(*order)

  # Datastore API using the default context.

  def iter(self, options=None):
    return QueryIterator(self, options=options)

  __iter__ = iter

  # TODO: support the rest for MultiQuery.

  def map(self, callback, options=None, merge_future=None):
    return self.map_async(callback, options=options,
                          merge_future=merge_future).get_result()

  def map_async(self, callback, options=None, merge_future=None):
    return tasklets.get_context().map_query(self, callback,
                                            options=options,
                                            merge_future=merge_future)

  def fetch(self, limit, offset=0):
    return self.fetch_async(limit, offset).get_result()

  @tasklets.tasklet
  def fetch_async(self, limit, offset=0):
    options = datastore_query.QueryOptions(limit=limit,
                                           prefetch_size=limit,
                                           batch_size=limit,
                                           offset=offset)
    res = []
    it = self.iter(options)
    while (yield it.has_next_async()):
      res.append(it.next())
    raise tasklets.Return(res)

  def count(self, limit):
    return self.count_async(limit).get_result()

  @tasklets.tasklet
  def count_async(self, limit):
    conn = tasklets.get_context()._conn
    options = datastore_query.QueryOptions(offset=limit, limit=0)
    rpc = self._get_query(conn).run_async(conn, options)
    total = 0
    while rpc is not None:
      batch = yield rpc
      rpc = batch.next_batch_async(options)
      total += batch.skipped_results
    raise tasklets.Return(total)


class QueryIterator(object):
  """This iterator works both for synchronous and async callers!

  For synchronous callers, just use:

    for entity in Account.all():
      <use entity>

  Async callers use this idiom:

    it = iter(Account.all())
    while (yield it.has_next_async()):
      entity = it.next()
      <use entity>
  """

  def __init__(self, query, options=None):
    ctx = tasklets.get_context()
    self._iter = ctx.iter_query(query, options=options)
    self._fut = None

  def __iter__(self):
    return self

  def has_next(self):
    return self.has_next_async().get_result()

  @tasklets.tasklet
  def has_next_async(self):
    if self._fut is None:
      self._fut = self._iter.getq()
    flag = True
    try:
      yield self._fut
    except EOFError:
      flag = False
    raise tasklets.Return(flag)

  def next(self):
    if self._fut is None:
      self._fut = self._iter.getq()
    try:
      try:
        return self._fut.get_result()
      except EOFError:
        raise StopIteration
    finally:
      self._fut = None


class _SubQueryIteratorState(object):
  # Helper class for MultiQuery.

  def __init__(self, entity, iterator, order):
    self.entity = entity
    self.iterator = iterator
    self.order = order

  def __cmp__(self, other):
    assert isinstance(other, _SubQueryIteratorState)
    assert self.order == other.order
    our_entity = self.entity
    their_entity = other.entity
    # TODO: Renamed properties again.
    if self.order:
      for propname, direction in self.order:
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
  # Queries' methods, e.g. offset (though not necessarily limit, and
  # I'm not sure about cursors).

  def __init__(self, subqueries, order=None):
    assert isinstance(subqueries, list), subqueries
    assert all(isinstance(subq, Query) for subq in subqueries), subqueries
    self.__subqueries = subqueries
    self.__order = order
    self.ancestor = None  # Hack for map_query().

  @tasklets.tasklet
  def run_to_queue(self, queue, conn, options=None):
    """Run this query, putting entities into the given queue."""
    # Create a list of (first-entity, subquery-iterator) tuples.
    # TODO: Use the specified sort order.
    assert options is None  # Don't know what to do with these yet.
    state = []
    for subq in self.__subqueries:
      subit = tasklets.SerialQueueFuture('MultiQuery.run_to_queue')
      subq.run_to_queue(subit, conn)
      try:
        ent = yield subit.getq()
      except EOFError:
        continue
      else:
        state.append(_SubQueryIteratorState(ent, subit, self.__order))

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
        queue.putq(ent)
      subit = item.iterator
      try:
        ent = yield subit.getq()
      except EOFError:
        pass
      else:
        item.entity = ent
        heapq.heappush(state, item)
    queue.complete()

  # Datastore API using the default context.

  def iter(self, options=None):
    return QueryIterator(self, options=options)

  __iter__ = iter
