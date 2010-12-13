"""Higher-level Query wrapper."""

import heapq

from google.appengine.api import datastore_errors
from google.appengine.api import datastore_types
from google.appengine.datastore import datastore_query
from google.appengine.datastore import datastore_rpc

from ndb import context
from ndb import model
from ndb import tasks


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

  def __init__(self, value=None):
    self.value = value

  def __repr__(self):
    return '%s(%r)' % (self.__class__.__name__, self.value)

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

  def _to_filter(self):
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

  def _to_filter(self):
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

  def __repr__(self):
    return '%s(%r, %r, %r)' % (self.__class__.__name__,
                               self.__name, self.__opsymbol, self.__value)

  def __eq__(self, other):
    if not isinstance(other, FilterNode):
      return NotImplemented
    return (self.__name == other.__name and
            self.__opsymbol == other.__opsymbol and
            self.__value == other.__value)

  def _to_filter(self):
    assert self.__opsymbol not in ('!=', 'in'), self.__opsymbol
    value = self.__value
    if isinstance(value, Binding):
      value = self.resolve()
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

  def _to_filter(self):
    filters = [node._to_filter() for node in self.__nodes]
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


class Query(object):

  # TODO: Add an all() or select() class method to Model that returns
  # a Query instance.

  @datastore_rpc._positional(1)
  def __init__(self, kind=None, ancestor=None, filter=None, order=None):
    if ancestor is not None:
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
    filter = self.__filter
    order = self.__order
    if ancestor is not None:
      ancestor = model.conn.adapter.key_to_pb(ancestor)
    if filter is not None:
      filter = filter._to_filter()
    if order:
      order = [datastore_query.PropertyOrder(*o) for o in order]
      if len(order) == 1:
        order = order[0]
      else:
        order = datastore_query.CompositeOrder(order)
    # TODO: Don't cache if there were Bindings, or invalidate cache
    # if bound values change.
    self.__query = datastore_query.Query(kind=kind, ancestor=ancestor,
                                         filter_predicate=filter,
                                         order=order)
    return self.__query

  def run_async(self, connection, options=None):
    return self._get_query(connection).run_async(connection, options)

  def run(self, connection, options=None):
    return self._get_query(connection).run(connection, options)

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

  # TODO: Pick a better name; decide on context or not.
  def looper(self, ctx=None, options=None):
    if ctx is None:
      ctx = context.get_default_context()
    multiquery = self._maybe_multi_query()
    if multiquery is not None:
      return multiquery.looper(ctx=ctx, options=options)
    qf = tasks.SerialQueueFuture()
    ctx.map_query(query=self, callback=None, options=options, merge_future=qf)
    # XXX The map_query() returns a Future that nobody will wait for.
    return qf

  # NOTE: This is an iterating generator, not a coroutine!
  def iterate(self, connection, options=None):
    multiquery = self._maybe_multi_query()
    if multiquery is not None:
      for result in multiquery.iterate(connection, options):
        yield result
      return
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

  def order_by(self, *args, **kwds):
    # q.order(prop1=ASC).order(prop2=DESC)
    # or q.order('prop1', ('prop2', DESC))
    # TODO: Again with the renamed properties.
    if not args and not kwds:
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
  # Query's run_async() methode, e.g. offset (though not necessarily
  # limit, and I'm not sure about cursors).

  def __init__(self, subqueries, order=None):
    assert isinstance(subqueries, list), subqueries
    assert all(isinstance(subq, Query) for subq in subqueries), subqueries
    self.__subqueries = subqueries
    self.__order = order

  # TODO: Implement equivalents to run() and run_async().  The latter
  # is needed so we can use this with map_query().

  # TODO: Pick a better name; decide on context or not.
  def looper(self, ctx=None, options=None):
    assert options is None  # Don't know what to do with these yet.
    if ctx is None:
      ctx = context.get_default_context()
    qf = tasks.SerialQueueFuture()
    @tasks.task
    def inner():
      # For comments, see iterate() below.
      state = []
      for subq in self.__subqueries:
        subit = subq.looper(ctx)
        try:
          ent = yield subit.getq()
        except EOFError:
          continue
        else:
          state.append(_SubQueryIteratorState(ent, subit, self.__order))
      heapq.heapify(state)
      keys_seen = set()
      while state:
        item = heapq.heappop(state)
        ent = item.entity
        if ent.key not in keys_seen:
          keys_seen.add(ent.key)
          qf.putq(ent)
        subit = item.iterator
        try:
          ent = yield subit.getq()
        except EOFError:
          pass
        else:
          item.entity = ent
          heapq.heappush(state, item)
      qf.complete()
    inner()  # XXX But nobody will be waiting for this.
    return qf

  # NOTE: This is an iterating generator, not a coroutine!
  def iterate(self, connection, options=None):
    assert options is None  # Don't know what to do with these yet.
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
        yield ent
      try:
        item.entity = item.iterator.next()
      except StopIteration:
        # The subquery is exhausted, so just forget about it.
        continue
      heapq.heappush(state, item)
