"""Higher-level Query wrapper.

There are perhaps too many query APIs in the world.

The fundamental API here overloads the 6 comparisons operators to
represent filters on property values, and supports AND and OR
operations (implemented as functions -- Python's 'and' and 'or'
operators cannot be overloaded, and the '&' and '|' operators have a
priority that conflicts with the priority of comparison operators).
For example:

  class Employee(Model):
    name = StringProperty()
    age = IntegerProperty()
    rank = IntegerProperty()

    @classmethod
    def demographic(cls, min_age, max_age):
      return cls.query().filter(AND(cls.age >= min_age, cls.age <= max_age))

    @classmethod
    def ranked(cls, rank):
      return cls.query(cls.rank == rank).order(cls.age)

  for emp in Employee.seniors(42, 5):
    print emp.name, emp.age, emp.rank

The 'in' operator cannot be overloaded, but is supported through the
IN() method.  For example:

  Employee.query().filter(Employee.rank.IN([4, 5, 6]))

Sort orders are supported through the order() method; unary minus is
overloaded on the Property class to represent a descending order:

  Employee.query().order(Employee.name, -Employee.age)

Besides using AND() and OR(), filters can also be combined by
repeatedly calling .filter():

  q1 = Employee.query()  # A query that returns all employees
  q2 = q1.filter(Employee.age >= 30)  # Only those over 30
  q3 = q2.filter(Employee.age < 40)  # Only those in their 30s

Query objects are immutable, so these methods always return a new
Query object; the above calls to filter() do not affect q1.

Sort orders can also be combined this way, and .filter() and .order()
calls may be intermixed:

  q4 = q3.order(-Employee.age)
  q5 = q4.order(Employee.name)
  q6 = q5.filter(Employee.rank == 5)

The simplest way to retrieve Query results is a for-loop:

  for emp in q3:
    print emp.name, emp.age

Some other operations:

  q.map(callback) # Call the callback function for each query result
  q.fetch(N) # Return a list of the first N results
  q.count(N) # Return the number of results, with a maximum of N

These have asynchronous variants as well, which return a Future; to
get the operation's ultimate result, yield the Future (when inside a
tasklet) or call the Future's get_result() method (outside a tasklet):

  q.map_async(callback)  # Callback may be a task or a plain function
  q.fetch_async(N)
  q.count_async(N)

Finally, there's an idiom to efficiently loop over the Query results
in a tasklet, properly yielding when appropriate:

  it = iter(q)
  while (yield it.has_next_async()):
    emp = it.next()
    print emp.name, emp.age
"""

__author__ = 'guido@google.com (Guido van Rossum)'

import heapq

from google.appengine.api import datastore_errors
from google.appengine.api import datastore_types
from google.appengine.datastore import datastore_query
from google.appengine.datastore import datastore_rpc
from google.appengine.ext import gql

from ndb import context
from ndb import model
from ndb import tasklets

__all__ = ['Binding', 'AND', 'OR', 'parse_gql', 'Query', 'QueryOptions']

QueryOptions = datastore_query.QueryOptions  # For export.

# TODO: Make these protected.
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

  def _post_filters(self):
    return None

  def apply(self, entity):
    return True

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


class PostFilterNode(Node):

  def __new__(cls, filter_func, filter_arg):
    self = super(PostFilterNode, cls).__new__(cls)
    self.filter_func = filter_func
    self.filter_arg = filter_arg
    return self

  def apply(self, entity):
    return self.filter_func(self.filter_arg, entity)

  def __eq__(self, other):
    if not isinstance(other, PostFilterNode):
      return NotImplemented
    return self is other

  def _to_filter(self, bindings):
    return None

  def resolve(self):
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
    filters = filter(None,
                     (node._to_filter(bindings) for node in self.__nodes))
    return datastore_query.CompositeFilter(_AND, filters)

  def _post_filters(self):
    post_filters = [node for node in self.__nodes
                    if isinstance(node, PostFilterNode)]
    if not post_filters:
      return None
    if len(post_filters) == 1:
      return post_filters[0]
    if post_filters == self.__nodes:
      return self
    return ConjunctionNode(post_filters)

  def apply(self, entity):
    for node in self.__nodes:
      if not node.apply(entity):
        return False
    return True

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


# TODO: Change ConjunctionNode and DisjunctionNode signatures so that
# AND and OR can just be aliases for them -- or possibly even rename.

def AND(*args):
  assert args
  assert all(isinstance(arg, Node) for arg in args)
  if len(args) == 1:
    return args[0]
  return ConjunctionNode(args)


def OR(*args):
  assert args
  assert all(isinstance(Node, arg) for arg in args)
  if len(args) == 1:
    return args[0]
  return DisjunctionNode(args)


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


# TODO: Not everybody likes GQL.

# TODO: GQL doesn't support querying for structured property values.

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
    filters = ConjunctionNode(filters)
  else:
    filters = None
  orderings = gql_qry.orderings()
  orders = []
  for (name, direction) in orderings:
    orders.append(datastore_query.PropertyOrder(name, direction))
  if not orders:
    orders = None
  elif len(orders) == 1:
    orders = orders[0]
  else:
    orders = datastore_query.CompositeOrder(orders)
  qry = Query(kind=gql_qry._entity,
              ancestor=ancestor,
              filters=filters,
              orders=orders)
  offset = gql_qry.offset()
  if offset < 0:
    offset = None
  limit = gql_qry.limit()
  if limit < 0:
    limit = None
  options = QueryOptions(offset=offset, limit=limit)
  return qry, options, bindings


class Query(object):

  @datastore_rpc._positional(1)
  def __init__(self, kind=None, ancestor=None, filters=None, orders=None):
    if ancestor is not None and not isinstance(ancestor, Binding):
      lastid = ancestor.pairs()[-1][1]
      assert lastid, 'ancestor cannot be an incomplete key'
    if filters is not None:
      assert isinstance(filters, Node)
    if orders is not None:
      assert isinstance(orders, datastore_query.Order)
    self.__kind = kind  # String
    self.__ancestor = ancestor  # Key
    self.__filters = filters  # None or Node subclass
    self.__orders = orders  # None or datastore_query.Order instance

  # TODO: __repr__().

  def _get_query(self, connection):
    kind = self.__kind
    ancestor = self.__ancestor
    bindings = {}
    if isinstance(ancestor, Binding):
      bindings[ancestor.key] = ancestor
      ancestor = ancestor.resolve()
    if ancestor is not None:
      ancestor = connection.adapter.key_to_pb(ancestor)
    filters = self.__filters
    post_filters = None
    if filters is not None:
      post_filters = filters._post_filters()
      filters = filters._to_filter(bindings)
    dsqry = datastore_query.Query(kind=kind,
                                  ancestor=ancestor,
                                  filter_predicate=filters,
                                  order=self.__orders)
    return dsqry, post_filters

  @tasklets.tasklet
  def run_to_queue(self, queue, conn, options=None):
    """Run this query, putting entities into the given queue."""
    multiquery = self._maybe_multi_query()
    if multiquery is not None:
      multiquery.run_to_queue(queue, conn, options=options)  # No return value.
      return
    dsqry, post_filters = self._get_query(conn)
    orig_options = options
    if (post_filters and options is not None and
        (options.offset or options.limit is not None)):
      options = datastore_query.QueryOptions(offset=None, limit=None,
                                             config=orig_options)
      assert options.limit is None and options.limit is None
    rpc = dsqry.run_async(conn, options)
    skipped = 0
    count = 0
    while rpc is not None:
      batch = yield rpc
      rpc = batch.next_batch_async(options)
      for ent in batch.results:
        if post_filters:
          if not post_filters.apply(ent):
            continue
          if orig_options is not options:
            if orig_options.offset and skipped < orig_options.offset:
              skipped += 1
              continue
            if orig_options.limit is not None and count >= orig_options.limit:
              rpc = None  # Quietly throw away the next batch.
              break
            count += 1
        queue.putq(ent)
    queue.complete()

  def _maybe_multi_query(self):
    filters = self.__filters
    if filters is not None:
      filters = filters.resolve()
      if isinstance(filters, DisjunctionNode):
        # Switch to a MultiQuery.
        subqueries = []
        for subfilter in filters:
          subquery = Query(kind=self.__kind, ancestor=self.__ancestor,
                           filters=subfilter, orders=self.__orders)
          subqueries.append(subquery)
        return MultiQuery(subqueries, orders=self.__orders)
    return None

  @property
  def kind(self):
    return self.__kind

  @property
  def ancestor(self):
    return self.__ancestor

  @property
  def filters(self):
    return self.__filters

  @property
  def orders(self):
    return self.__orders

  def filter(self, *args):
    if not args:
      return self
    preds = []
    f = self.filters
    if f:
      preds.append(f)
    for arg in args:
      assert isinstance(arg, Node)
      preds.append(arg)
    if not preds:
      pred = None
    elif len(preds) == 1:
      pred = preds[0]
    else:
      pred = ConjunctionNode(preds)
    return self.__class__(kind=self.kind, ancestor=self.ancestor,
                          orders=self.orders, filters=pred)

  # TODO: Change this to .order(<property>, -<property>, ...).

  def order(self, *args):
    # q.order(Eployee.name, -Employee.age)
    if not args:
      return self
    orders = []
    o = self.__orders
    if o:
      orders.append(o)
    for arg in args:
      if isinstance(arg, model.Property):
        orders.append(datastore_query.PropertyOrder(arg._name, ASC))
      elif isinstance(arg, datastore_query.Order):
        orders.append(arg)
      else:
        assert False, arg
    if not orders:
      orders = None
    elif len(orders) == 1:
      orders = orders[0]
    else:
      orders = datastore_query.CompositeOrder(orders)
    return self.__class__(kind=self.kind, ancestor=self.ancestor,
                          filters=self.filters, orders=orders)

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

  def fetch(self, limit, offset=0, options=None):
    return self.fetch_async(limit, offset, options=options).get_result()

  @tasklets.tasklet
  def fetch_async(self, limit, offset=0, options=None):
    options = QueryOptions(limit=limit,
                           prefetch_size=limit,
                           batch_size=limit,
                           offset=offset,
                           config=options)
    res = []
    it = self.iter(options)
    while (yield it.has_next_async()):
      res.append(it.next())
    raise tasklets.Return(res)

  def get(self, options=None):
    return self.get_async(options=options).get_result()

  @tasklets.tasklet
  def get_async(self, options=None):
    res = yield self.fetch_async(1, options=options)
    if not res:
      raise tasklets.Return(None)
    raise tasklets.Return(res[0])

  def count(self, limit, options=None):
    return self.count_async(limit, options=options).get_result()

  @tasklets.tasklet
  def count_async(self, limit, options=None):
    conn = tasklets.get_context()._conn
    options = QueryOptions(offset=limit, limit=0, config=options)
    dsqry, post_filters = self._get_query(conn)
    if post_filters:
      raise datastore_errors.BadQueryError(
        'Post-filters are not supported for count().')
    rpc = dsqry.run_async(conn, options)
    total = 0
    while rpc is not None:
      batch = yield rpc
      rpc = batch.next_batch_async(options)
      total += batch.skipped_results
    raise tasklets.Return(total)


class QueryIterator(object):
  """This iterator works both for synchronous and async callers!

  For synchronous callers, just use:

    for entity in Account.query():
      <use entity>

  Async callers use this idiom:

    it = iter(Account.query())
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

  def __init__(self, entity, iterator, orderings):
    self.entity = entity
    self.iterator = iterator
    self.orderings = orderings

  def __cmp__(self, other):
    assert isinstance(other, _SubQueryIteratorState)
    assert self.orderings == other.orderings
    our_entity = self.entity
    their_entity = other.entity
    # TODO: Renamed properties again.
    if self.orderings:
      for propname, direction in self.orderings:
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

  def __init__(self, subqueries, orders=None):
    assert isinstance(subqueries, list), subqueries
    assert all(isinstance(subq, Query) for subq in subqueries), subqueries
    if orders is not None:
      assert isinstance(orders, datastore_query.Order)
    self.__subqueries = subqueries
    self.__orders = orders
    self.ancestor = None  # Hack for map_query().

  @tasklets.tasklet
  def run_to_queue(self, queue, conn, options=None):
    """Run this query, putting entities into the given queue."""
    # Create a list of (first-entity, subquery-iterator) tuples.
    # TODO: Use the specified sort order.
    assert options is None  # Don't know what to do with these yet.
    state = []
    orderings = orders_to_orderings(self.__orders)
    for subq in self.__subqueries:
      subit = tasklets.SerialQueueFuture('MultiQuery.run_to_queue')
      subq.run_to_queue(subit, conn)
      try:
        ent = yield subit.getq()
      except EOFError:
        continue
      else:
        state.append(_SubQueryIteratorState(ent, subit, orderings))

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


def order_to_ordering(order):
  pb = order._to_pb()
  return (pb.property(), pb.direction())  # TODO: What about UTF-8?


def orders_to_orderings(orders):
  if orders is None:
    return []
  if isinstance(orders, datastore_query.PropertyOrder):
    return [order_to_ordering(orders)]
  if isinstance(orders, datastore_query.CompositeOrder):
    # TODO: What about UTF-8?
    return [(pb.property(), pb.direction())for pb in orders._to_pbs()]
  assert False, orders


def ordering_to_order(ordering):
  name, direction = ordering
  return datastore_query.PropertyOrder(name, direction)


def orderings_to_orders(orderings):
  orders = [ordering_to_order(o) for o in orderings]
  if not orders:
    return None
  if len(orders) == 1:
    return orders[0]
  return datastore_query.CompositeOrder(orders)
