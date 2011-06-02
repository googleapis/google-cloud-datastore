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

A further shortcut is calling .filter() with multiple arguments; this
implies AND():

  q1 = Employee.query()  # A query that returns all employees
  q3 = q1.filter(Employee.age >= 30,
                 Employee.age < 40)  # Only those in their 30s

And finally you can also pass one or more filter expressions directly
to the .query() method:

  q3 = Employee.query(Employee.age >= 30,
                      Employee.age < 40)  # Only those in their 30s

Query objects are immutable, so these methods always return a new
Query object; the above calls to filter() do not affect q1.  (On the
other hand, operations that are effectively no-ops may return the
original Query object.)

Sort orders can also be combined this way, and .filter() and .order()
calls may be intermixed:

  q4 = q3.order(-Employee.age)
  q5 = q4.order(Employee.name)
  q6 = q5.filter(Employee.rank == 5)

Again, multiple .order() calls can be combined:

  q5 = q3.order(-Employee.age, Employee.name)

The simplest way to retrieve Query results is a for-loop:

  for emp in q3:
    print emp.name, emp.age

Some other methods to run a query and access its results:

  q.iter() # Return an iterator; same as iter(q) but more flexible
  q.map(callback) # Call the callback function for each query result
  q.fetch(N) # Return a list of the first N results
  q.get() # Return the first result
  q.count(N) # Return the number of results, with a maximum of N
  q.fetch_page(N, start_cursor=cursor) # Return (results, cursor, has_more)

All of the above methods take a standard set of additional query
options, either in the form of keyword arguments such as
keys_only=True, or as QueryOptions object passed with
options=QueryOptions(...).  The most important query options are:

  keys_only: bool, if set the results are keys instead of entities
  limit: int, limits the number of results returned
  offset: int, skips this many results first
  start_cursor: Cursor, start returning results after this position
  end_cursor: Cursor, stop returning results after this position
  batch_size: int, hint for the number of results returned per RPC
  prefetch_size: int, hint for the number of results in the first RPC
  produce_cursors: bool, return Cursor objects with the results

For additional (obscure) query options and more details on them,
including an explanation of Cursors, see datastore_query.py.

All of the above methods except for iter() have asynchronous variants
as well, which return a Future; to get the operation's ultimate
result, yield the Future (when inside a tasklet) or call the Future's
get_result() method (outside a tasklet):

  q.map_async(callback)  # Callback may be a task or a plain function
  q.fetch_async(N)
  q.get_async()
  q.count_async(N)
  q.fetch_page_async(N, start_cursor=cursor)

Finally, there's an idiom to efficiently loop over the Query results
in a tasklet, properly yielding when appropriate:

  it = q.iter()
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

__all__ = ['Binding', 'AND', 'OR', 'parse_gql', 'Query',
           'QueryOptions', 'Cursor']

# Re-export some useful classes from the lower-level module.
QueryOptions = datastore_query.QueryOptions
Cursor = datastore_query.Cursor

# Some local renamings.
_ASC = datastore_query.PropertyOrder.ASCENDING
_DESC = datastore_query.PropertyOrder.DESCENDING
_AND = datastore_query.CompositeFilter.AND

# Table of supported comparison operators.
_OPS = frozenset(['=', '!=', '<', '<=', '>', '>=', 'in'])


class Binding(object):
  """Used with GQL; for now unsupported."""

  def __init__(self, value=None, key=None):
    """Constructor.  The value may be changed later."""
    self.value = value
    self.key = key

  def __repr__(self):
    return '%s(%r, %r)' % (self.__class__.__name__, self.value, self.key)

  def __eq__(self, other):
    # TODO: When comparing tree nodes containing Bindings, Bindings
    # should be compared by object identity?
    if not isinstance(other, Binding):
      return NotImplemented
    return self.value == other.value and self.key == other.key

  def resolve(self):
    """Return the value currently associated with this Binding."""
    value = self.value
    assert not isinstance(value, Binding), 'Recursive Binding'
    return value


class Node(object):
  """Base class for filter expression tree nodes.

  Tree nodes are considered immutable, even though they can contain
  Binding instances, which are not.  In particular, two identical
  trees may be represented by the same Node object in different
  contexts.
  """

  def __new__(cls):
    assert cls is not Node, 'Cannot instantiate Node, only a subclass'
    return super(Node, cls).__new__(cls)

  def __eq__(self, other):
    raise NotImplementedError

  def __ne__(self, other):
    eq = self.__eq__(other)
    if eq is not NotImplemented:
      eq = not eq
    return eq

  def __unordered(self, other):
    raise TypeError('Nodes cannot be ordered')
  __le__ = __lt__ = __ge__ = __gt__ = __unordered

  def _to_filter(self, bindings):
    """Helper to convert to datastore_query.Filter, or None."""
    raise NotImplementedError

  def _post_filters(self):
    """Helper to extract post-filter Nodes, if any."""
    return None

  def apply(self, entity):
    """Test whether an entity matches the filter."""
    return True

  def resolve(self):
    """Extract the Binding's value if necessary."""
    raise NotImplementedError


class FalseNode(Node):
  """Tree node for an always-failing filter."""

  def __new__(cls):
    return super(Node, cls).__new__(cls)

  def __eq__(self, other):
    if not isinstance(other, FalseNode):
      return NotImplemented
    return True

  def _to_filter(self, bindings):
    # Because there's no point submitting a query that will never
    # return anything.
    raise datastore_errors.BadQueryError(
      'Cannot convert FalseNode to predicate')

  def resolve(self):
    return self


class FilterNode(Node):
  """Tree node for a single filter expression."""

  def __new__(cls, name, opsymbol, value):
    if opsymbol == '!=':
      n1 = FilterNode(name, '<', value)
      n2 = FilterNode(name, '>', value)
      return DisjunctionNode([n1, n2])
    if opsymbol == 'in' and not isinstance(value, Binding):
      assert isinstance(value, (list, tuple, set, frozenset)), repr(value)
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
    # TODO: Should nodes with values that compare equal but have
    # different types really be considered equal?  IIUC the datastore
    # doesn't consider 1 equal to 1.0 when it compares property values.
    return (self.__name == other.__name and
            self.__opsymbol == other.__opsymbol and
            self.__value == other.__value)

  def _to_filter(self, bindings):
    assert self.__opsymbol not in ('!=', 'in'), repr(self.__opsymbol)
    value = self.__value
    if isinstance(value, Binding):
      bindings[value.key] = value
      value = value.resolve()
    return datastore_query.make_filter(self.__name, self.__opsymbol, value)

  def resolve(self):
    if self.__opsymbol == 'in':
      assert isinstance(self.__value, Binding), 'Unexpanded non-Binding IN'
      return FilterNode(self.__name, self.__opsymbol, self.__value.resolve())
    else:
      return self


class PostFilterNode(Node):
  """Tree node representing an in-memory filtering operation.

  This is used to represent filters that cannot be executed by the
  datastore, for example a query for a structured value.
  """

  def __new__(cls, filter_func, filter_arg):
    self = super(PostFilterNode, cls).__new__(cls)
    self.filter_func = filter_func
    self.filter_arg = filter_arg
    return self

  def __repr__(self):
    return '%s(%s, %s)' % (self.__class__.__name__,
                           self.filter_func,
                           self.filter_arg)

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
  """Tree node representing a Boolean AND operator on two or more nodes."""

  def __new__(cls, nodes):
    assert nodes, 'ConjunctionNode requires at least one node'
    if len(nodes) == 1:
      return nodes[0]
    clauses = [[]]  # Outer: Disjunction; inner: Conjunction.
    # TODO: Remove duplicates?
    for node in nodes:
      assert isinstance(node, Node), repr(node)
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
  """Tree node representing a Boolean OR operator on two or more nodes."""

  def __new__(cls, nodes):
    assert nodes, 'DisjunctionNode requires at least one node'
    if len(nodes) == 1:
      return nodes[0]
    self = super(DisjunctionNode, cls).__new__(cls)
    self.__nodes = []
    # TODO: Remove duplicates?
    for node in nodes:
      assert isinstance(node, Node), repr(node)
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
  """Construct a ConjunctionNode from one or more tree nodes."""
  assert args, 'AND requires at least one argument'
  assert all(isinstance(arg, Node) for arg in args), repr(args)
  if len(args) == 1:
    return args[0]
  return ConjunctionNode(args)


def OR(*args):
  """Construct a DisjunctionNode from one or more tree nodes."""
  assert args, 'OR requires at least one argument'
  assert all(isinstance(arg, Node) for arg in args), repr(args)
  if len(args) == 1:
    return args[0]
  return DisjunctionNode(args)


def _args_to_val(func, args, bindings):
  """Helper for GQL parsing."""
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
    assert len(vals) == 1, '"nop" requires exactly one value'
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
      assert len(values) == 1, '"is" requires exactly one value'
      [(func, args)] = values
      ancestor = _args_to_val(func, args, bindings)
      continue
    assert op in _OPS, repr(op)
    for (func, args) in values:
      val = _args_to_val(func, args, bindings)
      filters.append(FilterNode(name, op, val))
  if filters:
    filters.sort(key=lambda x: x._sort_key())  # For predictable tests.
    filters = ConjunctionNode(filters)
  else:
    filters = None
  orders = _orderings_to_orders(gql_qry.orderings())
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
  """Query object.

  Usually constructed by calling Model.query().

  See module docstring for examples.

  Note that not all operations on Queries are supported by _MultiQuery
  instances; the latter are generated as necessary when any of the
  operators !=, IN or OR is used.
  """

  @datastore_rpc._positional(1)
  def __init__(self, kind=None, ancestor=None, filters=None, orders=None):
    """Constructor.

    Args:
      kind: Optional kind string.
      ancestor: Optional ancestor Key.
      filters: Optional Node representing a filter expression tree.
      orders: Optional datastore_query.Order object.
    """
    if ancestor is not None and not isinstance(ancestor, Binding):
      lastid = ancestor.pairs()[-1][1]
      assert lastid, 'ancestor cannot be an incomplete key'
    if filters is not None:
      assert isinstance(filters, Node), repr(filters)
    if orders is not None:
      assert isinstance(orders, datastore_query.Order), repr(orders)
    self.__kind = kind  # String
    self.__ancestor = ancestor  # Key
    self.__filters = filters  # None or Node subclass
    self.__orders = orders  # None or datastore_query.Order instance

  def __repr__(self):
    args = []
    if self.__kind is not None:
      args.append('kind=%r' % self.__kind)
    if self.__ancestor is not None:
      args.append('ancestor=%r' % self.__ancestor)
    if self.__filters is not None:
      args.append('filters=%r' % self.__filters)
    if self.__orders is not None:
      args.append('orders=...')  # PropertyOrder doesn't have a good repr().
    return '%s(%s)' % (self.__class__.__name__, ', '.join(args))

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
      options = QueryOptions(offset=None, limit=None, config=orig_options)
      assert (options.limit is None and
              options.offset is None), repr(options._values)
    rpc = dsqry.run_async(conn, options)
    skipped = 0
    count = 0
    while rpc is not None:
      batch = yield rpc
      rpc = batch.next_batch_async(options)
      for i, ent in enumerate(batch.results):
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
        queue.putq((batch, i, ent))
    queue.complete()

  def _maybe_multi_query(self):
    filters = self.__filters
    if filters is not None:
      filters = filters.resolve()
      if isinstance(filters, DisjunctionNode):
        # Switch to a _MultiQuery.
        subqueries = []
        for subfilter in filters:
          subquery = Query(kind=self.__kind, ancestor=self.__ancestor,
                           filters=subfilter, orders=self.__orders)
          subqueries.append(subquery)
        return _MultiQuery(subqueries, orders=self.__orders)
    return None

  @property
  def kind(self):
    """Accessor for the kind (a string or None)."""
    return self.__kind

  @property
  def ancestor(self):
    """Accessor for the ancestor (a Key or None)."""
    return self.__ancestor

  @property
  def filters(self):
    """Accessor for the filters (a Node or None)."""
    return self.__filters

  @property
  def orders(self):
    """Accessor for the filters (a datastore_query.Order or None)."""
    return self.__orders

  def filter(self, *args):
    """Return a new Query with additional filter(s) applied."""
    if not args:
      return self
    preds = []
    f = self.filters
    if f:
      preds.append(f)
    for arg in args:
      assert isinstance(arg, Node), repr(arg)
      preds.append(arg)
    if not preds:
      pred = None
    elif len(preds) == 1:
      pred = preds[0]
    else:
      pred = ConjunctionNode(preds)
    return self.__class__(kind=self.kind, ancestor=self.ancestor,
                          orders=self.orders, filters=pred)

  def order(self, *args):
    """Return a new Query with additional sort order(s) applied."""
    # q.order(Eployee.name, -Employee.age)
    if not args:
      return self
    orders = []
    o = self.__orders
    if o:
      orders.append(o)
    for arg in args:
      if isinstance(arg, model.Property):
        orders.append(datastore_query.PropertyOrder(arg._name, _ASC))
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

  def iter(self, **q_options):
    """Construct an iterator over the query.

    Args:
      **q_options: All query options keyword arguments are supported.

    Returns:
      A QueryIterator object.
    """
    return QueryIterator(self, **q_options)

  __iter__ = iter

  @datastore_rpc._positional(2)
  def map(self, callback, merge_future=None, **q_options):
    """Map a callback function or tasklet over the query results.

    Args:
      callback: A function or tasklet to be applied to each result; see below.
      merge_future: Optional Future subclass; see below.
      **q_options: All query options keyword arguments are supported.

    Callback signature: The callback is normally called with an entity
    as argument.  However if keys_only=True is given, it is called
    with a Key.  Also, when produce_cursors=True is given, it is
    called with three arguments: the current batch, the index within
    the batch, and the entity or Key at that index.  The callback can
    return whatever it wants.

    Optional merge future: The merge_future is an advanced argument
    that can be used to override how the callback results are combined
    into the overall map() return value.  By default a list of
    callback return values is produced.  By substituting one of a
    small number of specialized alternatives you can arrange
    otherwise.  See tasklets.MultiFuture for the default
    implementation and a description of the protocol the merge_future
    object must implement the default.  Alternatives from the same
    module include QueueFuture, SerialQueueFuture and ReducingFuture.

    Returns:
      When the query has run to completion and all callbacks have
      returned, map() returns a list of the results of all callbacks.
      (But see 'optional merge future' above.)
    """
    return self.map_async(callback, merge_future=merge_future,
                          **q_options).get_result()

  @datastore_rpc._positional(2)
  def map_async(self, callback, merge_future=None, **q_options):
    """Map a callback function or tasklet over the query results.

    This is the asynchronous version of Query.map().
    """
    return tasklets.get_context().map_query(self, callback,
                                            options=_make_options(q_options),
                                            merge_future=merge_future)

  # TODO: support the rest for _MultiQuery.

  @datastore_rpc._positional(2)
  def fetch(self, limit, **q_options):
    """Fetch a list of query results, up to a limit.

    Args:
      limit: How many results to retrieve at most.
      **q_options: All query options keyword arguments are supported.

    Returns:
      A list of results.
    """
    # NOTE: limit can't be passed as a keyword.
    return self.fetch_async(limit, **q_options).get_result()

  @tasklets.tasklet
  @datastore_rpc._positional(2)
  def fetch_async(self, limit, **q_options):
    """Fetch a list of query results, up to a limit.

    This is the asynchronous version of Query.fetch().
    """
    assert 'limit' not in q_options, q_options
    if not isinstance(self.__filters, DisjunctionNode):
      # TODO: Set these once _MultiQuery supports options.
      q_options['limit'] = limit
      q_options.setdefault('prefetch_size', limit)
      q_options.setdefault('batch_size', limit)
    # TODO: Maybe it's better to use map_async() here?
    res = []
    it = self.iter(**q_options)
    while (yield it.has_next_async()):
      res.append(it.next())
      if len(res) >= limit:
        break
    raise tasklets.Return(res)

  def get(self, **q_options):
    """Get the first query result, if any.

    This is similar to calling q.fetch(1) and returning the first item
    of the list of results, if any, otherwise None.

    Args:
      **q_options: All query options keyword arguments are supported.

    Returns:
      A single result, or None if there are no results.
    """
    return self.get_async(**q_options).get_result()

  @tasklets.tasklet
  def get_async(self, **q_options):
    """Get the first query result, if any.

    This is the asynchronous version of Query.get().
    """
    res = yield self.fetch_async(1, **q_options)
    if not res:
      raise tasklets.Return(None)
    raise tasklets.Return(res[0])

  @datastore_rpc._positional(2)
  def count(self, limit, **q_options):
    """Count the number of query results, up to a limit.

    This returns the same result as len(q.fetch(limit)) but more
    efficiently.

    Note that you must pass a maximum value to limit the amount of
    work done by the query.

    Args:
      limit: How many results to count at most.
      **q_options: All query options keyword arguments are supported.

    Returns:
    """
    return self.count_async(limit, **q_options).get_result()

  @tasklets.tasklet
  @datastore_rpc._positional(2)
  def count_async(self, limit, **q_options):
    """Count the number of query results, up to a limit.

    This is the asynchronous version of Query.count().
    """
    assert 'offset' not in q_options, q_options
    assert 'limit' not in q_options, q_options
    if (self.__filters is not None and
        (isinstance(self.__filters, DisjunctionNode) or
         self.__filters._post_filters() is not None)):
      results = yield self.fetch_async(limit, **q_options)
      raise tasklets.Return(len(results))
    q_options['offset'] = limit
    q_options['limit'] = 0
    options = _make_options(q_options)
    conn = tasklets.get_context()._conn
    dsqry, post_filters = self._get_query(conn)
    rpc = dsqry.run_async(conn, options)
    total = 0
    while rpc is not None:
      batch = yield rpc
      rpc = batch.next_batch_async(options)
      total += batch.skipped_results
    raise tasklets.Return(total)

  @datastore_rpc._positional(2)
  def fetch_page(self, page_size, **q_options):
    """Fetch a page of results.

    This is a specialized method for use by paging user interfaces.

    Args:
      page_size: The requested page size.  At most this many results
        will be returned.

    In addition, any keyword argument supported by the QueryOptions
    class is supported.  In particular, to fetch the next page, you
    pass the cursor returned by one call to the next call using
    start_cursor=<cursor>.  A common idiom is to pass the cursor to
    the client using <cursor>.to_websafe_string() and to reconstruct
    that cursor on a subsequent request using
    Cursor.from_websafe_string(<string>).

    Returns:
      A tuple (results, cursor, more) where results is a list of query
      results, cursor is a cursor pointing just after the last result
      returned, and more is a bool indicating whether there are
      (likely) more results after that.
    """
    # NOTE: page_size can't be passed as a keyword.
    return self.fetch_page_async(page_size, **q_options).get_result()

  @tasklets.tasklet
  @datastore_rpc._positional(2)
  def fetch_page_async(self, page_size, **q_options):
    """Fetch a page of results.

    This is the asynchronous version of Query.fetch_page().
    """
    q_options.setdefault('batch_size', page_size)
    q_options.setdefault('produce_cursors', True)
    it = self.iter(limit=page_size+1, **q_options)
    results = []
    while (yield it.has_next_async()):
      results.append(it.next())
      if len(results) >= page_size:
        break
    try:
      cursor = it.cursor_after()
    except datastore_errors.BadArgumentError:
      cursor = None
    raise tasklets.Return(results, cursor, it.probably_has_next())


def _make_options(q_options):
  """Helper to construct a QueryOptions object from keyword arguents.

  Args:
    q_options: a dict of keyword arguments.

  Note that either 'options' or 'config' can be used to pass another
  QueryOptions object, but not both.  If another QueryOptions object is
  given it provides default values.

  Returns:
    A QueryOptions object, or None if q_options is empty.
  """
  if not q_options:
    return None
  if 'options' in q_options:
    # Move 'options' to 'config' since that is what QueryOptions() uses.
    assert 'config' not in q_options, q_options
    q_options['config'] = q_options.pop('options')
  return QueryOptions(**q_options)


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

  You can also use q.iter([options]) instead of iter(q); this allows
  passing query options such as keys_only or produce_cursors.

  When keys_only is set, it.next() returns a key instead of an entity.

  When produce_cursors is set, the methods it.cursor_before() and
  it.cursor_after() return Cursor objects corresponding to the query
  position just before and after the item returned by it.next().
  Before it.next() is called for the first time, both raise an
  exception.  Once the loop is exhausted, both return the cursor after
  the last item returned.  Calling it.has_next() does not affect the
  cursors; you must call it.next() before the cursors move.  Note that
  sometimes requesting a cursor requires a datastore roundtrip (but
  not if you happen to request a cursor corresponding to a batch
  boundary).  If produce_cursors is not set, both methods always raise
  an exception.

  Note that queries requiring in-memory merging of multiple queries
  (i.e. queries using the IN, != or OR operators) do not support query
  options.
  """

  # When produce_cursors is set, _lookahead collects (batch, index)
  # pairs passed to _extended_callback(), and (_batch, _index)
  # contain the info pertaining to the current item.
  _lookahead = None
  _batch = None
  _index = None

  # Indicate the loop is exhausted.
  _exhausted = False

  @datastore_rpc._positional(2)
  def __init__(self, query, **q_options):
    """Constructor.  Takes a Query and query options.

    This is normally called by Query.iter() or Query.__iter__().
    """
    ctx = tasklets.get_context()
    callback = None
    options = _make_options(q_options)
    if options is not None and options.produce_cursors:
      callback = self._extended_callback
    self._iter = ctx.iter_query(query, callback=callback, options=options)
    self._fut = None

  def _extended_callback(self, batch, index, ent):
    assert not self._exhausted, 'QueryIterator is already exhausted'
    # TODO: Make _lookup a deque.
    if self._lookahead is None:
      self._lookahead = []
    self._lookahead.append((batch, index))
    return ent

  def _consume_item(self):
    if self._lookahead:
      self._batch, self._index = self._lookahead.pop(0)
    else:
      self._batch = self._index = None

  def cursor_before(self):
    """Return the cursor before the current item.

    You must pass a QueryOptions object with produce_cursors=True
    for this to work.

    If there is no cursor or no current item, raise BadArgumentError.
    Before next() has returned there is no cursor.  Once the loop is
    exhausted, this returns the cursor after the last item.
    """
    if self._batch is None:
      raise datastore_errors.BadArgumentError('There is no cursor currently')
    # TODO: if cursor_after() was called for the previous item
    # reuse that result instead of computing it from scratch.
    # (Some cursor() calls make a datastore roundtrip.)
    return self._batch.cursor(self._index + self._exhausted)

  def cursor_after(self):
    """Return the cursor after the current item.

    You must pass a QueryOptions object with produce_cursors=True
    for this to work.

    If there is no cursor or no current item, raise BadArgumentError.
    Before next() has returned there is no cursor.    Once the loop is
    exhausted, this returns the cursor after the last item.
    """
    if self._batch is None:
      raise datastore_errors.BadArgumentError('There is no cursor currently')
    return self._batch.cursor(self._index + 1)

  def __iter__(self):
    """Iterator protocol: get the iterator for this iterator, i.e. self."""
    return self

  def probably_has_next(self):
    """Return whether a next item is (probably) available.

    This is not quite the same as has_next(), because when
    produce_cursors is set, some shortcuts are possible.  However, in
    some cases (e.g. when the query has a post_filter) we can get a
    false positive (returns True but next() will raise StopIteration).
    There are no false negatives, if Batch.more_results doesn't lie.
    """
    if self._lookahead:
      return True
    if self._batch is not None:
      return self._batch.more_results
    return self.has_next()

  def has_next(self):
    """Return whether a next item is available.

    See the module docstring for the usage pattern.
    """
    return self.has_next_async().get_result()

  @tasklets.tasklet
  def has_next_async(self):
    """Return a Future whose result will say whether a next item is available.

    See the module docstring for the usage pattern.
    """
    if self._fut is None:
      self._fut = self._iter.getq()
    flag = True
    try:
      yield self._fut
    except EOFError:
      flag = False
    raise tasklets.Return(flag)

  def next(self):
    """Iterator protocol: get next item or raise StopIteration."""
    if self._fut is None:
      self._fut = self._iter.getq()
    try:
      try:
        ent = self._fut.get_result()
        self._consume_item()
        return ent
      except EOFError:
        self._exhausted = True
        raise StopIteration
    finally:
      self._fut = None


class _SubQueryIteratorState(object):
  """Helper class for _MultiQuery."""

  def __init__(self, batch_i_entity, iterator, orderings):
    batch, i, entity = batch_i_entity
    self.entity = entity
    self.iterator = iterator
    self.orderings = orderings

  def __cmp__(self, other):
    assert isinstance(other, _SubQueryIteratorState), repr(other)
    assert self.orderings == other.orderings, (self.orderings, other.orderings)
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
        if direction == _ASC:
          func = min
        else:
          func = max
        if isinstance(our_value, list):
          our_value = func(our_value)
        if isinstance(their_value, list):
          their_value = func(their_value)
        flag = cmp(our_value, their_value)
        if direction == _DESC:
          flag = -flag
        if flag:
          return flag
    # All considered properties are equal; compare by key (ascending).
    # TODO: Comparison between ints and strings is arbitrary.
    return cmp(our_entity._key.pairs(), their_entity._key.pairs())


class _MultiQuery(object):
  """Helper class to run queries involving !=, IN or OR operators."""

  # This is not created by the user directly, but implicitly when
  # iterating over a query with at least one filter using an IN, OR or
  # != operator.  Note that some options must be interpreted by
  # _MultiQuery instead of passed to the underlying Queries' methods,
  # e.g. offset (though not necessarily limit, and I'm not sure about
  # cursors).

  def __init__(self, subqueries, orders=None):
    assert isinstance(subqueries, list), subqueries
    assert all(isinstance(subq, Query) for subq in subqueries), subqueries
    if orders is not None:
      assert isinstance(orders, datastore_query.Order), repr(orders)
    self.__subqueries = subqueries
    self.__orders = orders
    self.ancestor = None  # Hack for map_query().

  @tasklets.tasklet
  def run_to_queue(self, queue, conn, options=None):
    """Run this query, putting entities into the given queue."""
    # Create a list of (first-entity, subquery-iterator) tuples.
    # TODO: Use the specified sort order.
    assert options is None, '_MultiQuery does not take options yet'
    state = []
    orderings = _orders_to_orderings(self.__orders)
    for subq in self.__subqueries:
      subit = tasklets.SerialQueueFuture('_MultiQuery.run_to_queue')
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
    # _MultiQuery is a bit of a toy.  But where it works, it beats
    # expecting the user to do this themselves.)
    keys_seen = set()
    while state:
      item = heapq.heappop(state)
      ent = item.entity
      if ent._key not in keys_seen:
        keys_seen.add(ent._key)
        queue.putq((None, None, ent))
      subit = item.iterator
      try:
        batch, i, ent = yield subit.getq()
      except EOFError:
        pass
      else:
        item.entity = ent
        heapq.heappush(state, item)
    queue.complete()

  # Datastore API using the default context.

  def iter(self, **q_options):
    return QueryIterator(self, **q_options)

  __iter__ = iter


# Helper functions to convert between orders and orderings.  An order
# is a datastore_query.Order instance.  An ordering is a
# (property_name, direction) tuple.

def _order_to_ordering(order):
  pb = order._to_pb()
  return (pb.property(), pb.direction())  # TODO: What about UTF-8?


def _orders_to_orderings(orders):
  if orders is None:
    return []
  if isinstance(orders, datastore_query.PropertyOrder):
    return [_order_to_ordering(orders)]
  if isinstance(orders, datastore_query.CompositeOrder):
    # TODO: What about UTF-8?
    return [(pb.property(), pb.direction())for pb in orders._to_pbs()]
  assert False, orders


def _ordering_to_order(ordering):
  name, direction = ordering
  return datastore_query.PropertyOrder(name, direction)


def _orderings_to_orders(orderings):
  orders = [_ordering_to_order(o) for o in orderings]
  if not orders:
    return None
  if len(orders) == 1:
    return orders[0]
  return datastore_query.CompositeOrder(orders)
