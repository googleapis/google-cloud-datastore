"""Higher-level Query wrapper."""

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


class Query(object):

  # TODO: Add an all() or select() class method to Model that returns
  # a Query instance.

  @datastore_rpc._positional(1)
  def __init__(self, kind=None, ancestor=None, filter=None, order=None):
    """A wrapper for Query."""
    # TODO: Put off all this until run_async() is called.
    if ancestor is not None:
      ancestor = model.conn.adapter.key_to_pb(ancestor)
    self.__query = datastore_query.Query(kind=kind, ancestor=ancestor,
                                         filter_predicate=filter,
                                         order=order)

  def run_async(self, connection, options=None):
    return self.__query.run_async(connection, options)

  # TODO: These properties only work because our class name ('Query')
  # is the same as that of self.__query.  This is really bad style.

  @property
  def kind(self):
    return self.__query.__kind

  @property
  def ancestor(self):
    ancestor = self.__query.__ancestor
    if ancestor is not None:
      ancestor = model.conn.adapter.pb_to_key(ancestor)
    return ancestor

  @property
  def filter(self):
    # TODO: Return something that is actually useful to the user
    # (e.g. from which it is easy to reconstruct the arguments to
    # where()).  Alternately make the *Filter class introspectable.
    return self.__query.__filter_predicate

  @property
  def order(self):
    # TODO: See filter().
    return self.__query.__order

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
          pred = datastore_query.make_filter(name, opsymbol, value)
          preds.append(pred)
    if len(preds) == 1:
      pred = preds[0]
    else:
      pred = datastore_query.CompositeFilter(_AND, preds)
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
