"""Higher-level Query wrapper."""

from google.appengine.api import datastore_types

from core import datastore_query
from core import datastore_rpc

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

  @datastore_rpc._positional(1)
  def __init__(self, kind=None, ancestor=None, filter=None, order=None):
    """A wrapper for Query."""
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
    return self.__query.__ancestor

  @property
  def filter(self):
    return self.__query.__filter_predicate

  @property
  def order(self):
    return self.__query.__order

  def where(self, **kwds):
    # NOTE: Filters specified this way are not ordered; to force
    # ordered filters, use q.filter(...).filter(...).
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
    if not preds:
      pred = None
    elif len(preds) == 1:
      pred = preds[0]
    else:
      pred = datastore_query.CompositeFilter(_AND, preds)
    return self.__class__(kind=self.kind, ancestor=self.ancestor,
                          order=self.order, filter=pred)

  # TODO: Add or_where() -- client-side query merging.

  def order_by(self, *args, **kwds):
    # q.order(prop1=ASC).order(prop2=DESC)
    # or q.order('prop1', ('prop2', DESC))
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
    if not orders:
      order = None
    elif len(orders) == 1:
      order = orders[0]
    else:
      order = datastore_query.CompositeOrder(orders)
    return self.__class__(kind=self.kind, ancestor=self.ancestor,
                          filter=self.filter, order=order)
