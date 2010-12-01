"""Higher-level Query wrapper."""

from google.appengine.api import datastore_types

from core import datastore_query
from core import datastore_rpc

from ndb import model


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
  def __init__(self, kind=None, ancestor=None, __filter=None, __order=None):
    """A wrapper for Query."""
    self.__query = datastore_query.Query(kind=kind, ancestor=ancestor,
                                         filter_predicate=__filter,
                                         order=__order)

  def run_async(self, connection, options):
    return self.__query.run_async(connection, options)

  @property
  def filters(self):
    return self.__query.__filter_predicate  # XXX

  # XXX Etc. (app, namespace, kind, ancestor, orders)

  def where(self, **kwds):
    # NOTE: Filters specified this way are not ordered; to force
    # ordered filters, use q.filter(...).filter(...).
    if not kwds:
      return self
    preds = []
    if self.filters:
      preds.append(self.filters)
    for key, value in kwds.iteritems():
      for opname, opsymbol in _OPS.iteritems():
        if key.endswith(opname):
          name = key[:-len(opname)]
          pred = datastore_query.make_filter(name, opsymbol, value)
          preds.append(pred)
    if len(preds) == 1:
      pred = preds[0]
    else:
      pred = datastore_query.CompositeFilter(preds)
    return self.__class__(kind=self.kind, ancestor=self.ancestor,
                          __filter=pred)

  def order_by(self, *args, **kwds):
    # q.order(prop1=ASC).order(prop2=DESC)
    # or q.order('prop1', ('prop2', DESC))
    orders = list(self.orders)
    for arg in args:
      if isinstance(arg, tuple):
        propname, direction = arg
        assert direction in (ASC, DESC), direction
      else:
        propname = arg
        direction = ASC
      orders.append(XXX(propname, direction))
