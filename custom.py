"""A generic custom property.

There are three parts to this file:
- the Flexidate class, which acts as a non-trivial example
- the CustomProperty class, which implements the generic functionality
- classes FlexidateProperty and Actor, and main(), showing sample usage

(Arguably the CustomProperty class should come before the Flexidate
class, and arguably the tests should be in a separate file and use the
unittest framework.)
"""

import datetime

from google.appengine.ext import testbed

from ndb.model import *


class Flexidate(object):
  """A 'fuzzy date'.

  Given a start date and a fuzz in days, this has attributes
  start, fuzz, and end.
  """

  def __new__(cls, date, fuzz=1):
    obj = super(Flexidate, cls).__new__(cls)
    assert isinstance(date, datetime.date), repr(date)
    assert not isinstance(date, datetime.datetime), repr(date)  # !!!
    assert isinstance(fuzz, (int, long)), repr(fuzz)
    assert fuzz >= 1, repr(fuzz)
    obj.__date = date
    obj.__fuzz = fuzz
    return obj

  def __repr__(self):
    if self.__fuzz == 1:
      return 'Flexidate<%d-%d-%d>' % (self.__date.year, self.__date.month,
                                      self.__date.day)
    else:
      return 'Flexidate<%d-%d-%d+%d>' % (self.__date.year, self.__date.month,
                                         self.__date.day, self.__fuzz)

  @property
  def start(self):
    return self.__date

  @property
  def fuzz(self):
    return self.__fuzz

  # Just to make things interesting, end is not an attribute but an
  # accessor function -- you must call x.end().
  def end(self):
    return self.__date + datetime.timedelta(days=self.__fuzz - 1)

  def __eq__(self, other):
    if not isinstance(other, Flexidate):
      return NotImplemented
    return self.__date == other.__date and self.__fuzz == other.__fuzz

  def __ne__(self, other):
    eq = self.__eq__(other)
    if isinstance(eq, bool):
      eq = not eq
    return eq


class CustomProperty(StructuredProperty):
  """Custom property.

  Values stored for this property are either instances of a
  user-facing class (e.g. Flexidate), or instances of a synthetic
  model class which is constructed on the fly from the 'attributes'
  argument to the constructor.  Conversion between instances of the
  user-facing class and instances of the model class happen lazily:
  when the program requests a value using _get_value(), the value is
  converted to a user-facing class instance if necessary, while when
  the property is to be serialized, it is converted to a model class
  instance if necessary.  If the 'repeated' flag is set, all items in
  the list value are converted in one direction or back at once.
  """

  MISSING = object()  # Singleton to indicate "no value at all"

  def __init__(self,
               construct, # lambda model_object: user_object
               attributes,  # [attrname, ...] or {attrname: propname, ...}
               name=None,
               repeated=False,
               indexed=True,
               # etc.
               ):
    """Constructor.

    Args:
      construct: A function that takes a 'model class' instance and
        returns an instance of the desired user class.

      attributes: A list, set or tuple of attribute/property names, or
        a dict mapping attribute names to property names, Property
        instances, or (Property instance, function) tuples.  Each
        attribute name corresponds to an attribute of the user class;
        it will be serialized using the corresponding property name.
        If a function is given, it is used to extract the attribute
        from the user object; otherwise the attribute name is used to
        get the attribute from the user object.

      name, repeated, indexed, etc.: As for all properties.
    """
    assert construct is not None
    assert attributes is not None
    self._construct = construct
    if isinstance(attributes, dict):
      pass
    elif isinstance(attributes, (list, tuple, set, frozenset)):
      attributes = dict(zip(attributes, attributes))
    else:
      assert False, type(attributes)
    self._attrmap = {}
    classdict = {}
    for key, value in attributes.iteritems():
      # The value can be a string, a property, or a (property, function) pair.
      if isinstance(value, basestring):
        prop = GenericProperty()
        self._attrmap[key] = (prop, None)
        classdict[key] = prop
      elif isinstance(value, Property):
        self._attrmap[key] = (value, None)
        classdict[key] = value
      elif isinstance(value, tuple):
        assert len(value) == 2, repr(value)
        prop, func = value
        assert isinstance(prop, Property), repr(prop)
        assert callable(func), repr(func)
        self._attrmap[key] = value
        classdict[key] = prop
      else:
        assert False, repr(value)
    modelclass = MetaModel('<synthetic modelclass>', (Model,), classdict)
    super(CustomProperty, self).__init__(modelclass,
                                         name,
                                         repeated=repeated,
                                         indexed=indexed,
                                         # etc.
                                         )

  def _to_serializable(self, value):
    assert not isinstance(value, self._modelclass), repr(value)
    if not isinstance(value, self._modelclass):
      newvalue = self._modelclass()
      for attrname, (prop, func) in self._attrmap.iteritems():
        if func is None:
          attrval = getattr(value, attrname, self.MISSING)
        else:
          attrval = func(value)
        if attrval is not self.MISSING:
          setattr(newvalue, attrname, attrval)
      value = newvalue
    return value

  def _from_serializable(self, value):
    assert isinstance(value, self._modelclass), repr(value)
    if isinstance(value, self._modelclass):
      value = self._construct(value)
    return value

  # TODO: Not sure what _validate() should do here, since we don't
  # have a type to check for, only a 'constructor' function.


class FlexidateProperty(CustomProperty):
  def __init__(self, name=None, repeated=False, indexed=True):
    return super(FlexidateProperty, self).__init__(
      construct=lambda ent: Flexidate(ent.start, ent.fuzz),
      attributes={'start': DateProperty(),
                  'fuzz': 'fuzz',
                  'end': (DateProperty(), lambda fd: fd.end())},
      name=name,
      repeated=repeated,
      indexed=indexed,
      )

  def __repr__(self):
    return ('FlexidateProperty(%r, %r, %r)' %
            (self._name, self._repeated, self._indexed))

  def _validate(self, value):
    if not isinstance(value, Flexidate):
      raise TypeError('expected Flexidate, got %r' % (value,))


class Actor(Model):
  name = StringProperty()
  born = FlexidateProperty()
  events = FlexidateProperty(repeated=True)


def main():
  tb = testbed.Testbed()
  tb.activate()
  tb.init_datastore_v3_stub()
  tb.init_memcache_stub()

  print Actor.name
  print Actor.born
  print Actor.born.start
  print Actor.born.fuzz
  print Actor.born.end
  a = Actor(name='John Doe')
  a.born = Flexidate(datetime.date(1956, 1, 1), 366)
  print 'a =', a
  pb = a._to_pb()
  b = Actor._from_pb(pb)
  b.key = None
  print 'b =', b
  assert a == b, (a, b)
  a.put()
  b.name = 'Joan Doe'
  b.born = Flexidate(datetime.date(1956, 1, 1), 31)
  b.events = [a.born, b.born]
  b.put()
  print b
  q = Actor.query(Actor.born.start == datetime.date(1956, 1, 1))
  print 'q =', q
  for i, res in enumerate(q):
    print '%2d: %s' % (i, res)
  q = Actor.query(Actor.born == Flexidate(datetime.date(1956, 1, 1), 366))
  print 'q =', q
  for i, res in enumerate(q):
    print '%2d: %s' % (i, res)
  q = Actor.query(Actor.born.fuzz >= 31)
  print 'q =', q
  for i, res in enumerate(q):
    print '%2d: %s' % (i, res)
  q = Actor.query(Actor.events.fuzz == 366)
  print 'q =', q
  for i, res in enumerate(q):
    print '%2d: %s' % (i, res)

  tb.deactivate()


if __name__ == '__main__':
  main()
