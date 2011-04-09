"""Model and Property classes and associated stuff.

A model class represents the structure of entities stored in the
datastore.  Applications define model classes to indicate the
structure of their entities, then instantiate those model classes
to create entities.

All model classes must inherit (directly or indirectly) from Model.
Through the magic of metaclasses, straightforward assignments in the
model class definition can be used to declare the model's structure:

  class Person(Model):
    name = StringProperty()
    age = IntegerProperty()

We can now create a Person entity and write it to the datastore:

  p = Person(name='Arthur Dent', age=42)
  k = p.put()

The return value from put() is a Key (see the documentation for
ndb/key.py), which can be used to retrieve the same entity later:

  p2 = k.get()
  assert p2 == p

To update an entity, simple change its attributes and write it back
(note that this doesn't change the key):

  p2.name = 'Arthur Philip Dent'
  p2.put()

We can also delete an entity (by using the key):

  k.delete()

The property definitions in the class body tell the system the names
and the types of the fields to be stored in the datastore, whether
they must be indexed, their default value, and more.

Many different Property types exist.  Most are indexed by default, the
exceptions indicated in the list below:

- StringProperty: a short text string, limited to 500 bytes

- TextProperty: an unlimited text string; unindexed

- BlobProperty: an unlimited byte string; unindexed

- IntegerProperty: a 64-bit signed integer

- FloatProperty: a double precision floating point number

- BooleanProperty: a bool value

- DateTimeProperty: a datetime object.  Note: App Engine always uses
  UTC as the timezone

- DateProperty: a date object

- TimeProperty: a time object

- GeoPtProperty: a geographical location, i.e. (latitude, longitude)

- KeyProperty: a datastore Key value

- UserProperty: a User object.  Note: this exists for backwards
  compatibility with existing datastore schemas only; we do not
  recommend storing User objects directly in the datastore, but
  instead recommend storing the user.user_id() value

- StructuredProperty: a field that is itself structured like an
  entity; see below for more details

- LocalStructuredProperty: like StructuredProperty but the on-disk
  representation is an opaque blob; unindexed

- ComputedProperty: a property whose value is computed from other
  properties by a user-defined function.  The property value is
  written to the datastore so that it can be used in queries, but the
  value from the datastore is not used when the entity is read back

- GenericProperty: a property whose type is not constrained; mostly
  used by the Expando class (see below) but also usable explicitly

Most Property classes have similar constructor signatures.  They
accept several optional keyword arguments:

- name=<string>: the name used to store the property value in the
  datastore.  Unlike the following options, this may also be given as
  a positional argument

- indexed=<bool>: indicates whether the property should be indexed
  (allowing queries on this property's value)

- repeated=<bool>: indicates that this property can have multiple
  values in the same entity.

- required=<bool>: indicates that this property must be given a value

- default=<value>: a default value if no explicit value is given

- choices=<list of values>: a list or tuple of allowable values

- validator=<function>: a general-purpose validation function.  It
  will be called with two arguments (prop, value) and should either
  return the validated value or raise an exception.  It is also
  allowed for the function to modify the value, but calling it again
  on the modified value should not modify the value further.  (For
  example: a validator that returns value.strip() or value.lower() is
  fine, but one that returns value + '$' is not.)

The repeated, required and default options are mutually exclusive: a
repeated property cannot be required nor can it specify a default
value (the default is always an empty list and an empty list is always
an allowed value), and a required property cannot have a default.

Some property types have additional arguments.  Some property types
do not support all options.

Repeated properties are always represented as Python lists; if there
is only one value, the list has only one element.  When a new list is
assigned to a repeated property, all elements of the list are
validated.  Since it is also possible to mutate lists in place,
repeated properties are re-validated before they are written to the
datastore.

No validation happens when an entity is read from the datastore;
however property values read that have the wrong type (e.g. a string
value for an IntegerProperty) are ignored.

For non-repeated properties, None is always a possible value, and no
validation is called when the value is set to None.  However for
required properties, writing the entity to the datastore requires
the value to be something other than None (and valid).

The StructuredProperty is different from most other properties; it
lets you define a sub-structure for your entities.  The substructure
itself is defined using a model class, and the attribute value is an
instance of that model class.  However it is not stored in the
datastore as a separate entity; instead, its attribute values are
included in the parent entity using a naming convention (the name of
the structured attribute followed by a dot followed by the name of the
subattribute).  For example:

  class Address(Model):
    street = StringProperty()
    city = StringProperty()

  class Person(Model):
    name = StringProperty()
    address = StructuredProperty(Address)

  p = Person(name='Harry Potter',
             address=Address(street='4 Privet Drive',
                             city='Little Whinging'))
  k.put()

This would write a single 'Person' entity with three attributes (as
you could verify using the Datastore Viewer in the Admin Console):

  name = 'Harry Potter'
  address.street = '4 Privet Drive'
  address.city = 'Little Whinging'

Structured property types can be nested and have the repeated flag
set, but in a hierarchy of nested structured property types, only one
level can be repeated.  It is fine to have multiple structured
properties referencing the same model class.

It is also fine to use the same model class both as a top-level entity
class and as for a structured property; however queries for the model
class will only return the top-level entities.

The LocalStructuredProperty works similar to StructuredProperty on the
Python side.  For example:

  class Address(Model):
    street = StringProperty()
    city = StringProperty()

  class Person(Model):
    name = StringProperty()
    address = LocalStructuredProperty(Address)

  p = Person(name='Harry Potter',
             address=Address(street='4 Privet Drive',
                             city='Little Whinging'))
  k.put()

However the data written to the datastore is different; it writes a
'Person' entity with a 'name' attribute as before and a single
'address' attribute whose value is a blob which encodes the Address
value (using the standard"protocol buffer" encoding).

Sometimes the set of properties is not known ahead of time.  In such
cases you can use the Expando class.  This is a Model subclass that
creates properties on the fly, both upon assignment and when loading
an entity from the datastore.  For example:

  class SuperPerson(Expando):
    name = StringProperty()
    superpower = StringProperty()

  razorgirl = SuperPerson(name='Molly Millions',
                          superpower='bionic eyes, razorblade hands',
                          rasta_name='Steppin\' Razor',
                          alt_name='Sally Shears')
  elastigirl = SuperPerson(name='Helen Parr',
                           superpower='stretchable body')
  elastigirl.max_stretch = 30  # Meters

You can inspect the properties of an expando instance using the
_properties attribute:

  >>> print razorgirl._properties.keys()
  ['rasta_name', 'name', 'superpower', 'alt_name']
  >>> print elastigirl._properties
  {'max_stretch': GenericProperty('max_stretch'),
   'name': StringProperty('name'),
   'superpower': StringProperty('superpower')}

Note: this property exists for plain Model instances too; it is just
not as interesting for those.

The Model class offers basic query support.  You can create a Query
object by calling the query() class method.  Iterating over a Query
object returns the entities matching the query one at a time.

Query objects are fully described in the docstring for query.py, but
there is one handy shortcut that is only available through
Model.query(): positional arguments are interpreted as filter
expressions which are combined through an AND operator.  For example:

  Person.query(Person.name == 'Harry Potter', Person.age >= 11)

is equivalent to:

  Person.query().filter(Person.name == 'Harry Potter', Person.age >= 11)

Keyword arguments passed to .query() are passed along to the Query()
constructor.
"""

__author__ = 'guido@google.com (Guido van Rossum)'

# TODO: docstrings on all Property classes, Expando, and all methods.
# TODO: change asserts to better exceptions.
# TODO: rename CapWords methods of Model to _underscore_names.
# TODO: add _underscore aliases to lowercase_names Model methods.
# TODO: reject unknown property names in assignment (for Model) (?)
# TODO: BlobKeyProperty.

import copy
import datetime
import logging
import zlib

from google.appengine.api import datastore_errors
from google.appengine.api import datastore_types
from google.appengine.api import users
from google.appengine.datastore import datastore_query
from google.appengine.datastore import datastore_rpc
from google.appengine.datastore import entity_pb

import ndb.key
# NOTE: Don't import ndb.query here; it would cause circular import
# problems.  It is imported dynamically as needed.

Key = ndb.key.Key  # For export.

# NOTE: Property and Error classes are added later.
__all__ = ['Key', 'ModelAdapter', 'MetaModel', 'Model', 'Expando',
           'transaction', 'transaction_async']


class KindError(datastore_errors.BadValueError):
  """Raised when an implementation for a kind can't be found."""


class ComputedPropertyError(datastore_errors.Error):
  """Raised when attempting to assign a value to a computed property."""


class ModelAdapter(datastore_rpc.AbstractAdapter):
  """Conversions between 'our' Key and Model classes and protobufs.

  This is needed to construct a Connection object, which in turn is
  needed to construct a Context object.

  See the base class docstring for more info about the signatures.
  """

  def __init__(self, default_model=None):
    """Constructor.

    Args:
      default_model: If an implementation for the kind cannot be found, use this
        model class. If none is specified, an exception will be thrown
        (default).
    """
    self.default_model = default_model

  def pb_to_key(self, pb):
    return Key(reference=pb)

  def key_to_pb(self, key):
    return key.reference()

  def pb_to_entity(self, pb):
    kind = None
    if pb.has_key():
      # TODO: Fix the inefficiency here: we extract the key just so we
      # can get the kind just so we can find the intended model class,
      # but the key is extracted again and stored in the entity by _from_pb().
      key = Key(reference=pb.key())
      kind = key.kind()
    modelclass = Model._kind_map.get(kind, self.default_model)
    if modelclass is None:
      raise KindError("No implementation found for kind '%s'" % kind)
    return modelclass._from_pb(pb)

  def entity_to_pb(self, ent):
    pb = ent._to_pb()
    return pb


def make_connection(config=None, default_model=None):
  """Create a new Connection object with the right adapter.

  Optionally you can pass in a datastore_rpc.Configuration object.
  """
  return datastore_rpc.Connection(
      adapter=ModelAdapter(default_model),
      config=config)


class MetaModel(type):
  """Metaclass for Model.

  This exists to fix up the properties -- they need to know their name.
  This is accomplished by calling the class's FixProperties() method.
  """

  def __init__(cls, name, bases, classdict):
    super(MetaModel, cls).__init__(name, bases, classdict)
    cls._fix_up_properties()


class Model(object):
  """A class describing datastore entities.

  Model instances are usually called entities.  All model classes
  inheriting from Model automatically have MetaModel as their
  metaclass, so that the properties are fixed up properly after the
  class once the class is defined.

  Because of this, you cannot use the same Property object to describe
  multiple properties -- you must create separate Property objects for
  each property.  E.g. this does not work:

    wrong_prop = StringProperty()
    class Wrong(Model):
      wrong1 = wrong_prop
      wrong2 = wrong_prop

  The kind is normally equal to the class name (exclusive of the
  module name or any other parent scope).  To override the kind,
  define a class method named _get_kind(), as follows:

    class MyModel(Model):
      @classmethod
      def _get_kind(cls):
        return 'AnotherKind'
  """

  __metaclass__ = MetaModel

  # TODO: Prevent accidental attribute assignments

  # Class variables updated by _fix_up_properties()
  _properties = None
  _has_repeated = False
  _kind_map = {}  # Dict mapping {kind: Model subclass}

  # Defaults for instance variables.
  _key = None
  _values = None

  # TODO: Make _ versions of all methods, and make non-_ versions
  # simple aliases. That way the _ version is still accessible even if
  # the non-_ version has been obscured by a property.

  # TODO: Distinguish between purposes: to call _from_pb() or setvalue() etc.
  @datastore_rpc._positional(1)
  def __init__(self, key=None, id=None, parent=None, **kwds):
    """Creates a new instance of this model (a.k.a. as an entity).

    The new entity must be written to the datastore using an explicit
    call to .put().

    Args:
      key: Key instance for this model. If key is used, id and parent must
        be None.
      id: Key id for this model. If id is used, key must be None.
      parent: Key instance for the parent model or None for a top-level one.
        If parent is used, key must be None.
      **kwds: Keyword arguments mapping to properties of this model.

    Note: you cannot define a property named key; the .key attribute
    always refers to the entity's key.  But you can define properties
    named id or parent.  Values for the latter cannot be passed
    through the constructor, but can be assigned to entity attributes
    after the entity has been created.
    """
    if key is not None:
      if id is not None:
        raise datastore_errors.BadArgumentError(
            'Model constructor accepts key or id, not both.')
      if parent is not None:
        raise datastore_errors.BadArgumentError(
            'Model constructor accepts key or parent, not both.')
      # Using _setkey() here to trigger the basic Key checks.
      # self.key = key doesn't work because of Expando's __setattr__().
      self._setkey(key)
    elif id is not None or parent is not None:
      # When parent is set but id is not, we have an incomplete key.
      # Key construction will fail with invalid ids or parents, so no check
      # is needed.
      # TODO: should this be restricted to string ids?
      self._key = Key(self._get_kind(), id, parent=parent)

    self._values = {}
    self._set_attributes(kwds)

  def _set_attributes(self, kwds):
    """Internal helper to set attributes from keyword arguments."""
    cls = self.__class__
    for name, value in kwds.iteritems():
      prop = getattr(cls, name)  # Raises AttributeError for unknown properties.
      assert isinstance(prop, Property)
      prop.SetValue(self, value)

  def _find_uninitialized(self):
    """Internal helper to find uninitialized properties.

    Returns:
      A set of property names.
    """
    return set(name
               for name, prop in self._properties.iteritems()
               if not prop.IsInitialized(self))

  def _check_initialized(self):
    """Internal helper to check for uninitialized properties.

    Raises:
      BadValueError if it finds any.
    """
    baddies = self._find_uninitialized()
    if baddies:
      raise datastore_errors.BadValueError(
        'Entity has uninitialized properties: %s' % ', '.join(baddies))

  def __repr__(self):
    """Return an unambiguous string representation of an entity."""
    args = []
    done = set()
    for prop in self._properties.itervalues():
      if prop.HasValue(self):
        args.append('%s=%r' % (prop._code_name, prop.RetrieveValue(self)))
        done.add(prop._name)
    args.sort()
    if self._key is not None:
      args.insert(0, 'key=%r' % self._key)
    s = '%s(%s)' % (self.__class__.__name__, ', '.join(args))
    return s

  @classmethod
  def _get_kind(cls):
    """Return the kind name for this class.

    This defaults to cls.__name__; users may overrid this to give a
    class a different on-disk name than its class name.
    """
    return cls.__name__

  @classmethod
  def _get_kind_map(cls):
    """Internal helper to return the kind map."""
    return cls._kind_map

  @classmethod
  def _reset_kind_map(cls):
    """Clear the kind map.  Useful for testing."""
    cls._kind_map.clear()

  def _has_complete_key(self):
    """Return whether this entity has a complete key."""
    return self._key is not None and self._key.id() is not None

  def _getkey(self):
    """Getter for key attribute."""
    return self._key

  def _setkey(self, key):
    """Setter for key attribute."""
    if key is not None:
      if not isinstance(key, Key):
        raise datastore_errors.BadValueError(
            'Expected Key instance, got %r' % key)
      if self.__class__ not in (Model, Expando):
        if key.kind() != self._get_kind():
          raise KindError('Expected Key kind to be %s; received %s' %
                          (self._get_kind(), key.kind()))
    self._key = key

  def _delkey(self):
    """Deleter for key attribute."""
    self._key = None

  key = property(_getkey, _setkey, _delkey,
                 """The Key of an entity, or None if not set yet.""")

  def __hash__(self):
    """Dummy hash function.

    Raises:
      Always TypeError to emphasize that entities are mutable.
    """
    raise TypeError('Model is not immutable')

  def __eq__(self, other):
    """Compare two entities of the same class for equality."""
    if other.__class__ is not self.__class__:
      return NotImplemented
    # It's okay to use private names -- we're the same class
    if self._key != other._key:
      # TODO: If one key is None and the other is an explicit
      # incomplete key of the simplest form, this should be OK.
      return False
    return self._equivalent(other)

  def _equivalent(self, other):
    """Compare two entities of the same class, excluding keys."""
    assert other.__class__ is self.__class__  # TODO: What about subclasses?
    # It's all about determining inequality early.
    if len(self._properties) != len(other._properties):
      return False  # Can only happen for Expandos.
    my_prop_names = set(self._properties.iterkeys())
    their_prop_names = set(other._properties.iterkeys())
    if my_prop_names != their_prop_names:
      return False  # Again, only possible for Expandos.
    for name in my_prop_names:
      my_value = self._properties[name].GetValue(self)
      their_value = other._properties[name].GetValue(other)
      if my_value != their_value:
        return False
    return True

  def __ne__(self, other):
    """Implement self != other as not(self == other)."""
    eq = self.__eq__(other)
    if eq is NotImplemented:
      return NotImplemented
    return not eq

  def _to_pb(self, pb=None):
    """Internal helper to turn an entity into an EntityProto protobuf."""
    self._check_initialized()
    if pb is None:
      pb = entity_pb.EntityProto()

    # TODO: Move the key stuff into ModelAdapter.entity_to_pb()?
    key = self._key
    if key is None:
      pairs = [(self._get_kind(), None)]
      ref = ndb.key._ReferenceFromPairs(pairs, reference=pb.mutable_key())
    else:
      ref = key._reference()  # Don't copy
      pb.mutable_key().CopyFrom(ref)
    group = pb.mutable_entity_group()
    elem = ref.path().element(0)
    if elem.id() or elem.name():
      group.add_element().CopyFrom(elem)

    for name, prop in sorted(self._properties.iteritems()):
      prop.Serialize(self, pb)

    return pb

  @classmethod
  def _from_pb(cls, pb, set_key=True):
    """Internal helper to create an entity from an EntityProto protobuf."""
    assert isinstance(pb, entity_pb.EntityProto)
    ent = cls()

    # TODO: Move the key stuff into ModelAdapter.pb_to_entity()?
    if set_key and pb.has_key():
      ent._key = Key(reference=pb.key())

    indexed_properties = pb.property_list()
    unindexed_properties = pb.raw_property_list()
    for plist in [indexed_properties, unindexed_properties]:
      for p in plist:
        prop = ent._get_property_for(p, plist is indexed_properties)
        prop.Deserialize(ent, p)

    return ent

  def _get_property_for(self, p, indexed=True, depth=0):
    """Internal helper to get the Property for a protobuf-level property."""
    name = p.name()
    parts = name.split('.')
    assert len(parts) > depth, (p.name(), parts, depth)
    next = parts[depth]
    prop = self._properties.get(next)
    if prop is None:
      prop = self._fake_property(p, next, indexed)
    return prop

  def _clone_properties(self):
    """Internal helper to clone self._properties if necessary."""
    cls = self.__class__
    if self._properties is cls._properties:
      self._properties = dict(cls._properties)

  def _fake_property(self, p, next, indexed=True):
    """Internal helper to create a fake Property."""
    self._clone_properties()
    if p.name() != next and not p.name().endswith('.' + next):
      prop = StructuredProperty(Expando, next)
      self._values[prop._name] = Expando()
    else:
      prop = GenericProperty(next,
                             repeated=p.multiple(),
                             indexed=indexed)
    self._properties[prop._name] = prop
    return prop

  @classmethod
  def _fix_up_properties(cls):
    """Fix up the properties by calling their FixUp() method.

    Note: This is called by MetaModel, but may also be called manually
    after dynamically updating a model class.
    """
    cls._properties = {}  # Map of {name: Property}
    if cls.__module__ == __name__:  # Skip the classes in *this* file.
      return
    for name in set(dir(cls)):
      prop = getattr(cls, name, None)
      if isinstance(prop, Property):
        assert not name.startswith('_')
        # TODO: Tell prop the class, for error message.
        prop.FixUp(name)
        if prop._repeated:
          cls._has_repeated = True
        cls._properties[prop._name] = prop
    cls._kind_map[cls._get_kind()] = cls

  # TODO: Rename following methods to start with an underscore, and
  # then define convenience aliases without an underscore.  (Also some
  # of the methods above need such aliases.)

  @classmethod
  def query(cls, *args, **kwds):
    from ndb.query import Query  # Import late to avoid circular imports.
    qry = Query(kind=cls._get_kind(), **kwds)
    if args:
      qry = qry.filter(*args)
    return qry

  # Datastore API using the default context.
  # These use local import since otherwise they'd be recursive imports.

  def put(self):
    return self.put_async().get_result()

  def put_async(self):
    from ndb import tasklets
    return tasklets.get_context().put(self)

  @classmethod
  def get_or_insert(cls, name, parent=None, **kwds):
    """Transactionally retrieves an existing entity or creates a new one.

    Args:
      name: Key name to retrieve or create.
      parent: Parent entity key, if any.
      **kwds: Keyword arguments to pass to the constructor of the model class
        if an instance for the specified key name does not already exist. If
        an instance with the supplied key_name and parent already exists,
        these arguments will be discarded.

    Returns:
      Existing instance of Model class with the specified key name and parent
      or a new one that has just been created.
    """
    return cls.get_or_insert_async(name=name, parent=parent,
                                   **kwds).get_result()

  @classmethod
  def get_or_insert_async(cls, name, parent=None, **kwds):
    """Transactionally retrieves an existing entity or creates a new one.

    This is the asynchronous version of Model.get_or_insert().
    """
    from ndb import tasklets
    ctx = tasklets.get_context()
    return ctx.get_or_insert(cls, name=name, parent=parent, **kwds)

  @classmethod
  def allocate_ids(cls, size=None, max=None, parent=None):
    """Allocates a range of key IDs for this model class.

    Args:
      size: Number of IDs to allocate. Either size or max can be specified,
        not both.
      max: Maximum ID to allocate. Either size or max can be specified,
        not both.
      parent: Parent key for which the IDs will be allocated.

    Returns:
      A tuple with (start, end) for the allocated range, inclusive.
    """
    return cls.allocate_ids_async(size=size, max=max,
                                  parent=parent).get_result()

  @classmethod
  def allocate_ids_async(cls, size=None, max=None, parent=None):
    """Allocates a range of key IDs for this model class.

    This is the asynchronous version of Model.allocate_ids().
    """
    from ndb import tasklets
    key = Key(cls._get_kind(), None, parent=parent)
    return tasklets.get_context().allocate_ids(key, size=size, max=max)

  @classmethod
  def get_by_id(cls, id, parent=None):
    """Returns a instance of Model class by ID.

    Args:
      id: A string or integer key ID.
      parent: Parent key of the model to get.

    Returns:
      A model instance or None if not found.
    """
    return cls.get_by_id_async(id, parent=parent).get_result()

  @classmethod
  def get_by_id_async(cls, id, parent=None):
    """Returns a instance of Model class by ID.

    This is the asynchronous version of Model.get_by_id().
    """
    from ndb import tasklets
    key = Key(cls._get_kind(), id, parent=parent)
    return tasklets.get_context().get(key)


class Property(object):
  # TODO: Separate 'simple' properties from base Property class

  _code_name = None
  _name = None
  _indexed = True
  _repeated = False
  _required = False
  _default = None
  _choices = None
  _validator = None

  _attributes = ['_name', '_indexed', '_repeated', '_required', '_default',
                 '_choices', '_validator']
  _positional = 1

  @datastore_rpc._positional(1 + _positional)
  def __init__(self, name=None, indexed=None, repeated=None,
               required=None, default=None, choices=None, validator=None):
    if name is not None:
      assert '.' not in name  # The '.' is used elsewhere.
      self._name = name
    if indexed is not None:
      self._indexed = indexed
    if repeated is not None:
      self._repeated = repeated
    if required is not None:
      self._required = required
    if default is not None:
      self._default = default
    assert (bool(self._repeated) +
            bool(self._required) +
            (self._default is not None)) <= 1  # Allow at most one of these
    if choices is not None:
      assert isinstance(choices, (tuple, list))
      self._choices = tuple(choices)
    if validator is not None:
      # The validator is called as follows:
      #   value = validator(prop, value)
      # It should return the value to be used, or raise an exception.
      # It should be idempotent, i.e. calling it a second time should
      # not further modify the value.  So a validator that returns e.g.
      # value.lower() or value.strip() is fine, but one that returns
      # value + '$' is not.
      assert callable(validator)
      self._validator = validator

  def __repr__(self):
    args = []
    cls = self.__class__
    for i, attr in enumerate(self._attributes):
      val = getattr(self, attr)
      if val is not getattr(cls, attr):
        if isinstance(val, type):
          s = val.__name__
        else:
          s = repr(val)
        if i >= cls._positional:
          if attr.startswith('_'):
            attr = attr[1:]
          s = '%s=%s' % (attr, s)
        args.append(s)
    s = '%s(%s)' % (self.__class__.__name__, ', '.join(args))
    return s

  def _comparison(self, op, value):
    from ndb.query import FilterNode  # Import late to avoid circular imports.
    if value is not None:
      # TODO: Allow query.Binding instances?
      value = self.Validate(value)
    return FilterNode(self._name, op, value)

  def __eq__(self, value):
    return self._comparison('=', value)

  def __ne__(self, value):
    return self._comparison('!=', value)

  def __lt__(self, value):
    return self._comparison('<', value)

  def __le__(self, value):
    return self._comparison('<=', value)

  def __gt__(self, value):
    return self._comparison('>', value)

  def __ge__(self, value):
    return self._comparison('>=', value)

  # TODO: Rename IN to _IN and add IN back as an alias.

  def IN(self, value):
    from ndb.query import FilterNode  # Import late to avoid circular imports.
    if not isinstance(value, (list, tuple)):
      raise datastore_errors.BadValueError('Expected list or tuple, got %r' %
                                           (value,))
    values = []
    for val in value:
      if val is not None:
        val is self.Validate(val)
        values.append(val)
    return FilterNode(self._name, 'in', values)

  def __neg__(self):
    return datastore_query.PropertyOrder(
      self._name, datastore_query.PropertyOrder.DESCENDING)

  def __pos__(self):
    # So you can write q.order(-cls.age, +cls.name).
    return datastore_query.PropertyOrder(self._name)

  # TODO: Rename these methods to start with _.

  def Validate(self, value):
    # Return the value, possibly modified.
    return value

  def DoValidate(self, value):
    value = self.Validate(value)
    if self._choices is not None:
      if value not in self._choices:
        raise datastore_errors.BadValueError(
          'Value %r for property %s is not an allowed choice' %
          (value, self._name))
    if self._validator is not None:
      value = self._validator(self, value)
    return value

  def FixUp(self, code_name):
    self._code_name = code_name
    if self._name is None:
      self._name = code_name

  def StoreValue(self, entity, value):
    entity._values[self._name] = value

  def SetValue(self, entity, value):
    if self._repeated:
      if not isinstance(value, (list, tuple)):
        raise datastore_errors.BadValueError('Expected list or tuple, got %r' %
                                             (value,))
      values = []
      for val in value:
        val = self.DoValidate(val)
        values.append(val)
    else:
      if value is not None:
        value = self.DoValidate(value)
    self.StoreValue(entity, value)

  def HasValue(self, entity):
    return self._name in entity._values

  def RetrieveValue(self, entity):
    return entity._values.get(self._name, self._default)

  def GetValue(self, entity):
     value = self.RetrieveValue(entity)
     if value is None and self._repeated:
       value = []
       self.StoreValue(entity, value)
     return value

  def DeleteValue(self, entity):
    if self._name in entity._values:
      del entity._values[self._name]

  def IsInitialized(self, entity):
    return not self._required or (self.HasValue(entity) and
                                  self.GetValue(entity) is not None)

  def __get__(self, obj, cls=None):
    if obj is None:
      return self  # __get__ called on class
    return self.GetValue(obj)

  def __set__(self, obj, value):
    self.SetValue(obj, value)

  def __delete__(self, obj):
    self.DeleteValue(obj)

  def Serialize(self, entity, pb, prefix='', parent_repeated=False):
    # entity -> pb; pb is an EntityProto message
    value = self.RetrieveValue(entity)
    if value is None and self._repeated:
      value = []
    elif not isinstance(value, list):
      value = [value]
    for val in value:
      if self._repeated:
        # Re-validate repeated values, since the user could have
        # appended values to the list, bypassing validation.
        val = self.DoValidate(val)
      if self._indexed:
        p = pb.add_property()
      else:
        p = pb.add_raw_property()
      p.set_name(prefix + self._name)
      p.set_multiple(self._repeated or parent_repeated)
      v = p.mutable_value()
      if val is not None:
        self.DbSetValue(v, p, val)

  def Deserialize(self, entity, p, depth=1):
    # entity <- p; p is a Property message
    # In this class, depth is unused.
    v = p.value()
    val = self.DbGetValue(v, p)
    if self._repeated:
      if self.HasValue(entity):
        value = self.RetrieveValue(entity)
        if not isinstance(value, list):
          value = [value]
        value.append(val)
      else:
        value = [val]
    else:
      if not self.HasValue(entity):
        value = val
      else:
        oldval = self.RetrieveValue(entity)
        # Maybe upgrade to a list property.  Or ignore null.
        if val is None:
          value = oldval
        elif oldval is None:
          value = val
        elif isinstance(oldval, list):
          oldval.append(val)
          value = oldval
        else:
          value = [oldval, val]
    try:
      self.StoreValue(entity, value)
    except ComputedPropertyError, e:
      pass


class BooleanProperty(Property):

  def Validate(self, value):
    if not isinstance(value, bool):
      raise datastore_errors.BadValueError('Expected bool, got %r' %
                                           (value,))
    return value

  def DbSetValue(self, v, p, value):
    assert isinstance(value, bool), (self._name)
    v.set_booleanvalue(value)

  def DbGetValue(self, v, p):
    if not v.has_booleanvalue():
      return None
    # The booleanvalue field is an int32, so booleanvalue() returns an
    # int, hence the conversion.
    return bool(v.booleanvalue())


class IntegerProperty(Property):

  def Validate(self, value):
    if not isinstance(value, (int, long)):
      raise datastore_errors.BadValueError('Expected integer, got %r' %
                                           (value,))
    return int(value)

  def DbSetValue(self, v, p, value):
    assert isinstance(value, (bool, int, long)), (self._name)
    v.set_int64value(value)

  def DbGetValue(self, v, p):
    if not v.has_int64value():
      return None
    return int(v.int64value())


class FloatProperty(Property):

  def Validate(self, value):
    if not isinstance(value, (int, long, float)):
      raise datastore_errors.BadValueError('Expected float, got %r' %
                                           (value,))
    return float(value)

  def DbSetValue(self, v, p, value):
    assert isinstance(value, (bool, int, long, float)), (self._name)
    v.set_doublevalue(float(value))

  def DbGetValue(self, v, p):
    if not v.has_doublevalue():
      return None
    return v.doublevalue()


class StringProperty(Property):

  # TODO: Enforce size limit when indexed.

  def Validate(self, value):
    if not isinstance(value, basestring):
      raise datastore_errors.BadValueError('Expected string, got %r' %
                                           (value,))
    # TODO: Always convert to Unicode?  But what if it's unconvertible?
    return value

  def DbSetValue(self, v, p, value):
    assert isinstance(value, basestring)
    if isinstance(value, unicode):
      value = value.encode('utf-8')
    v.set_stringvalue(value)
    if not self._indexed:
      p.set_meaning(entity_pb.Property.TEXT)

  def DbGetValue(self, v, p):
    if not v.has_stringvalue():
      return None
    raw = v.stringvalue()
    try:
      raw.decode('ascii')
      return raw  # Don't bother with Unicode in this case
    except UnicodeDecodeError:
      try:
        value = raw.decode('utf-8')
        return value
      except UnicodeDecodeError:
        return raw


class TextProperty(StringProperty):

  # TODO: Maybe just use StringProperty(indexed=False)?

  _indexed = False

  def __init__(self, *args, **kwds):
    assert not kwds.get('indexed', False)
    super(TextProperty, self).__init__(*args, **kwds)


class BlobProperty(Property):

  # TODO: Enforce size limit when indexed.

  _indexed = False

  def Validate(self, value):
    if not isinstance(value, str):
      raise datastore_errors.BadValueError('Expected 8-bit string, got %r' %
                                           (value,))
    return value

  def DbSetValue(self, v, p, value):
    assert isinstance(value, str)
    v.set_stringvalue(value)
    if self._indexed:
      p.set_meaning(entity_pb.Property.BYTESTRING)
    else:
      p.set_meaning(entity_pb.Property.BLOB)

  def DbGetValue(self, v, p):
    if not v.has_stringvalue():
      return None
    return v.stringvalue()


class GeoPt(tuple):

  """A geographical point.  This is a tuple subclass and immutable.

  Fields:
    lat: latitude, a float in degrees with abs() <= 90.
    lon: longitude, a float in degrees with abs() <= 180.
  """

  # TODO: Use collections.namedtuple once we can drop Python 2.5 support.

  __slots__ = []

  def __new__(cls, lat=0.0, lon=0.0):
    # TODO: assert abs(lat) <= 90 and abs(lon) <= 180 ???
    return tuple.__new__(cls, (float(lat), float(lon)))

  @property
  def lat(self):
    return self[0]

  @property
  def lon(self):
    return self[1]

  def __repr__(self):
    return '%s(%.16g, %.16g)' % (self.__class__.__name__, self.lat, self.lon)


class GeoPtProperty(Property):

  def Validate(self, value):
    if not isinstance(value, GeoPt):
      raise datastore_errors.BadValueError('Expected GeoPt, got %r' %
                                           (value,))
    return value

  def DbSetValue(self, v, p, value):
    assert isinstance(value, GeoPt), (self._name)
    pv = v.mutable_pointvalue()
    pv.set_x(value.lat)
    pv.set_y(value.lon)

  def DbGetValue(self, v, p):
    if not v.has_pointvalue():
      return None
    pv = v.pointvalue()
    return GeoPt(pv.x(), pv.y())


def _unpack_user(v):
  uv = v.uservalue()
  email = unicode(uv.email().decode('utf-8'))
  auth_domain = unicode(uv.auth_domain().decode('utf-8'))
  obfuscated_gaiaid = uv.obfuscated_gaiaid().decode('utf-8')
  obfuscated_gaiaid = unicode(obfuscated_gaiaid)

  federated_identity = None
  if uv.has_federated_identity():
    federated_identity = unicode(
        uv.federated_identity().decode('utf-8'))

  value = users.User(email=email,
                     _auth_domain=auth_domain,
                     _user_id=obfuscated_gaiaid,
                     federated_identity=federated_identity)
  return value


class UserProperty(Property):

  def Validate(self, value):
    if not isinstance(value, users.User):
      raise datastore_errors.BadValueError('Expected User, got %r' %
                                           (value,))
    return value

  def DbSetValue(self, v, p, value):
    datastore_types.PackUser(p.name(), value, v)

  def DbGetValue(self, v, p):
    return _unpack_user(v)


class KeyProperty(Property):

  # TODO: namespaces
  # TODO: optionally check the kind (validation)

  def Validate(self, value):
    if not isinstance(value, Key):
      raise datastore_errors.BadValueError('Expected Key, got %r' % (value,))
    # Reject incomplete keys.
    if not value.id():
      raise datastore_errors.BadValueError('Expected complete Key, got %r' %
                                           (value,))
    return value

  def DbSetValue(self, v, p, value):
    assert isinstance(value, Key)
    # See datastore_types.PackKey
    ref = value._reference()  # Don't copy
    rv = v.mutable_referencevalue()  # A Reference
    rv.set_app(ref.app())
    if ref.has_name_space():
      rv.set_name_space(ref.name_space())
    for elem in ref.path().element_list():
      rv.add_pathelement().CopyFrom(elem)

  def DbGetValue(self, v, p):
    if not v.has_referencevalue():
      return None
    ref = entity_pb.Reference()
    rv = v.referencevalue()
    if rv.has_app():
      ref.set_app(rv.app())
    if rv.has_name_space():
      ref.set_name_space(rv.name_space())
    path = ref.mutable_path()
    for elem in rv.pathelement_list():
      path.add_element().CopyFrom(elem)
    return Key(reference=ref)


_EPOCH = datetime.datetime.utcfromtimestamp(0)

class DateTimeProperty(Property):

  # NOTE: Unlike Django, auto_now_add can be overridden by setting the
  # value before writing the entity.  And unlike classic db, auto_now
  # does not supply a default value.  Also unlike classic db, when the
  # entity is written, the property values are updated to match what
  # was written.  Finally, beware that this also updates the value in
  # the in-process cache, *and* that auto_now_add may interact weirdly
  # with transaction retries (a retry of a property with auto_now_add
  # set will reuse the value that was set on the first try).

  _attributes = Property._attributes + ['_auto_now', '_auto_now_add']

  @datastore_rpc._positional(1 + Property._positional)
  def __init__(self, name=None, auto_now=False, auto_now_add=False, **kwds):
    super(DateTimeProperty, self).__init__(name=name, **kwds)
    if self._repeated:
      assert not auto_now
      assert not auto_now_add
    self._auto_now = auto_now
    self._auto_now_add = auto_now_add

  def Validate(self, value):
    if not isinstance(value, datetime.datetime):
      raise datastore_errors.BadValueError('Expected datetime, got %r' %
                                           (value,))
    return value

  def Now(self):
    return datetime.datetime.now()

  def Serialize(self, entity, *rest):
    if (self._auto_now or
        (self._auto_now_add and self.RetrieveValue(entity) is None)):
      value = self.Now()
      self.StoreValue(entity, value)
    super(DateTimeProperty, self).Serialize(entity, *rest)

  def DbSetValue(self, v, p, value):
    assert isinstance(value, datetime.datetime)
    assert value.tzinfo is None
    dt = value - _EPOCH
    ival = dt.microseconds + 1000000 * (dt.seconds + 24*3600 * dt.days)
    v.set_int64value(ival)
    p.set_meaning(entity_pb.Property.GD_WHEN)

  def DbGetValue(self, v, p):
    if not v.has_int64value():
      return None
    ival = v.int64value()
    return _EPOCH + datetime.timedelta(microseconds=ival)


def _date_to_datetime(value):
  """Convert a date to a datetime for datastore storage.

  Args:
    value: A datetime.date object.

  Returns:
    A datetime object with time set to 0:00.
  """
  assert isinstance(value, datetime.date)
  return datetime.datetime(value.year, value.month, value.day)


def _time_to_datetime(value):
  """Convert a time to a datetime for datastore storage.

  Args:
    value: A datetime.time object.

  Returns:
    A datetime object with date set to 1970-01-01.
  """
  assert isinstance(value, datetime.time)
  return datetime.datetime(1970, 1, 1,
                           value.hour, value.minute, value.second,
                           value.microsecond)


class DateProperty(DateTimeProperty):

  def Validate(self, value):
    if (not isinstance(value, datetime.date) or
        isinstance(value, datetime.datetime)):
      raise datastore_errors.BadValueError('Expected date, got %r' %
                                           (value,))
    return value

  def Now(self):
    return datetime.date.today()

  def DbSetValue(self, v, p, value):
    value = _date_to_datetime(value)
    super(DateProperty, self).DbSetValue(v, p, value)

  def DbGetValue(self, v, p):
    value = super(DateProperty, self).DbGetValue(v, p)
    return value.date()


class TimeProperty(DateTimeProperty):

  def Validate(self, value):
    if not isinstance(value, datetime.time):
      raise datastore_errors.BadValueError('Expected time, got %r' %
                                           (value,))
    return value

  def Now(self):
    return datetime.datetime.now().time()

  def DbSetValue(self, v, p, value):
    value = _time_to_datetime(value)
    super(TimeProperty, self).DbSetValue(v, p, value)

  def DbGetValue(self, v, p):
    value = super(TimeProperty, self).DbGetValue(v, p)
    return value.time()


class StructuredProperty(Property):

  _modelclass = None

  _attributes = ['_modelclass'] + Property._attributes
  _positional = 2

  @datastore_rpc._positional(1 + _positional)
  def __init__(self, modelclass, name=None, **kwds):
    super(StructuredProperty, self).__init__(name=name, **kwds)
    if self._repeated:
      assert not modelclass._has_repeated
    self._modelclass = modelclass

  def FixUp(self, code_name):
    super(StructuredProperty, self).FixUp(code_name)
    self.FixUpNestedProperties()

  def FixUpNestedProperties(self):
    for name, prop in self._modelclass._properties.iteritems():
      prop_copy = copy.copy(prop)
      prop_copy._name = self._name + '.' + prop._name
      if isinstance(prop_copy, StructuredProperty):
        # Guard against simple recursive model definitions.
        # See model_test: testRecursiveStructuredProperty().
        # TODO: Guard against indirect recursion.
        if prop_copy._modelclass is not self._modelclass:
          prop_copy.FixUpNestedProperties()
      setattr(self, prop._code_name, prop_copy)

  def _comparison(self, op, value):
    if op != '=':
      raise datastore_errors.BadFilterError(
        'StructuredProperty filter can only use ==')
    # Import late to avoid circular imports.
    from ndb.query import FilterNode, ConjunctionNode, PostFilterNode
    value = self.Validate(value)  # None is not allowed!
    filters = []
    for name, prop in value._properties.iteritems():
      val = prop.RetrieveValue(value)
      if val is not None:
        filters.append(FilterNode(self._name + '.' + name, op, val))
    if not filters:
      raise datastore_errors.BadFilterError(
        'StructuredProperty filter without any values')
    if len(filters) == 1:
      return filters[0]
    filters.append(PostFilterNode(self._filter_func, value))
    return ConjunctionNode(filters)

  def _filter_func(self, value, entity):
    if isinstance(entity, Key):
      raise datastore_errors.BadQueryError(
        'StructuredProperty filter cannot be used with keys_only query')
    subentities = getattr(entity, self._code_name, None)
    if subentities is None:
      return False
    if not isinstance(subentities, list):
      subentities = [subentities]
    for subentity in subentities:
      for name, prop in value._properties.iteritems():
        val = prop.RetrieveValue(value)
        if val is not None:
          if prop.RetrieveValue(subentity) != val:
            break
      else:
        return True
    return False

  def Validate(self, value):
    if not isinstance(value, self._modelclass):
      raise datastore_errors.BadValueError('Expected %s instance, got %r' %
                                           (self._modelclass.__name__, value))
    return value

  def Serialize(self, entity, pb, prefix='', parent_repeated=False):
    # entity -> pb; pb is an EntityProto message
    value = self.RetrieveValue(entity)
    if value is None:
      # TODO: Is this the right thing for queries?
      # Skip structured values that are None.
      return
    cls = self._modelclass
    if self._repeated:
      assert isinstance(value, list)
      values = value
    else:
      assert isinstance(value, cls)
      values = [value]
    for value in values:
      # TODO: Avoid re-sorting for repeated values.
      for name, prop in sorted(value._properties.iteritems()):
        prop.Serialize(value, pb, prefix + self._name + '.',
                       self._repeated or parent_repeated)

  def Deserialize(self, entity, p, depth=1):
    if not self._repeated:
      subentity = self.RetrieveValue(entity)
      if subentity is None:
        subentity = self._modelclass()
        self.StoreValue(entity, subentity)
      assert isinstance(subentity, self._modelclass)
      prop = subentity._get_property_for(p, depth=depth)
      prop.Deserialize(subentity, p, depth + 1)
      return

    # The repeated case is more complicated.
    # TODO: Prove we won't get here for orphans.
    name = p.name()
    parts = name.split('.')
    assert len(parts) > depth, (depth, name, parts)
    next = parts[depth]
    prop = self._modelclass._properties.get(next)
    assert prop is not None  # QED

    values = self.RetrieveValue(entity)
    if values is None:
      values = []
    elif not isinstance(values, list):
      values = [values]
    self.StoreValue(entity, values)
    # Find the first subentity that doesn't have a value for this
    # property yet.
    for sub in values:
      assert isinstance(sub, self._modelclass)
      if not prop.HasValue(sub):
        subentity = sub
        break
    else:
      subentity = self._modelclass()
      values.append(subentity)
    prop.Deserialize(subentity, p, depth + 1)


_MEANING_COMPRESSED = 18


class LocalStructuredProperty(Property):
  """Substructure that is serialized to an opaque blob.

  This looks like StructuredProperty on the Python side, but is
  written to the datastore as a single opaque blob.  It is not indexed
  and you cannot query for subproperties.
  """

  _indexed = False
  _compressed = False
  _modelclass = None

  _attributes = ['_modelclass'] + Property._attributes + ['_compressed']
  _positional = 2

  @datastore_rpc._positional(1 + _positional)
  def __init__(self, modelclass, name=None, compressed=False, **kwds):
    super(LocalStructuredProperty, self).__init__(name=name, **kwds)
    assert not self._indexed
    if self._repeated:
      assert not modelclass._has_repeated
    self._modelclass = modelclass
    self._compressed = compressed

  def Validate(self, value):
    if not isinstance(value, self._modelclass):
      raise datastore_errors.BadValueError('Expected %s instance, got %r' %
                                           (self._modelclass.__name__, value))
    return value

  def DbSetValue(self, v, p, value):
    pb = value._to_pb()
    serialized = pb.Encode()
    if self._compressed:
      p.set_meaning(_MEANING_COMPRESSED)
      v.set_stringvalue(zlib.compress(serialized))
    else:
      p.set_meaning(entity_pb.Property.BLOB)
      v.set_stringvalue(serialized)

  def DbGetValue(self, v, p):
    if not v.has_stringvalue():
      return None
    serialized = v.stringvalue()
    if p.has_meaning() and p.meaning() == _MEANING_COMPRESSED:
      serialized = zlib.decompress(serialized)
    pb = entity_pb.EntityProto(serialized)
    return self._modelclass._from_pb(pb, set_key=False)


class GenericProperty(Property):
  # This is mainly used for orphans but can also be used explicitly
  # for properties with dynamically-typed values, and in Expandos.

  def DbGetValue(self, v, p):
    # This is awkward but there seems to be no faster way to inspect
    # what union member is present.  datastore_types.FromPropertyPb(),
    # the undisputed authority, has the same series of if-elif blocks.
    # (We don't even want to think about multiple members... :-)
    if v.has_stringvalue():
      sval = v.stringvalue()
      if p.meaning() not in (entity_pb.Property.BLOB,
                             entity_pb.Property.BYTESTRING):
        try:
          sval.decode('ascii')
          # If this passes, don't return unicode.
        except UnicodeDecodeError:
          try:
            sval = unicode(sval.decode('utf-8'))
          except UnicodeDecodeError:
            pass
      return sval
    elif v.has_int64value():
      ival = v.int64value()
      if p.meaning() == entity_pb.Property.GD_WHEN:
        return _EPOCH + datetime.timedelta(microseconds=ival)
      return ival
    elif v.has_booleanvalue():
      # The booleanvalue field is an int32, so booleanvalue() returns
      # an int, hence the conversion.
      return bool(v.booleanvalue())
    elif v.has_doublevalue():
      return v.doublevalue()
    elif v.has_referencevalue():
      rv = v.referencevalue()
      pairs = [(elem.type(), elem.id() or elem.name())
               for elem in rv.pathelement_list()]
      return Key(pairs=pairs)  # TODO: app, namespace
    elif v.has_pointvalue():
      pv = v.pointvalue()
      return GeoPt(pv.x(), pv.y())
    elif v.has_uservalue():
      return _unpack_user(v)
    else:
      # A missing value implies null.
      return None

  def DbSetValue(self, v, p, value):
    # TODO: use a dict mapping types to functions
    if isinstance(value, str):
      v.set_stringvalue(value)
      # TODO: Set meaning to BLOB or BYTESTRING if it's not UTF-8?
      # (Or TEXT if unindexed.)
    elif isinstance(value, unicode):
      v.set_stringvalue(value.encode('utf8'))
      if not self._indexed:
        p.set_meaning(entity_pb.Property.TEXT)
    elif isinstance(value, bool):  # Must test before int!
      v.set_booleanvalue(value)
    elif isinstance(value, (int, long)):
      assert -2**63 <= value < 2**63
      v.set_int64value(value)
    elif isinstance(value, float):
      v.set_doublevalue(value)
    elif isinstance(value, Key):
      # See datastore_types.PackKey
      ref = value._reference()  # Don't copy
      rv = v.mutable_referencevalue()  # A Reference
      rv.set_app(ref.app())
      if ref.has_name_space():
        rv.set_name_space()
      for elem in ref.path().element_list():
        rv.add_pathelement().CopyFrom(elem)
    elif isinstance(value, datetime.datetime):
      assert value.tzinfo is None
      dt = value - _EPOCH
      ival = dt.microseconds + 1000000 * (dt.seconds + 24*3600 * dt.days)
      v.set_int64value(ival)
      p.set_meaning(entity_pb.Property.GD_WHEN)
    elif isinstance(value, GeoPt):
      pv = v.mutable_pointvalue()
      pv.set_x(value.lat)
      pv.set_y(value.lon)
    elif isinstance(value, users.User):
      datastore_types.PackUser(p.name(), value, v)
    else:
      # TODO: blobkey, atom and gdata types
      assert False, type(value)


class ComputedProperty(GenericProperty):
  """A property that has its value determined by a user-supplied function.

  Computed properties cannot be set directly, but are instead generated by a
  function when required. They are useful to provide fields in the datastore
  that can be used for filtering or sorting without having to manually set the
  value in code - for example, sorting on the length of a BlobProperty, or
  using an equality filter to check if another field is not empty.

  ComputedProperty can be declared as a regular property, passing a function as
  the first argument, or it can be used as a decorator for the function that
  does the calculation.

  Example:

  >>> class DatastoreFile(Model):
  ...   name = StringProperty()
  ...   name_lower = ComputedProperty(lambda self: self.name.lower())
  ...
  ...   data = BlobProperty()
  ...
  ...   @ComputedProperty
  ...   def size(self):
  ...     return len(self.data)
  ...
  ...   def _compute_hash(self):
  ...     return hashlib.sha1(self.data).hexdigest()
  ...   hash = ComputedProperty(_compute_hash, name='sha1')
  """

  def __init__(self, derive_func, *args, **kwargs):
    """Constructor.

    Args:
      func: A function that takes one argument, the model instance, and returns
            a calculated value.
    """
    super(ComputedProperty, self).__init__(*args, **kwargs)
    self.__derive_func = derive_func

  def HasValue(self, entity):
    return True

  def StoreValue(self, entity, value):
    raise ComputedPropertyError("Cannot assign to a ComputedProperty")

  def DeleteValue(self, entity):
    raise ComputedPropertyError("Cannot delete a ComputedProperty")

  def RetrieveValue(self, entity):
    return self.__derive_func(entity)


class Expando(Model):

  def _set_attributes(self, kwds):
    for name, value in kwds.iteritems():
      setattr(self, name, value)

  def __getattr__(self, name):
    if (name.startswith('_') or
        isinstance(getattr(self.__class__, name, None), Property)):
      return super(Expando, self).__getattr__(name)
    prop = self._properties.get(name)
    if prop is None:
      return super(Expando, self).__getattribute__(name)
    return prop.GetValue(self)

  def __setattr__(self, name, value):
    if (name.startswith('_') or
        isinstance(getattr(self.__class__, name, None), Property)):
      return super(Expando, self).__setattr__(name, value)
    self._clone_properties()
    if isinstance(value, Model):
      prop = StructuredProperty(Model, name)
    else:
      prop = GenericProperty(name)
    prop._code_name = name
    self._properties[name] = prop
    prop.SetValue(self, value)


@datastore_rpc._positional(1)
def transaction(callback, retry=None, entity_group=None):
  """Run a callback in a transaction.

  Args:
    callback: A function or tasklet to be called.
    retry: Optional retry count (keyword only; default set by
      ndb.context.Context.transaction()).
    entity_group: Optional root key to use as transaction entity group
      (keyword only; defaults to the root part of the first key used
      in the transaction).

  Returns:
    Whatever callback() returns.

  Raises:
    Whatever callback() raises; datastore_errors.TransactionFailedError
    if the transaction failed.

  Note:
    To pass arguments to a callback function, use a lambda, e.g.
      def my_callback(key, inc):
        ...
      transaction(lambda: my_callback(Key(...), 1))
  """
  fut = transaction_async(callback, retry=retry, entity_group=entity_group)
  return fut.get_result()


@datastore_rpc._positional(1)
def transaction_async(callback, retry=None, entity_group=None):
  """Run a callback in a transaction.

  This is the asynchronous version of transaction().
  """
  from ndb import tasklets
  kwds = {}
  if retry is not None:
    kwds['retry'] = retry
  if entity_group is not None:
    kwds['entity_group'] = entity_group
  return tasklets.get_context().transaction(callback, **kwds)


def get_multi_async(keys):
  """Fetches a sequence of keys.

  Args:
    keys: A sequence of keys.

  Returns:
    A list of futures.
  """
  return [key.get_async() for key in keys]


def get_multi(keys):
  """Fetches a sequence of keys.

  Args:
    keys: A sequence of keys.

  Returns:
    A list whose items are either a Model instance or None if the key wasn't
    found.
  """
  return [future.get_result() for future in get_multi_async(keys)]


def put_multi_async(models):
  """Stores a sequence of Model instances.

  Args:
    models: A sequence of Model instances.

  Returns:
    A list of futures.
  """
  return [model.put_async() for model in models]


def put_multi(models):
  """Stores a sequence of Model instances.

  Args:
    models: A sequence of Model instances.

  Returns:
    A list with the stored keys.
  """
  return [future.get_result() for future in put_multi_async(models)]


def delete_multi_async(keys):
  """Deletes a sequence of keys.

  Returns:
    A list of futures.
  """
  return [key.delete_async() for key in keys]


def delete_multi(keys):
  """Deletes a sequence of keys.

  Args:
    keys: A sequence of keys.
  """
  # A list full of Nones!!!
  return [future.get_result() for future in delete_multi_async(keys)]


# Update __all__ to contain all Property and Exception subclasses.
for _name, _object in globals().items():
  if ((_name.endswith('Property') and issubclass(_object, Property)) or
      (_name.endswith('Error') and issubclass(_object, Exception))):
    __all__.append(_name)
