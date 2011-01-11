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

We can also delete an entity:

  k.delete()

The property definitions in the class body tell the system the names
and the types of the fields to be stored in the datastore, whether
they must be indexed, their default value, and more.

Many different Property types exist, including StringProperty
(strings), IntegerProperty (64-bit signed integers), FloatProperties
(double precision floating point numbers).  Some more specialized
properties also exist: TextProperty represents a longer string that is
not indexed (StringProperty is limited to 500 bytes); BlobProperty
represents an uninterpreted, unindexed byte string; KeyProperty
represents a datastore Key.  Finally, StructuredProperty represents a
field that is itself structured like an entity -- more about these
later.

TODO: DatetimeProperty etc.

Most Property classes have the same constructor signature.  They
accept several optional keyword arguments: name=<string> to change the
name used to store the property value in the datastore,
indexed=<boolean> to indicate whether the property should be indexed
(allowing queries on this property's value), and repeated=<boolean> to
indicate that this property can have multiple values in the same
entity.  Repeated properties are always represented using Python
lists; if there is only one value, the list has only one element.

TODO: default and other keywords affecting validation.

TODO: More on StructuredProperty.

TODO: Querying support.
"""

__author__ = 'guido@google.com (Guido van Rossum)'

# TODO: docstrings, style.
# TODO: Change asserts to better exceptions.
# TODO: get full property name out of StructuredProperty
# TODO: validation; at least reject bad property types upon assignment
# TODO: reject unknown property names in assignment (for Model) (?)

import datetime
import logging

from google.appengine.datastore import datastore_rpc
from google.appengine.datastore import entity_pb

import ndb.key
Key = ndb.key.Key  # For export.

# Property and its subclasses are added later.
__all__ = ['Key', 'ModelAdapter', 'MetaModel', 'Model', 'Expando']


class ModelAdapter(datastore_rpc.AbstractAdapter):
  """Conveersions between 'our' Key and Model classes and protobufs.

  This is needed to construct a Connection object, which in turn is
  needed to construct a Context object.

  See the base class docstring for more info about the signatures.
  """

  def pb_to_key(self, pb):
    return Key(reference=pb)

  def key_to_pb(self, key):
    return key.reference()

  def pb_to_entity(self, pb):
    kind = None
    if pb.has_key():
      # TODO: Fix the inefficiency here: we extract the key just so we
      # can get the kind just so we can find the intended model class,
      # but the key is extracted again and stored in the entity by FromPb().
      key = Key(reference=pb.key())
      kind = key.kind()
    # When unpacking an unknown kind, default to Expando.
    modelclass = Model._kind_map.get(kind, Expando)
    ent = modelclass()
    ent.FromPb(pb)
    return ent

  def entity_to_pb(self, ent):
    pb = ent.ToPb()
    return pb

def make_connection(config=None):
  """Create a new Connection object with the right adapter."""
  return datastore_rpc.Connection(adapter=ModelAdapter(), config=config)


class MetaModel(type):
  """Metaclass for Model."""

  def __init__(cls, name, bases, classdict):
    super(MetaModel, cls).__init__(name, bases, classdict)
    cls.FixUpProperties()


class Model(object):
  """A mutable datastore entity."""

  __metaclass__ = MetaModel

  # TODO: Prevent accidental attribute assignments

  # Class variables updated by FixUpProperties()
  _properties = None
  _has_repeated = False
  _kind_map = {}  # Dict mapping {kind: Model subclass}

  # Defaults for instance variables.
  _key = None
  _values = None

  # TODO: Make _ versions of all methods, and make non-_ versions
  # simple aliases. That way the _ version is still accessible even if
  # the non-_ version has been obscured by a property.

  # TODO: Support things like Person(id=X) as a shortcut for
  # Person(key=Key(pairs=[(Person.GetKind(), X)]).
  # TODO: Add parent keyword so that Person(id=X, parent=Y) is the same as
  # Person(key=Key(pairs=Y.pairs() + [(Person.GetKind(), X)])).

  # TODO: Distinguish between purposes: to call FromPb() or setvalue() etc.
  @datastore_rpc._positional(1)
  def __init__(self, key=None, **kwds):
    self._key = key
    self._values = {}
    self.SetAttributes(kwds)

  def SetAttributes(self, kwds):
    cls = self.__class__
    for name, value in kwds.iteritems():
      prop = getattr(cls, name)  # Raises AttributeError for unknown properties.
      assert isinstance(prop, Property)
      prop.SetValue(self, value)

  def __repr__(self):
    args = []
    done = set()
    for prop in self._properties.itervalues():
      if prop.name in self._values:
        args.append('%s=%r' % (prop.code_name, self._values[prop.name]))
        done.add(prop.name)
    args.sort()
    if self._key is not None:
      args.insert(0, 'key=%r' % self._key)
    s = '%s(%s)' % (self.__class__.__name__, ', '.join(args))
    return s

  # TODO: Make kind a property also?
  @classmethod
  def GetKind(cls):
    return cls.__name__

  @classmethod
  def GetKindMap(cls):
    return cls._kind_map

  def _getkey(self):
    return self._key

  def _setkey(self, key):
    if key is not None:
      assert isinstance(key, Key), repr(key)
      if self.__class__ is not Model:
        assert list(key.pairs())[-1][0] == self.GetKind()
    self._key = key

  def _delkey(self):
    self._key = None

  key = property(_getkey, _setkey, _delkey)

  def __hash__(self):
    raise TypeError('Model is not immutable')

  def __eq__(self, other):
    if other.__class__ is not self.__class__:
      return NotImplemented
    # It's okay to use private names -- we're the same class
    if self._key != other._key:
      # TODO: If one key is None and the other is an explicit
      # incomplete key of the simplest form, this should be OK.
      return False
    # TODO: Turn the rest of this into an Equivalent() method.
    # Ignore differences in values that are None.
    self_values = [(name, value)
                   for name, value in self._values.iteritems()
                   if value is not None]
    self_values.sort()
    other_values = [(name, value)
                    for name, value in other._values.iteritems()
                    if value is not None]
    other_values.sort()
    return self_values == other_values

  def __ne__(self, other):
    eq = self.__eq__(other)
    if eq is  NotImplemented:
      return NotImplemented
    return not eq

  # TODO: Refactor ToPb() so pb is an argument?
  def ToPb(self):
    pb = entity_pb.EntityProto()

    # TODO: Move the key stuff into ModelAdapter.entity_to_pb()?
    key = self._key
    if key is None:
      ref = ndb.key._ReferenceFromPairs([(self.GetKind(), None)],
                                        reference=pb.mutable_key())
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

  # TODO: Make this a class method?
  def FromPb(self, pb):
    assert not self._key
    assert not self._values
    assert isinstance(pb, entity_pb.EntityProto)

    # TODO: Move the key stuff into ModelAdapter.pb_to_entity()?
    if pb.has_key():
      self._key = Key(reference=pb.key())

    indexed_properties = pb.property_list()
    unindexed_properties = pb.raw_property_list()
    for plist in [indexed_properties, unindexed_properties]:
      for p in plist:
        prop = self.GetPropertyFor(p, plist is indexed_properties)
        prop.Deserialize(self, p)

  def GetPropertyFor(self, p, indexed=True, depth=0):
    name = p.name()
    parts = name.split('.')
    assert len(parts) > depth, (p.name(), parts, depth)
    next = parts[depth]
    prop = self._properties.get(next)
    if prop is None:
      prop = self.FakeProperty(p, next, indexed)
    return prop

  def CloneProperties(self):
    cls = self.__class__
    if self._properties is cls._properties:
      self._properties = dict(cls._properties)

  def FakeProperty(self, p, next, indexed=True):
    self.CloneProperties()
    if p.name() != next and not p.name().endswith('.' + next):
      prop = StructuredProperty(Expando, next)
      self._values[prop.name] = Expando()
    else:
      prop = GenericProperty(next,
                             repeated=p.multiple(),
                             indexed=indexed)
    self._properties[prop.name] = prop
    return prop

  @classmethod
  def FixUpProperties(cls):
    # NOTE: This is called by MetaModel, but may also be called manually
    # after dynamically updating a model class.
    cls._properties = {}  # Map of {name: Property}
    if cls.__module__ == __name__:  # Skip the classes in *this* file.
      return
    for name in set(dir(cls)):
      prop = getattr(cls, name, None)
      if isinstance(prop, Property):
        assert not name.startswith('_')
        # TODO: Tell prop the class, for error message.
        prop.FixUp(name)
        if prop.repeated:
          cls._has_repeated = True
        cls._properties[prop.name] = prop
    cls._kind_map[cls.GetKind()] = cls

  @classmethod
  def ResetKindMap(cls):
    cls._kind_map.clear()

  @classmethod
  def query(cls, **kwds):
    from ndb.query import Query  # Import late to avoid circular imports.
    return Query(kind=cls.GetKind(), **kwds)

  # Datastore API using the default context.
  # These use local import since otherwise they'd be recursive imports.

  def put(self):
    return self.put_async().get_result()

  def put_async(self):
    from ndb import tasklets
    return tasklets.get_context().put(self)

  @classmethod
  def get_or_insert(cls, name, parent=None, **kwds):
    return cls.get_or_insert_async(cls, name=name, parent=parent, **kwds)

  @classmethod
  def get_or_insert_async(cls, name, parent=None, **kwds):
    from ndb import tasklets
    ctx = tasklets.get_context()
    return ctx.get_or_insert(cls, name=name, parent=parent, **kwds)

  @classmethod
  def allocate_ids(cls, size=None, max=None, parent=None):
    return cls.allocate_ids(cls, size=size, max=max, parent=parent)

  @classmethod
  def allocate_ids_async(cls, size=None, max=None, parent=None):
    from ndb import tasklets
    if parent is None:
      pairs = []
    else:
      pairs = parent.pairs()
    pairs.append((cls.GetKind(), None))
    key = Key(pairs=pairs)
    return tasklets.get_context().allocate_ids(key, size=size, max=max)

# TODO: More Property types


class Property(object):
  # TODO: Separate 'simple' properties from base Property class

  code_name = None
  name = None
  indexed = True
  repeated = False

  _attributes = ['name', 'indexed', 'repeated']
  _positional = 1

  @datastore_rpc._positional(1 + _positional)
  def __init__(self, name=None, indexed=None, repeated=None):
    if name is not None:
      assert '.' not in name  # The '.' is used elsewhere.
      self.name = name
    if indexed is not None:
      self.indexed = indexed
    if repeated is not None:
      self.repeated = repeated

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
          s = '%s=%s' % (attr, s)
        args.append(s)
    s = '%s(%s)' % (self.__class__.__name__, ', '.join(args))
    return s

  def _comparison(self, op, other):
    from ndb.query import FilterNode  # Import late to avoid circular imports.
    return FilterNode(self.name, op, other)

  def __eq__(self, other):
    return self._comparison('=', other)

  def __ne__(self, other):
    return self._comparison('!=', other)

  def __lt__(self, other):
    return self._comparison('<', other)

  def __le__(self, other):
    return self._comparison('<=', other)

  def __gt__(self, other):
    return self._comparison('>', other)

  def __ge__(self, other):
    return self._comparison('>=', other)

  def IN(self, other):
    return self._comparison('in', other)

  def FixUp(self, name):
    self.code_name = name
    if self.name is None:
      self.name = name

  def SetValue(self, entity, value):
    if self.repeated:
      assert isinstance(value, list)
    else:
      assert not isinstance(value, list)
    # TODO: validation
    entity._values[self.name] = value

  def GetValue(self, entity):
     value = entity._values.get(self.name)
     if value is None and self.repeated:
       value = []
       entity._values[self.name] = value
     return value

  def __get__(self, obj, cls=None):
    if obj is None:
      return self  # __get__ called on class
    return self.GetValue(obj)

  def __set__(self, obj, value):
    self.SetValue(obj, value)

  # TODO: __delete__

  def Serialize(self, entity, pb, prefix='', parent_repeated=False):
    # entity -> pb; pb is an EntityProto message
    value = entity._values.get(self.name)
    if value is None and self.repeated:
      value = []
    elif not isinstance(value, list):
      value = [value]
    for val in value:
      if self.indexed:
        p = pb.add_property()
      else:
        p = pb.add_raw_property()
      p.set_name(prefix + self.name)
      p.set_multiple(self.repeated or parent_repeated)
      v = p.mutable_value()
      if val is not None:
        self.DbSetValue(v, p, val)

  def Deserialize(self, entity, p, depth=1):
    # entity <- p; p is a Property message
    # In this class, depth is unused.
    v = p.value()
    val = self.DbGetValue(v, p)
    if self.repeated:
      if self.name in entity._values:
        value = entity._values[self.name]
        if not isinstance(value, list):
          value = [value]
        value.append(val)
      else:
        value = [val]
    else:
      if self.name not in entity._values:
        value = val
      else:
        # Maybe upgrade to a list property.  Or ignore null.
        oldval = entity._values[self.name]
        if val is None:
          value = oldval
        elif oldval is None:
          value = val
        elif isinstance(oldval, list):
          oldval.append(val)
          value = oldval
        else:
          value = [oldval, val]
    entity._values[self.name] = value


class IntegerProperty(Property):

  def DbSetValue(self, v, p, value):
    assert isinstance(value, (bool, int, long)), (self.name)
    v.set_int64value(value)

  def DbGetValue(self, v, p):
    if not v.has_int64value():
      return None
    return int(v.int64value())


class FloatProperty(Property):

  def DbSetValue(self, v, p, value):
    assert isinstance(value, (bool, int, long, float)), (self.name)
    v.set_doublevalue(float(value))

  def DbGetValue(self, v, p):
    if not v.has_doublevalue():
      return None
    return v.doublevalue()


class StringProperty(Property):

  def DbSetValue(self, v, p, value):
    assert isinstance(value, basestring)
    if isinstance(value, unicode):
      value = value.encode('utf-8')
    v.set_stringvalue(value)

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
  indexed = False


class BlobProperty(Property):
  indexed = False

  def DbSetValue(self, v, p, value):
    assert isinstance(value, str)
    v.set_stringvalue(value)

  def DbGetValue(self, v, p):
    if not v.has_stringvalue():
      return None
    return v.stringvalue()


class KeyProperty(Property):
  # TODO: namespaces
  # TODO: optionally check the kind (validation)

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


class StructuredProperty(Property):

  modelclass = None

  _attributes = ['modelclass'] + Property._attributes
  _positional = 2

  @datastore_rpc._positional(1 + _positional)
  def __init__(self, modelclass, name=None, indexed=None, repeated=None):
    super(StructuredProperty, self).__init__(name=name,
                                             indexed=indexed,
                                             repeated=repeated)
    if self.repeated:
      assert not modelclass._has_repeated
    self.modelclass = modelclass

  def Serialize(self, entity, pb, prefix='', parent_repeated=False):
    # entity -> pb; pb is an EntityProto message
    value = entity._values.get(self.name)
    if value is None:
      # TODO: Is this the right thing for queries?
      # Skip structured values that are None.
      return
    cls = self.modelclass
    if self.repeated:
      assert isinstance(value, list)
      values = value
    else:
      assert isinstance(value, cls)
      values = [value]
    for value in values:
      # TODO: Avoid re-sorting for repeated values.
      for name, prop in sorted(value._properties.iteritems()):
        prop.Serialize(value, pb, prefix + self.name + '.',
                       self.repeated or parent_repeated)

  def Deserialize(self, entity, p, depth=1):
    if not self.repeated:
      subentity = entity._values.get(self.name)
      if subentity is None:
        subentity = self.modelclass()
        entity._values[self.name] = subentity
      assert isinstance(subentity, self.modelclass)
      prop = subentity.GetPropertyFor(p, depth=depth)
      prop.Deserialize(subentity, p, depth + 1)
      return

    # The repeated case is more complicated.
    # TODO: Prove we won't get here for orphans.
    name = p.name()
    parts = name.split('.')
    assert len(parts) > depth, (depth, name, parts)
    next = parts[depth]
    prop = self.modelclass._properties.get(next)
    assert prop is not None  # QED

    if self.name in entity._values:
      values = entity._values[self.name]
      if not isinstance(values, list):
        values = [values]
    else:
      values = []
    entity._values[self.name] = values
    # Find the first subentity that doesn't have a value for this
    # property yet.
    for sub in values:
      assert isinstance(sub, self.modelclass)
      if prop.name not in sub._values:
        subentity = sub
        break
    else:
      subentity = self.modelclass()
      values.append(subentity)
    prop.Deserialize(subentity, p, depth + 1)


_EPOCH = datetime.datetime.utcfromtimestamp(0)

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
      return v.booleanvalue()
    elif v.has_doublevalue():
      return v.doublevalue()
    elif v.has_referencevalue():
      rv = v.referencevalue()
      pairs = [(elem.type(), elem.id() or elem.name())
               for elem in rv.pathelement_list()]
      return Key(pairs=pairs)  # TODO: app, namespace
    elif v.has_pointvalue():
      assert False, 'Points are not yet supported'
    elif v.has_uservalue():
      assert False, 'Users are not yet supported'
    else:
      # A missing value imples null.
      return None

  def DbSetValue(self, v, p, value):
    # TODO: use a dict mapping types to functions
    if isinstance(value, str):
      v.set_stringvalue(value)
      # TODO: Set meaning to BLOB if it's not UTF-8?
    elif isinstance(value, unicode):
      v.set_stringvalue(value.encode('utf8'))
      # TODO: I don't think this is correct; TEXT implies unindexed IIUC.
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
    else:
      # TODO: point, user, blobkey, date, time, atom and gdata types
      assert False, type(value)


class Expando(Model):

  def SetAttributes(self, kwds):
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
    self.CloneProperties()
    if isinstance(value, Model):
      prop = StructuredProperty(Model, name)
    else:
      prop = GenericProperty(name)
    prop.code_name = name
    self._properties[name] = prop
    prop.SetValue(self, value)


# Update __all__ to contain all Property subclasses.
for _name, _object in globals().items():
  if _name.endswith('Property') and issubclass(_object, Property):
    __all__.append(_name)
