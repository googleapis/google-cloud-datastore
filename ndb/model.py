"""Model class and associated stuff.

TODO: docstrings, style
"""

import calendar
import datetime
import logging

from google.appengine.datastore import entity_pb

from core import datastore_rpc

# NOTE: Key is meant for export, too.
from ndb.key import Key, _ReferenceFromPairs, _DefaultAppId

kind_map = {}  # Dict mapping {kind: Model subclass}

class ModelAdapter(datastore_rpc.AbstractAdapter):

  def pb_to_key(self, pb):
    return Key(reference=pb)

  def key_to_pb(self, key):
    return key.reference()

  def pb_to_entity(self, pb):
    kind = None
    if pb.has_key():
      key = Key(reference=pb.key())  # TODO: Avoid doing this twice
      for kind, _ in key.pairs():
        pass  # As a side effect, set kind to the last kind, if any
    modelclass = kind_map.get(kind, Model)
    ent = modelclass()
    ent.FromPb(pb)
    return ent

  def entity_to_pb(self, ent):
    pb = ent.ToPb()
    return pb

conn = datastore_rpc.Connection(adapter=ModelAdapter())

class Model(object):
  """A mutable datastore entity."""

  # TODO: Prevent accidental attribute assignments

  _properties = None  # Set to a dict by FixUpProperties()
  _db_properties = None  # Set to a dict by FixUpProperties()
  _has_repeated = False

  # TODO: Make _ versions of all methods, and make non-_ versions
  # simple aliases. That way the _ version is still accessible even if
  # the non-_ version has been obscured by a property.

  # TODO: Prevent property names starting with _

  # TODO: Distinguish between purposes: to call FromPb() or setvalue() etc.
  # TODO: Support keyword args to initialize property values
  def __init__(self, **kwds):
    cls = self.__class__
    if kwds:
      # TODO: Enable this unconditionally (it currently breaks some old tests)
      if cls._properties is None:
        FixUpProperties(cls)
    self._key = None
    self._values = {}
    for name, value in kwds.iteritems():
      prop = getattr(cls, name)
      assert isinstance(prop, Property)
      prop.SetValue(self, value)

  # TODO: Make a property 'kind'?
  @classmethod
  def getkind(cls):
    return cls.__name__

  def getkey(self):
    return self._key

  def setkey(self, key):
    if key is not None:
      assert isinstance(key, Key)
      if self.__class__ is not Model:
        assert list(key.pairs())[-1][0] == self.__class__.__name__
    self._key = key

  def delkey(self):
    self._key = None

  key = property(getkey, setkey, delkey)

  def getvalue(self, name, default=None):
    return self._values.get(name, default)

  def setvalue(self, name, value):
    self._values[name] = value

  def delvalue(self, name):
    if name in self._values:
      del self._values[name]

  def propnames(self):
    return self._values.keys()

  def __hash__(self):
    raise TypeError('Model is not immutable')

  def __eq__(self, other):
    if other.__class__ is not self.__class__:
      return NotImplemented
    # It's okay to use private names -- we're the same class
    if self._key != other._key:
      return False
    return self._values == other._values

  def __ne__(self, other):
    eq = self.__eq__(other)
    if eq is  NotImplemented:
      return NotImplemented
    return not eq

  def ToPb(self):
    pb = entity_pb.EntityProto()
    key = self._key
    if key is None:
      ref = _ReferenceFromPairs([(self.getkind(), None)], pb.mutable_key())
      ref.set_app(_DefaultAppId())
    else:
      ref = key._Key__reference  # Don't copy
      pb.mutable_key().CopyFrom(ref)
    group = pb.mutable_entity_group()
    elem = ref.path().element(0)
    if elem.id() or elem.name():
      group.add_element().CopyFrom(elem)
    if self._properties is not None:
      # TODO: Sort by property declaration order
      for name, prop in sorted(self._properties.iteritems()):
        prop.Serialize(self, pb)
    else:
      # TODO: Change this to only do "orphan" values
      for name, value in sorted(self._values.iteritems()):
        # TODO: list properties
        serialized = _SerializeProperty(name, value)
        if self._IsUnindexed(name, value):
          pb.raw_property_list().append(serialized)
        else:
          pb.property_list().append(serialized)
    return pb

  def _IsUnindexed(self, name, value):
    # TODO: Kill this (it's subsumed by Property.indexed)
    return isinstance(value, basestring) and len(value) > 500

  # TODO: Make this a class method?
  def FromPb(self, pb):
    assert not self._key
    assert not self._values
    assert isinstance(pb, entity_pb.EntityProto)
    if pb.has_key():
      self._key = Key(reference=pb.key())
    for plist in pb.property_list(), pb.raw_property_list():
      for p in plist:
        db_name = p.name()
        if self._db_properties is not None:
          prop = self._db_properties.get(db_name)
          if prop is None and '.' in db_name:
            # Hackish approach to structured properties
            head, tail = db_name.split('.', 1)
            prop = self._db_properties.get(head)
          if prop is not None:
            prop.Deserialize(self, p)
            continue
        # TODO: Use a GenericProperty for this case
        assert not p.multiple()
        # TODO: utf8 -> unicode?
        assert db_name not in self._values  # TODO: support list values
        value = _DeserializeProperty(p)
        self._values[db_name] = value

  @classmethod
  def get(cls, key):
    return conn.get([key])[0]

  def put(self):
    key = conn.put([self])[0]
    if self._key != key:
      self._key = key
    return key

  def delete(self):
    conn.delete([self.key()])

  # TODO: queries, transaction
  # TODO: lifecycle hooks

def _SerializeProperty(name, value, multiple=False):
  assert isinstance(name, basestring)
  if isinstance(name, unicode):
    name = name.encode('utf8')
  p = entity_pb.Property()
  p.set_name(name)
  p.set_multiple(multiple)  # Why on earth is this a required field?
  v = p.mutable_value()  # a PropertyValue
  # TODO: use a dict mapping types to functions
  if isinstance(value, str):
    v.set_stringvalue(value)
  elif isinstance(value, unicode):
    v.set_stringvalue(value.encode('utf8'))
  elif isinstance(value, bool):  # Must test before int!
    v.set_booleanvalue(value)
  elif isinstance(value, (int, long)):
    assert -2**63 <= value < 2**63
    v.set_int64value(value)
  elif isinstance(value, float):
    v.set_doublevalue(value)
  elif isinstance(value, Key):
    # See datastore_types.PackKey
    ref = value._Key__reference  # Don't copy
    rv = v.mutable_referencevalue()  # A Reference
    rv.set_app(ref.app())
    if ref.has_name_space():
      rv.set_name_space()
    for elem in ref.path().element_list():
      rv.add_pathelement().CopyFrom(elem)
  elif isinstance(value, datetime.datetime):
    assert value.tzinfo is None
    ival = (long(calendar.timegm(value.timetuple()) * 1000000L) +
            value.microsecond)
    v.set_int64value(ival)
    p.set_meaning(entity_pb.Property.GD_WHEN)
  else:
    # TODO: blob, blobkey, user, datetime, atom types, gdata types, geopt
    assert False, type(value)
  return p

_EPOCH = datetime.datetime.utcfromtimestamp(0)

def _DeserializeProperty(p):
  v = p.value()
  if v.has_stringvalue():
    return v.stringvalue()
  elif v.has_booleanvalue():
    return v.booleanvalue()
  elif v.has_int64value():
    ival = v.int64value()
    if p.meaning() == entity_pb.Property.GD_WHEN:
      return _EPOCH + datetime.timedelta(microseconds=ival)
    return ival
  elif v.has_doublevalue():
    return v.doublevalue()
  elif v.has_referencevalue():
    rv = v.referencevalue()
    pairs = [(elem.type(), elem.id() or elem.name())
             for elem in rv.pathelement_list()]
    return Key(pairs=pairs)  # TODO: app, namespace
  else:
    assert False, str(v)

### Properties done right ###

# TODO: Kill _SerializeProperty() and _DeserializeProperty() above
# TODO: Use a metaclass to automatically call FixUpProperties()
# TODO: More Property types
# TODO: Generic properties (to be used by Expando models)
# TODO: Split Property into Property and SimpleProperty

class Property(object):
  # TODO: Separate 'simple' properties from base Property class

  indexed = True
  repeated = False

  def __init__(self, db_name=None, indexed=None, repeated=None):
    # Don't set self.name -- it's set by FixUp()
    if db_name:
      assert '.' not in db_name  # The '.' is used elsewhere.
    self.db_name = db_name
    if indexed is not None:
      self.indexed = indexed
    if repeated is not None:
      self.repeated = repeated

  def FixUp(self, name):
    self.name = name
    if self.db_name is None:
      self.db_name = name

  def SetValue(self, entity, value):
    if self.repeated:
      assert isinstance(value, list)
    else:
      assert not isinstance(value, list)
    # TODO: validation
    entity._values[self.name] = value

  def GetValue(self, entity):
    return entity._values.get(self.name)

  def __get__(self, obj, cls=None):
    if obj is None:
      return self  # __get__ called on class
    return self.GetValue(obj)

  def __set__(self, obj, value):
    self.SetValue(obj, value)

  # TODO: __delete__

  def Serialize(self, entity, pb, prefix=''):
    # entity -> pb; pb is an EntityProto message
    # TODO: None vs. empty list
    value = entity._values.get(self.name)
    if self.repeated:
      assert isinstance(value, list)
    else:
      assert not isinstance(value, list)
      value = [value]
    for val in value:
      if self.indexed:
        p = pb.add_property()
      else:
        p = pb.add_raw_property()
      p.set_name(prefix + self.db_name)
      p.set_multiple(self.repeated)
      v = p.mutable_value()
      if val is not None:
        self.DbSetValue(v, val)

  def Deserialize(self, entity, p, prefix=''):
    # entity <- p; p is a Property message
    # In this class, prefix is unused.
    v = p.value()
    val = self.DbGetValue(v)
    if self.repeated:
      if self.name in entity._values:
        value = entity._values[self.name]
        if not isinstance(value, list):
          value = [value]
        value.append(val)
      else:
        value = [val]
    else:
      # TODO: What if we don't have the repeated flag set, yet
      # multiple values are read from the datastore?
      value = val
    entity._values[self.name] = value

class IntegerProperty(Property):

  def DbSetValue(self, v, value):
    assert isinstance(value, (bool, int, long))
    v.set_int64value(value)

  def DbGetValue(self, v):
    return int(v.int64value())

class StringProperty(Property):

  def DbSetValue(self, v, value):
    assert isinstance(value, basestring)
    if isinstance(value, unicode):
      value = value.encode('utf-8')
    v.set_stringvalue(value)

  def DbGetValue(self, v):
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

  def DbSetValue(self, v, value):
    assert isinstance(value, str)
    v.set_stringvalue(value)

  def DbGetValue(self, v):
    return v.stringvalue()

class KeyProperty(Property):
  # TODO: namespaces

  def DbSetValue(self, v, value):
    assert isinstance(value, Key)
    # See datastore_types.PackKey
    ref = value._Key__reference  # Don't copy
    rv = v.mutable_referencevalue()  # A Reference
    rv.set_app(ref.app())
    if ref.has_name_space():
      rv.set_name_space(ref.name_space())
    for elem in ref.path().element_list():
      rv.add_pathelement().CopyFrom(elem)

  def DbGetValue(self, v):
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

def FixUpProperties(cls):
  # NOTE: This may be called multiple times if properties are
  # dynamically added to the class.
  cls._properties = {}  # Map of {name: Property}
  cls._db_properties = {}  # Map of {db_name: Property}
  for name in set(dir(cls)):
    prop = getattr(cls, name, None)
    if isinstance(prop, Property):
      assert not name.startswith('_')
      prop.FixUp(name)
      if prop.repeated:
        cls._has_repeated = True
      cls._properties[name] = prop
      cls._db_properties[prop.db_name] = prop
  if issubclass(cls, Model):
    kind_map[cls.getkind()] = cls

class StructuredProperty(Property):

  def __init__(self, minimodelclass, db_name=None, indexed=None,
               repeated=None):
    if repeated:
      if minimodelclass._properties is None:
        FixUpProperties(minimodelclass)
      assert not minimodelclass._has_repeated
    super(StructuredProperty, self).__init__(db_name=db_name, indexed=indexed,
                                             repeated=repeated)
    self.minimodelclass = minimodelclass

  def Serialize(self, entity, pb, prefix=''):
    # entity -> pb; pb is an EntityProto message
    value = entity._values.get(self.name)
    if value is None:
      # TODO: Is this the right thing for queries?
      # Skip structured values that are None.
      return
    cls = self.minimodelclass
    if cls._properties is None:
      FixUpProperties(cls)
    if self.repeated:
      assert isinstance(value, list)
      values = value
    else:
      assert isinstance(value, cls)
      values = [value]
    # TODO: Sort by property declaration order
    items = sorted(cls._properties.iteritems())
    for value in values:
      for name, prop in items:
        prop.Serialize(value, pb, prefix + self.db_name + '.')

  def Deserialize(self, entity, p, prefix=''):
    db_name = p.name()
    subentity = entity._values.get(self.name)
    if subentity is None:
      subentity = self.minimodelclass()
      entity._values[self.name] = subentity
    # TODO: Distinguish py_name from db_name
    if prefix:
      assert prefix.endswith('.')
    n = prefix.count('.') + 1  # Nesting level
    parts = db_name.split('.')
    assert len(parts) > n, (prefix, db_name, parts, n)
    tail = parts[n]
    prop = self.minimodelclass._db_properties.get(tail)
    assert prop is not None, (prefix, db_name, parts, tail)
    if prop is not None:
      prop.Deserialize(subentity, p, prefix + tail + '.')
