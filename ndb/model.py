"""Model class and associated stuff.

TODO: docstrings, style
"""

import calendar
import datetime
import logging

from google.appengine.datastore import entity_pb

from core import datastore_rpc

from ndb.key import Key, _ReferenceFromPairs, _DefaultAppId

class ModelAdapter(datastore_rpc.AbstractAdapter):

  def pb_to_key(self, pb):
    return Key(reference=pb)

  def key_to_pb(self, key):
    return key.reference()

  def pb_to_entity(self, pb):
    ent = Model()
    ent.FromPb(pb)
    return ent

  def entity_to_pb(self, ent):
    pb = ent.ToPb()
    return pb

conn = datastore_rpc.Connection(adapter=ModelAdapter())

class Model(object):
  """A mutable datastore entity.

  TODO: almost everything
  """

  __slots__ = ['_values', '_key']

  _properties = None  # Set to a dict {name: Property} by FixUpProperties()

  # TODO: Distinguish between purposes: to call FromPb() or setvalue() etc.
  def __init__(self):
    self._key = None
    self._values = {}

  # TODO: Make a property 'kind'?
  def getkind(self):
    return self.__class__.__name__

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
    if not isinstance(other, Model):
      return NotImplemented
    if other.__class__ is not self.__class__:
      return False
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
    # TODO: Do this properly
    return isinstance(value, basestring) and len(value) > 500

  def FromPb(self, pb):
    assert not self._key
    assert not self._values
    assert isinstance(pb, entity_pb.EntityProto)
    if pb.has_key():
      self._key = Key(reference=pb.key())
    for pblist in pb.property_list(), pb.raw_property_list():
      for pb in pblist:
        name = pb.name()
        if self._properties is not None:
          prop = self._properties.get(name)
          if prop is not None:
            # TODO: This may not be right for structured properties
            prop.Deserialize(self, pb)
            continue
        assert not pb.multiple()
        # TODO: utf8 -> unicode?
        assert name not in self._values  # TODO: support list values
        value = _DeserializeProperty(pb)
        self._values[name] = value

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
  pb = entity_pb.Property()
  pb.set_name(name)
  pb.set_multiple(multiple)  # Why on earth is this a required field?
  v = pb.mutable_value()  # a PropertyValue
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
    pb.set_meaning(entity_pb.Property.GD_WHEN)
  else:
    # TODO: blob, blobkey, user, datetime, atom types, gdata types, geopt
    assert False, type(value)
  return pb

_EPOCH = datetime.datetime.utcfromtimestamp(0)

def _DeserializeProperty(pb):
  v = pb.value()
  if v.has_stringvalue():
    return v.stringvalue()
  elif v.has_booleanvalue():
    return v.booleanvalue()
  elif v.has_int64value():
    ival = v.int64value()
    if pb.meaning() == entity_pb.Property.GD_WHEN:
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
# TODO: Make Property a descriptor
# TODO: Use a metaclass to automatically call FixUpProperties()
# TODO: More Property types
# TODO: Generic properties (to be used by Expando models)
# TODO: Decide on names starting with underscore
# TODO: List properties (and Set and Dict)
# TODO: Use a builder pattern in the [de]serialization API
# TODO: etc., etc., etc.

class Property(object):
  # TODO: Separate 'simple' properties from base Property class

  indexed = True

  def __init__(self, db_name=None):
    # Don't set self.name -- it's set by FixUp()
    self.db_name = db_name

  def FixUp(self, name):
    self.name = name
    if self.db_name is None:
      self.db_name = name

  def SetValue(self, entity, value):
    # TODO: validation
    entity._values[self.name] = value

  def GetValue(self, entity):
    return entity._values.get(self.name)

  def Serialize(self, entity, pb):
    # entity -> pb; pb is an EntityProto message
    value = entity._values.get(self.name)
    if self.indexed:
      p = pb.add_property()
    else:
      p = pb.add_raw_property()
    p.set_name(self.db_name)
    p.set_multiple(False)
    v = p.mutable_value()
    if value is not None:
      self.DbSetValue(v, value)

  def Deserialize(self, entity, p):
    # entity <- p; p is a Property message
    v = p.value()
    value = self.DbGetValue(v)
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
  cls._properties = {}
  for name in set(dir(cls)):
    prop = getattr(cls, name, None)
    if isinstance(prop, Property):
      prop.FixUp(name)
      cls._properties[name] = prop
