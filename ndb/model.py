"""Model and Property classes and associated stuff.

TODO: docstrings, style, asserts
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

  # Class variables updated by FixUpProperties()
  _properties = None
  _db_properties = None
  _has_repeated = False

  # Defaults for instance variables.
  _key = None
  _values = None

  # TODO: Make _ versions of all methods, and make non-_ versions
  # simple aliases. That way the _ version is still accessible even if
  # the non-_ version has been obscured by a property.

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

  def __repr__(self):
    s = '%s(**%s)' % (self.__class__.__name__, self._values)
    if self._key is not None:
      s += '<key=%s>' % self._key
    return s

  # TODO: Make a property 'kind'?
  @classmethod
  def getkind(cls):
    return cls.__name__

  def _getkey(self):
    return self._key

  def _setkey(self, key):
    if key is not None:
      assert isinstance(key, Key)
      if self.__class__ is not Model:
        assert list(key.pairs())[-1][0] == self.getkind()
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
    # Ignore differences in values that are None.
    self_values = [(name, value)
                   for name, value in self._values.iteritems()
                   if value is not None]
    other_values = [(name, value)
                    for name, value in other._values.iteritems()
                    if value is not None]
    return self_values == other_values

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
    if self._properties:
      # TODO: Sort by property declaration order
      for name, prop in sorted(self._properties.iteritems()):
        prop.Serialize(self, pb)
    return pb

  # TODO: Make this a class method?
  def FromPb(self, pb):
    assert not self._key
    assert not self._values
    assert isinstance(pb, entity_pb.EntityProto)
    if pb.has_key():
      self._key = Key(reference=pb.key())
    for plist in pb.property_list(), pb.raw_property_list():
      for p in plist:
        # TODO: There's code to be shared here with
        # StructuredProperty.Deserialize()
        db_name = p.name()
        head = db_name
        if '.' in db_name:
          head, tail = db_name.split('.', 1)
        if self._db_properties:
          prop = self._db_properties.get(head)
          if prop is not None:
            prop.Deserialize(self, p)
            continue
        prop = FakeProperty(self, p, db_name, head,
                            (plist is pb.property_list()))
        prop.Deserialize(self, p)

  # TODO: Move db methods out of this class?

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

def FakeProperty(self, p, db_name, head, indexed=True):
  cls = self.__class__
  if self._db_properties is cls._db_properties:
    self._db_properties = dict(cls._db_properties or ())
  if self._properties is cls._properties:
    self._properties = dict(cls._properties or ())
  if '.' in db_name:
    prop = StructuredProperty(Model, head)
  else:
    assert head == db_name
    prop = GenericProperty(head,
                           repeated=p.multiple(),
                           indexed=indexed)
  prop.FixUp(str(id(prop)))  # Use a unique string as Python name.
  self._db_properties[prop.db_name] = prop
  self._properties[prop.name] = prop
  return prop

# TODO: Use a metaclass to automatically call FixUpProperties()
# TODO: More Property types
# TODO: Orphan properties

class Property(object):
  # TODO: Separate 'simple' properties from base Property class

  name = None
  db_name = None
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

  def __repr__(self):
    s = '%s(db_name=%r, indexed=%r, repeated=%r)' % (
      self.__class__.__name__,
      self.db_name, self.indexed, self.repeated)
    if self.name != self.db_name:
      s += '<name=%r>' % self.name
    return s

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
        self.DbSetValue(v, p, val)

  def Deserialize(self, entity, p, prefix=''):
    # entity <- p; p is a Property message
    # In this class, prefix is unused.
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
      # TODO: What if we don't have the repeated flag set, yet
      # multiple values are read from the datastore?
      value = val
    entity._values[self.name] = value

class IntegerProperty(Property):

  def DbSetValue(self, v, p, value):
    assert isinstance(value, (bool, int, long))
    v.set_int64value(value)

  def DbGetValue(self, v, p):
    if not v.has_int64value():
      return None
    return int(v.int64value())

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
    ref = value._Key__reference  # Don't copy
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

def FixUpProperties(cls):
  # NOTE: This may be called multiple times if properties are
  # dynamically added to the class.
  assert cls is not Model
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

  def __repr__(self):
    s = '%s(%s, db_name=%r, indexed=%r, repeated=%r)' % (
      self.__class__.__name__, self.minimodelclass.__name__,
      self.db_name, self.indexed, self.repeated)
    if self.name != self.db_name:
      s += '<name=%r>' % self.name
    return s

  def Serialize(self, entity, pb, prefix=''):
    # entity -> pb; pb is an EntityProto message
    value = entity._values.get(self.name)
    if value is None:
      # TODO: Is this the right thing for queries?
      # Skip structured values that are None.
      return
    cls = self.minimodelclass
    if cls._properties is None and cls is not Model:
      FixUpProperties(cls)
    if self.repeated:
      assert isinstance(value, list)
      values = value
    else:
      assert isinstance(value, cls)
      values = [value]
    gitems = None
    if cls._properties:
      # TODO: Sort by property declaration order
      gitems = sorted(cls._properties.iteritems())
    for value in values:
      litems = gitems
      if litems is None and value._properties:
        litems = sorted(value._properties.iteritems())
      if litems:
        for name, prop in litems:
          prop.Serialize(value, pb, prefix + self.db_name + '.')

  def Deserialize(self, entity, p, prefix=''):
    db_name = p.name()
    if prefix:
      assert prefix.endswith('.')
    n = prefix.count('.') + 1  # Nesting level
    parts = db_name.split('.')
    assert len(parts) > n, (prefix, db_name, parts, n)
    next = parts[n]
    prop = None
    if self.minimodelclass._db_properties:
      prop = self.minimodelclass._db_properties.get(next)
    if prop is None:
      subentity = entity._values.get(self.name)
      if subentity is None:
        subentity = self.minimodelclass()
        entity._values[self.name] = subentity
      prop = FakeProperty(subentity, p, '.'.join(parts[n:]), next)
    if self.repeated:
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
        assert isinstance(sub, self.minimodelclass)
        if prop.name not in sub._values:
          subentity = sub
          break
      else:
        subentity = self.minimodelclass()
        values.append(subentity)
    else:
      subentity = entity._values.get(self.name)
      if subentity is None:
        subentity = self.minimodelclass()
        entity._values[self.name] = subentity
    prop.Deserialize(subentity, p, prefix + next + '.')

_EPOCH = datetime.datetime.utcfromtimestamp(0)

class GenericProperty(Property):

  def DbGetValue(self, v, p):
    # This is awkward but there seems to be no faster way to inspect
    # what union member is present.  datastore_types.FromPropertyPb(),
    # the undisputed authority, has a series of if-elif blocks.
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
      # TODO: point, user, blobkey, date, time, atom and gdata types
      assert False, type(value)
