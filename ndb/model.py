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
  @datastore_rpc._positional(1)
  def __init__(self, key=None, **kwds):
    cls = self.__class__
    if (cls is not Model and
        (cls._properties is None or cls._db_properties is None)):
      FixUpProperties(cls)
    self._key = key
    self._values = {}
    for name, value in kwds.iteritems():
      prop = getattr(cls, name)
      assert isinstance(prop, Property)
      prop.SetValue(self, value)

  def __repr__(self):
    args = []
    if self._key is not None:
      args.append('key=%r' % self._key)
    for name_value in sorted(self._values.iteritems()):
      args.append('%s=%r' % name_value)
    s = '%s(%s)' % (self.__class__.__name__, ', '.join(args))
    return s

  # TODO: Make kind a property also?
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

    # TODO: Move the key stuff into ModelAdapter.entity_to_pb()?
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
      for name, prop in sorted(self._db_properties.iteritems()):
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
    db_name = p.name()
    parts = db_name.split('.')
    if len(parts) <= depth:
      import pdb; pdb.set_trace()
    assert len(parts) > depth, (p.name(), parts, depth)
    next = parts[depth]
    prop = None
    if self._db_properties:
      prop = self._db_properties.get(next)
    if prop is None:
      prop = self.FakeProperty(p, next, indexed)
    return prop

  def FakeProperty(self, p, next, indexed=True):
    cls = self.__class__
    if self._db_properties is cls._db_properties:
      self._db_properties = dict(cls._db_properties or ())
    if self._properties is cls._properties:
      self._properties = dict(cls._properties or ())

    if p.name() != next and not p.name().endswith('.' + next):
      prop = StructuredProperty(Model, next)
      pid = str(id(prop))
      assert pid not in self._values
      self._values[pid] = Model()
    else:
      prop = GenericProperty(next,
                             repeated=p.multiple(),
                             indexed=indexed)

    prop.FixUp(str(id(prop)))  # Use a unique string as Python name.

    self._db_properties[prop.db_name] = prop
    self._properties[prop.name] = prop
    return prop

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

# TODO: Use a metaclass to automatically call FixUpProperties()?
# TODO: More Property types
# TODO: Orphan properties

class Property(object):
  # TODO: Separate 'simple' properties from base Property class

  db_name = None
  name = None
  indexed = True
  repeated = False

  _attributes = ['db_name', 'name', 'indexed', 'repeated']
  _positional = 1

  @datastore_rpc._positional(1 + _positional)
  def __init__(self, db_name=None, name=None, indexed=None, repeated=None):
    if db_name is not None:
      assert '.' not in db_name  # The '.' is used elsewhere.
      self.db_name = db_name
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

  def Serialize(self, entity, pb, prefix=''):
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
      p.set_name(prefix + self.db_name)
      p.set_multiple(self.repeated)
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

  modelclass = None

  _attributes = ['modelclass'] + Property._attributes
  _positional = 2

  @datastore_rpc._positional(1 + _positional)
  def __init__(self, modelclass, db_name=None, name=None,
               indexed=None, repeated=None):
    super(StructuredProperty, self).__init__(db_name=db_name,
                                             name=name,
                                             indexed=indexed,
                                             repeated=repeated)
    if (modelclass is not Model and
      (modelclass._properties is None or modelclass._db_properties is None)):
      FixUpProperties(modelclass)
    if self.repeated:
      assert not modelclass._has_repeated
    self.modelclass = modelclass

  def Serialize(self, entity, pb, prefix=''):
    # entity -> pb; pb is an EntityProto message
    value = entity._values.get(self.name)
    if value is None:
      # TODO: Is this the right thing for queries?
      # Skip structured values that are None.
      return
    cls = self.modelclass
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
      gitems = sorted(cls._db_properties.iteritems())
    for value in values:
      litems = gitems
      if litems is None and value._properties:
        litems = sorted(value._db_properties.iteritems())
      if litems:
        for name, prop in litems:
          prop.Serialize(value, pb, prefix + self.db_name + '.')

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
    # TODO: Prove this won't happen for orphans.
    db_name = p.name()
    parts = db_name.split('.')
    assert len(parts) > depth, (depth, db_name, parts)
    next = parts[depth]
    prop = None
    if self.modelclass._db_properties:
      prop = self.modelclass._db_properties.get(next)
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
  # for properties with dynalically-typed values, and in Expandos.

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
