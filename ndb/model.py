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
    logging.info('pb = %s', pb)
    return pb

conn = datastore_rpc.Connection(adapter=ModelAdapter())

class Model(object):
  """a mutable datastore entity.

  TODO: everything
  """

  __slots__ = ['__values', '__key']

  # TODO: Distinguish between purposes: to call FromPb() or setvalue() etc.
  def __init__(self):
    self.__key = None
    self.__values = {}

  def getkind(self):
    return self.__class__.__name__

  def getkey(self):
    return self.__key

  def setkey(self, key):
    if key is not None:
      assert isinstance(key, Key)
      if self.__class__ is not Model:
        assert list(key.pairs())[-1][0] == self.__class__.__name__
    self.__key = key

  def delkey(self):
    self.__key = None

  key = property(getkey, setkey, delkey)

  def getvalue(self, name, default=None):
    return self.__values.get(name, default)

  def setvalue(self, name, value):
    self.__values[name] = value

  def delvalue(self, name):
    if name in self.__values:
      del self.__values[name]

  def propnames(self):
    return self.__values.keys()

  def __hash__(self):
    raise TypeError('Model is not immutable')

  def __eq__(self, other):
    if not isinstance(other, Model):
      return NotImplemented
    if other.__class__ is not self.__class__:
      return False
    # It's okay to use private names -- we're the same class
    if self.__key != other.__key:
      return False
    return self.__values == other.__values

  def __ne__(self, other):
    eq = self.__eq__(other)
    if eq is  NotImplemented:
      return NotImplemented
    return not eq

  def ToPb(self):
    pb = entity_pb.EntityProto()
    key = self.__key
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
    for name, value in sorted(self.__values.iteritems()):
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
    assert not self.__key
    assert not self.__values
    assert isinstance(pb, entity_pb.EntityProto)
    if pb.has_key():
      self.__key = Key(reference=pb.key())
    for pblist in pb.property_list(), pb.raw_property_list():
      for pb in pblist:
        assert not pb.multiple()
        name = pb.name()
        # TODO: utf8 -> unicode?
        assert name not in self.__values  # TODO: support list values
        value = _DeserializeProperty(pb)
        self.__values[name] = value

  @classmethod
  def get(cls, key):
    return conn.get([key])[0]

  def put(self):
    key = conn.put([self])[0]
    if self.__key != key:
      self.__key = key
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
