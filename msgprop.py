"""Prototype MessageProperty for ProtoRPC.

Run this using 'make x CUSTOM=msgprop'.

This requires copying (or symlinking) protorpc/python/protorpc into
the appengine-ndb-experiment directory (making ndb and the inner
protorpc directory siblings).
"""

import time

from google.appengine.ext import testbed

from protorpc import messages

import ndb


def make_model_class(message_type):
  props = {}
  for field in message_type.all_fields():
    if isinstance(field, messages.MessageField):
      prop = MessageProperty(field.type, field.name, repeated=field.repeated)
    elif isinstance(field, messages.EnumField):
      prop = EnumProperty(field.type, field.name, repeated=field.repeated)
    elif isinstance(field, messages.BytesField):
      prop = ndb.BlobProperty(field.name, repeated=field.repeated)
    else:
      # IntegerField, FloatField, BooleanField, StringField.
      prop = ndb.GenericProperty(field.name, repeated=field.repeated)
    props[field.name] = prop
  return ndb.MetaModel('%s__Model' % message_type.__name__, (ndb.Model,), props)


class EnumProperty(ndb.StringProperty):

  def __init__(self, enum_type, name=None, repeated=False):
    self._enum_type = enum_type
    super(EnumProperty, self).__init__(name, repeated=repeated)

  def _validate(self, value):
    if not isinstance(value, self._enum_type):
      raise TypeError('Expected a %s instance, got %r instead' %
                      (self._enum_type.__name__, value))

  def _to_base_type(self, enum):
    assert isinstance(enum, self._enum_type), repr(enum)
    return enum.name

  def _from_base_type(self, val):
    assert isinstance(val, basestring)
    return self._enum_type(val)


class MessageProperty(ndb.StructuredProperty):

  def __init__(self, message_type, name=None, repeated=False):
    self._message_type = message_type
    modelclass = make_model_class(message_type)
    super(MessageProperty, self).__init__(modelclass, name, repeated=repeated)

  def __repr__(self):
    return '%s(%s, %r, repeated=%r)' % (self.__class__.__name__,
                                        self._message_type.__name__,
                                        self._name, self._repeated)

  def _validate(self, value):
    if not isinstance(value, self._message_type):
      raise TypeError('Expected a %s instance, got %r instead' %
                      (self._message_type.__name__, value))

  def _to_base_type(self, msg):
    """Convert a message_type instance to a modelclass instance."""
    assert isinstance(msg, self._message_type), repr(msg)
    ent = self._modelclass()
    for name in self._modelclass._properties:
      val = getattr(msg, name)
      setattr(ent, name, val)
    return ent

  def _from_base_type(self, ent):
    assert isinstance(ent, self._modelclass), repr(ent)
    msg = self._message_type()
    for name in self._modelclass._properties:
      val = getattr(ent, name)
      setattr(msg, name, val)
    return msg


# Example classes from protorpc/demos/guestbook/server/

class Note(messages.Message):

  text = messages.StringField(1, required=True)
  when = messages.IntegerField(2)


class GetNotesRequest(messages.Message):

  limit = messages.IntegerField(1, default=10)
  on_or_before = messages.IntegerField(2)

  class Order(messages.Enum):
   WHEN = 1
   TEXT = 2
  order = messages.EnumField(Order, 3, default=Order.WHEN)


class Notes(messages.Message):
  notes = messages.MessageField(Note, 1, repeated=True)


class DbNote(ndb.Model):
  note = MessageProperty(Note)


class DbNotes(ndb.Model):
  danotes = MessageProperty(Notes)


def main():
  tb = testbed.Testbed()
  tb.activate()
  tb.init_datastore_v3_stub()
  tb.init_memcache_stub()

  ctx = ndb.get_context()
  ctx.set_cache_policy(False)
  ctx.set_memcache_policy(False)

  print DbNotes.danotes

  note1 = Note(text='blah', when=int(time.time()))
  print 'Before:', note1
  ent = DbNote(note=note1)
  ent.put()
  print 'After:', ent.key.get()

  print '-'*20

  note2 = Note(text=u'blooh\u1234\U00102345blooh', when=0)
  notes = Notes(notes=[note1, note2])
  print 'Before:', notes
  ent = DbNotes(danotes=notes)
  print 'Entity:', ent
  print ent._to_pb(set_key=False)
  ent.put()
  pb = ent._to_pb()
  ent2 = DbNotes._from_pb(pb)
  print 'After:', ent.key.get()

  print '-'*20

  req = GetNotesRequest(on_or_before=42)
  class M(ndb.Model):
    req = MessageProperty(GetNotesRequest)
  m = M(req=req)
  print m
  print m.put().get()

  tb.deactivate()


if __name__ == '__main__':
  main()
