"""Prototype MessageProperty for ProtoRPC.

Run this using 'make msgprop CUSTOM=msgprop'.

This requires copying (or symlinking) protorpc/python/protorpc into
the appengine-ndb-experiment directory (making ndb and the inner
protorpc directory siblings).
"""

import time

from google.appengine.ext import testbed

from protorpc import messages

from ndb.model import *


def _message_to_model(msg, message_class):
  assert isinstance(msg, message_class), repr(msg)
  exp = Expando()
  for field in message_class.all_fields():
    val = field.__get__(msg, message_class)
    if isinstance(val, messages.Message):
      exp._clone_properties()
      exp._properties[field.name] = prop = MessageProperty(val.__class__)
      prop._fix_up(Expando, field.name)
    elif field.repeated:
      assert isinstance(val, list)
      if val and isinstance(val[0], messages.Message):
        exp._clone_properties()
        exp._properties[field.name] = prop = MessageProperty(val[0].__class__,
                                                             repeated=True)
        prop._fix_up(Expando, field.name)
      newval = []
      for v in val:
        newval.append(v)
      val = newval
    setattr(exp, field.name, val)
  return exp


class MessageProperty(StructuredProperty):

  def __init__(self, message_class, meta_data=None,
               name=None, repeated=False):
    self._message_class = message_class
    self._meta_data = meta_data
    super(MessageProperty, self).__init__(Expando, name, repeated=repeated)

  def _validate(self, value):
    if not isinstance(value, self._message_class):
      raise TypeError('Expected a %s instance, got %r instead' %
                      (self._message_class.__name__, value))

  def _to_bot(self, value):
    """Convert a message_class instance to a synthetic Model instance."""
    return _message_to_model(value, self._message_class)

  def _to_top(self, value):
    assert isinstance(value, Model), repr(value)
    kwds = {}
    for field in self._message_class.all_fields():
      if hasattr(value, field.name):
        kwds[field.name] = getattr(value, field.name)
    return self._message_class(**kwds)


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


class DbNote(Model):
  note = MessageProperty(Note)


class DbNotes(Model):
  danotes = MessageProperty(Notes)


def main():
  tb = testbed.Testbed()
  tb.activate()
  tb.init_datastore_v3_stub()
  tb.init_memcache_stub()

  note1 = Note(text='blah', when=int(time.time()))
  print 'Before:', note1
  ent = DbNote(note=note1)
  ent.put(use_cache=False)
  print 'After:', ent.key.get(use_cache=False)

  print '-'*20

  note2 = Note(text='blooh', when=0)
  notes = Notes(notes=[note1, note2])
  print 'Before:', notes
  ent = DbNotes(danotes=notes)
  print 'Entity:', ent
  print ent._to_pb(set_key=False)
  ent.put(use_cache=False)
  pb = ent._to_pb()
  import pdb; pdb.set_trace()
  ent2 = DbNotes._from_pb(pb)
  print 'After:', ent.key.get(use_cache=False)

  tb.deactivate()


if __name__ == '__main__':
  main()
