"""Alternate MessageProperty implementation.

Run this using 'make x CUSTOM=msgprop'.

This requires copying (or symlinking) protorpc/python/protorpc into
the appengine-ndb-experiment directory (making ndb and the inner
protorpc directory siblings).
"""

import time

from google.appengine.ext import testbed

from protorpc import messages, protojson

import ndb


class MessageProperty(ndb.BlobProperty):
  """Store a protorpc Message object as JSON."""

  def __init__(self, messageclass):
    self._messageclass = messageclass
    super(MessageProperty, self).__init__()

  def _to_base_type(self, value):
    assert isinstance(value, self._messageclass), \
           'expected %s instance' % self._messageclass.__name__
    return protojson.encode_message(value)

  def _from_base_type(self, value):
    assert isinstance(value, str), 'expected bytes string'
    return protojson.decode_message(self._messageclass, value)


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


# Models to store Messages.

class DbNote(ndb.Model):
  note = MessageProperty(Note)
  note_when = ndb.ComputedProperty(lambda ent: ent.note.when)


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
