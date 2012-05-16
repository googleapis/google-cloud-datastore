"""Tests for msgprop.py."""

import unittest

from protorpc import messages

from . import model
from . import msgprop
from . import test_utils


SAMPLE_PB = r"""key <
  app: "_"
  path <
    Element {
      type: "Storage"
      id: 1
    }
  >
>
entity_group <
  Element {
    type: "Storage"
    id: 1
  }
>
property <
  name: "greet.text"
  value <
    stringValue: "abc"
  >
  multiple: false
>
raw_property <
  meaning: 14
  name: "greet.__protojson__"
  value <
    stringValue: "{\"text\": \"abc\", \"when\": 123}"
  >
  multiple: false
>
"""


class MessagePropertyTests(test_utils.NDBTest):

  def setUp(self):
    super(MessagePropertyTests, self).setUp()
    global Greeting, Storage
    class Greeting(messages.Message):
      text = messages.StringField(1, required=True)
      when = messages.IntegerField(2)
    class Storage(model.Model):
      greet = msgprop.MessageProperty(Greeting, indexed_fields=['text'])

  def testBasics(self):
    greet = Greeting(text='abc', when=123)
    store = Storage(greet=greet)
    key = store.put()
    result = key.get()
    self.assertFalse(result is store)
    self.assertEqual(result.greet.text, 'abc')
    self.assertEqual(result.greet.when, 123)
    self.assertEqual(result.greet, Greeting(when=123, text='abc'))
    self.assertEqual(result,
                     Storage(greet=Greeting(when=123, text='abc'), key=key))
    self.assertEqual(str(result._to_pb()), SAMPLE_PB)

  def testQuery(self):
    greet1 = Greeting(text='abc', when=123)
    store1 = Storage(greet=greet1)
    store1.put()
    greet2 = Greeting(text='def', when=456)
    store2 = Storage(greet=greet2)
    store2.put()
    q = Storage.query(Storage.greet.text == 'abc')
    self.assertEqual(q.fetch(), [store1])
    self.assertRaises(AttributeError, lambda: Storage.greet.when)

  def testErrors(self):
    # Call MessageProperty(x) where x is not a Message class.
    self.assertRaises(TypeError, msgprop.MessageProperty, Storage)
    self.assertRaises(TypeError, msgprop.MessageProperty, 42)
    self.assertRaises(TypeError, msgprop.MessageProperty, None)

    # Call MessageProperty(Greeting, indexed_fields=x) where x
    # includes invalid field names.
    self.assertRaises(ValueError, msgprop.MessageProperty,
                      Greeting, indexed_fields=['text', 'nope'])
    self.assertRaises(ValueError, msgprop.MessageProperty,
                      Greeting, indexed_fields=['text', 42])
    self.assertRaises(ValueError, msgprop.MessageProperty,
                      Greeting, indexed_fields=['text', None])

    # Set a MessageProperty value to a non-Message instance.
    self.assertRaises(TypeError, Storage, greet=42)

  def testNothingIndexed(self):
    class Store(model.Model):
      gr = msgprop.MessageProperty(Greeting)
    gr = Greeting(text='abc', when=123)
    st = Store(gr=gr)
    st.put()
    self.assertEqual(Store.query().fetch(), [st])
    self.assertRaises(AttributeError, lambda: Store.gr.when)

  def testForceProtocol(self):
    class Store(model.Model):
      gr = msgprop.MessageProperty(Greeting, protocol='protobuf')
    gr = Greeting(text='abc', when=123)
    st = Store(gr=gr)
    st.put()
    self.assertEqual(Store.query().fetch(), [st])

  def testRepeatedMessageProperty(self):
    class StoreSeveral(model.Model):
      greets = msgprop.MessageProperty(Greeting, repeated=True,
                                       # Duplicate field name should be no-op.
                                       indexed_fields=['text', 'when', 'text'])
    ga = Greeting(text='abc', when=123)
    gb = Greeting(text='abc', when=456)
    gc = Greeting(text='def', when=123)
    gd = Greeting(text='def', when=456)
    s1 = StoreSeveral(greets=[ga, gb])
    k1 = s1.put()
    s2 = StoreSeveral(greets=[gc, gd])
    k2 = s2.put()
    res1 = k1.get()
    self.assertEqual(res1, s1)
    self.assertFalse(res1 is s1)
    self.assertEqual(res1.greets, [ga, gb])
    res = StoreSeveral.query(StoreSeveral.greets.text == 'abc').fetch()
    self.assertEqual(res, [s1])
    res = StoreSeveral.query(StoreSeveral.greets.when == 123).fetch()
    self.assertEqual(res, [s1, s2])

  # TODO:
  # - MessageProperty for Message class with repeated field, indexed
  # - Enums (string or int?)
  # - nested Message and index nested fields, possibly repeated


def main():
  unittest.main()


if __name__ == '__main__':
  main()
