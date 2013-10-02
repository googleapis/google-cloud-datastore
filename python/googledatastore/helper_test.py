#
# Copyright 2013 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""googledatastore helper test suite."""

__author__ = 'proppy@google.com (Johan Euphrosine)'

import collections
import copy
import datetime
import unittest


import googledatastore as datastore
from googledatastore.helper import *


class DatastoreHelperTest(unittest.TestCase):
  def testSetKeyPath(self):
    key = datastore.Key()
    add_key_path(key, 'Foo', 1, 'Bar', 'bar')
    self.assertEquals(2, len(key.path_element))
    self.assertEquals('Foo', key.path_element[0].kind)
    self.assertEquals(1, key.path_element[0].id)
    self.assertEquals('Bar', key.path_element[1].kind)
    self.assertEquals('bar', key.path_element[1].name)

  def testIncompleteKey(self):
    key = datastore.Key()
    add_key_path(key, 'Foo')
    self.assertEquals(1, len(key.path_element))
    self.assertEquals('Foo', key.path_element[0].kind)
    self.assertEquals(0, key.path_element[0].id)
    self.assertEquals('', key.path_element[0].name)

  def testInvalidKey(self):
    key = datastore.Key()
    self.assertRaises(TypeError, add_key_path, key, 'Foo', 1.0)

  def testPropertyValues(self):
    blob_key = datastore.Value()
    blob_key.blob_key_value = 'blob-key'
    property_dict = collections.OrderedDict(
        a_string=u'a',
        a_blob='b',
        a_boolean=True,
        a_integer=1,
        a_double=1.0,
        a_timestamp_microseconds=datetime.datetime.now(),
        a_key=datastore.Key(),
        a_entity=datastore.Entity(),
        a_blob_key=blob_key,
        many_integer=[1, 2, 3])
    entity = datastore.Entity()
    add_properties(entity, property_dict)
    d = dict((prop.name, get_value(prop.value))
             for prop in entity.property)
    self.assertDictEqual(d, property_dict)

  def testAddPropertyValuesBlindlyAdd(self):
    entity = datastore.Entity()
    add_properties(entity, {'a': 1})
    add_properties(entity, {'a': 2})
    self.assertEquals(2, len(entity.property))

  def testEmptyValues(self):
    v = datastore.Value()
    self.assertEquals(None, get_value(v))

  def testSetPropertyOverwrite(self):
    property = datastore.Property()
    set_property(property, 'a', 1, indexed=False)
    set_property(property, 'a', 'a')
    self.assertEquals('a', get_value(property.value))
    self.assertEquals(True, property.value.indexed)

  def testIndexedPropagation_Literal(self):
    value = datastore.Value()

    set_value(value, 'a')
    self.assertEquals(False, value.HasField('indexed'))
    set_value(value, 'a', False)
    self.assertEquals(True, value.HasField('indexed'))
    self.assertEquals(False, value.indexed)
    set_value(value, 'a', True)
    self.assertEquals(False, value.HasField('indexed'))
    self.assertEquals(True, value.indexed)

  def testIndexedPropagation_Value(self):
    value = datastore.Value()
    set_value(value, datastore.Value())
    self.assertEquals(False, value.HasField('indexed'))

    set_value(value, datastore.Value(), False)
    self.assertEquals(True, value.HasField('indexed'))
    self.assertEquals(False, value.indexed)
    set_value(value, copy.deepcopy(value))
    self.assertEquals(True, value.HasField('indexed'))
    self.assertEquals(False, value.indexed)

    set_value(value, datastore.Value(), True)
    self.assertEquals(False, value.HasField('indexed'))
    self.assertEquals(True, value.indexed)
    value.indexed = True
    set_value(value, copy.deepcopy(value))
    self.assertEquals(True, value.HasField('indexed'))
    self.assertEquals(True, value.indexed)

  def testIndexedPropagation_List(self):
    value = datastore.Value()
    set_value(value, ['a'])
    self.assertEquals(False, value.HasField('indexed'))
    self.assertEquals(False, value.list_value[0].HasField('indexed'))

    set_value(value, ['a'], True)
    self.assertEquals(False, value.HasField('indexed'))
    self.assertEquals(False, value.list_value[0].HasField('indexed'))
    self.assertEquals(True, value.list_value[0].indexed)

    set_value(value, ['a'], False)
    self.assertEquals(False, value.HasField('indexed'))
    self.assertEquals(True, value.list_value[0].HasField('indexed'))
    self.assertEquals(False, value.list_value[0].indexed)

  def testSetValueBadType(self):
    value = datastore.Value()
    self.assertRaises(TypeError, set_value, value, 'a', object())
    self.assertRaises(TypeError, set_value, value, object(), None)

  def testSetPropertyIndexed(self):
    property = datastore.Property()
    set_property(property, 'a', 1)
    self.assertEquals(False, property.value.HasField('indexed'))
    set_property(property, 'a', 1, indexed=True)
    self.assertEquals(False, property.value.HasField('indexed'))
    self.assertEquals(True, property.value.indexed)
    set_property(property, 'a', 1, indexed=False)
    self.assertEquals(True, property.value.HasField('indexed'))
    self.assertEquals(False, property.value.indexed)

  def testQuery(self):
    q = datastore.Query()
    set_kind(q, 'Foo')
    self.assertEquals('Foo', q.kind[0].name)
    add_property_orders(q, '-bar', 'foo')
    self.assertEquals(datastore.PropertyOrder.DESCENDING,
                      q.order[0].direction)
    self.assertEquals('bar', q.order[0].property.name)
    self.assertEquals(datastore.PropertyOrder.ASCENDING,
                      q.order[1].direction)
    self.assertEquals('foo', q.order[1].property.name)
    add_projection(q, '__key__', 'bar')
    self.assertEquals('__key__', q.projection[0].property.name)
    self.assertEquals('bar', q.projection[1].property.name)

  def testFilter(self):
    f = datastore.Filter()
    set_composite_filter(
        f,
        datastore.CompositeFilter.AND,
        set_property_filter(datastore.Filter(),
                            'foo', datastore.PropertyFilter.EQUAL, u'bar'),
        set_property_filter(datastore.Filter(),
                            'hop', datastore.PropertyFilter.GREATER_THAN, 2.0))
    cf = f.composite_filter
    pf = cf.filter[0].property_filter
    self.assertEquals('foo', pf.property.name)
    self.assertEquals('bar', pf.value.string_value)
    self.assertEquals(datastore.PropertyFilter.EQUAL, pf.operator)
    pf = cf.filter[1].property_filter
    self.assertEquals('hop', pf.property.name)
    self.assertEquals(2.0, pf.value.double_value)
    self.assertEquals(datastore.PropertyFilter.GREATER_THAN, pf.operator)
    self.assertEquals(datastore.CompositeFilter.AND, cf.operator)

if __name__ == '__main__':
  unittest.main()
