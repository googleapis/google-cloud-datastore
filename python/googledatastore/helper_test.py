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
import os
import unittest

import mox
import pytz

import googledatastore as datastore
from googledatastore.helper import *
from google.protobuf.timestamp_pb2 import Timestamp


class DatastoreHelperTest(unittest.TestCase):

  def setUp(self):
    self.mox = mox.Mox()

  def tearDown(self):
    self.mox.UnsetStubs()
    self.mox.ResetAll()

  def testSetKeyPath(self):
    key = datastore.Key()
    add_key_path(key, 'Foo', 1, 'Bar', 'bar')
    self.assertEquals(2, len(key.path))
    self.assertEquals('Foo', key.path[0].kind)
    self.assertEquals(1, key.path[0].id)
    self.assertEquals('Bar', key.path[1].kind)
    self.assertEquals('bar', key.path[1].name)

  def testIncompleteKey(self):
    key = datastore.Key()
    add_key_path(key, 'Foo')
    self.assertEquals(1, len(key.path))
    self.assertEquals('Foo', key.path[0].kind)
    self.assertEquals(0, key.path[0].id)
    self.assertEquals('', key.path[0].name)

  def testInvalidKey(self):
    key = datastore.Key()
    self.assertRaises(TypeError, add_key_path, key, 'Foo', 1.0)

  def testPropertyValues(self):
    property_dict = collections.OrderedDict(
        a_string=u'a',
        a_blob='b',
        a_boolean=True,
        a_integer=1L,
        a_double=1.0,
        a_timestamp_microseconds=datetime.datetime.now(),
        a_key=datastore.Key(),
        a_entity=datastore.Entity(),
        many_integer=[1L, 2L, 3L])
    entity = datastore.Entity()
    add_properties(entity, property_dict)
    d = dict((key, get_value(value))
             for key, value in entity.properties.items())
    self.maxDiff = None
    self.assertDictEqual(d, property_dict)

  def testEmptyValues(self):
    v = datastore.Value()
    self.assertEquals(None, get_value(v))

  def testSetPropertyOverwrite(self):
    entity = datastore.Entity()
    set_property(entity.properties, 'a', 1, exclude_from_indexes=True)
    set_property(entity.properties, 'a', 'a')
    self.assertEquals('a', get_value(entity.properties['a']))
    self.assertEquals(False, entity.properties['a'].exclude_from_indexes)

  def testIndexedPropagation_Literal(self):
    value = datastore.Value()

    set_value(value, 'a', True)
    self.assertEquals(True, value.exclude_from_indexes)
    set_value(value, 'a', False)
    self.assertEquals(False, value.exclude_from_indexes)

  def testIndexedPropagation_Value(self):
    value = datastore.Value()

    set_value(value, datastore.Value(), True)
    self.assertEquals(True, value.exclude_from_indexes)
    set_value(value, copy.deepcopy(value))
    self.assertEquals(True, value.exclude_from_indexes)
    set_value(value, datastore.Value(), False)
    self.assertEquals(False, value.exclude_from_indexes)
    value.exclude_from_indexes = False
    set_value(value, copy.deepcopy(value))
    self.assertEquals(False, value.exclude_from_indexes)

  def testIndexedPropagation_List(self):
    value = datastore.Value()
    set_value(value, ['a'])
    self.assertEquals(False, value.exclude_from_indexes)
    self.assertEquals(False, value.array_value.values[0].exclude_from_indexes)

    set_value(value, ['a'], True)
    self.assertEquals(False, value.exclude_from_indexes)
    self.assertEquals(True, value.array_value.values[0].exclude_from_indexes)

    set_value(value, ['a'], False)
    self.assertEquals(False, value.exclude_from_indexes)
    self.assertEquals(False, value.array_value.values[0].exclude_from_indexes)

  def testSetValueBadType(self):
    value = datastore.Value()
    self.assertRaises(TypeError, set_value, value, 'a', object())
    self.assertRaises(TypeError, set_value, value, object(), None)

  def testSetPropertyIndexed(self):
    entity = datastore.Entity()
    set_property(entity.properties, 'a', 1)
    self.assertEquals(False, entity.properties['a'].exclude_from_indexes)
    set_property(entity.properties, 'a', 1, exclude_from_indexes=True)
    self.assertEquals(True, entity.properties['a'].exclude_from_indexes)

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
    pf = cf.filters[0].property_filter
    self.assertEquals('foo', pf.property.name)
    self.assertEquals('bar', pf.value.string_value)
    self.assertEquals(datastore.PropertyFilter.EQUAL, pf.op)
    pf = cf.filters[1].property_filter
    self.assertEquals('hop', pf.property.name)
    self.assertEquals(2.0, pf.value.double_value)
    self.assertEquals(datastore.PropertyFilter.GREATER_THAN, pf.op)
    self.assertEquals(datastore.CompositeFilter.AND, cf.op)

  def testDatetimeTimezone(self):
    dt_secs = 10000000L
    dt = datetime.datetime.fromtimestamp(dt_secs,
                                         pytz.timezone('US/Pacific'))
    # We should fail if the datetime has a timezone set.
    ts = Timestamp()
    self.assertRaises(TypeError, to_timestamp, dt, ts)
    dt = dt.astimezone(pytz.utc)
    # Even if the timezone is set to UTC, we should still fail since storing a
    # datetime with UTC will be read from Datastore as a naive datetime.
    self.assertRaises(TypeError, to_timestamp, dt, ts)

    dt = dt.replace(tzinfo=None)
    to_timestamp(dt, ts)
    self.assertEqual(dt_secs, ts.seconds)
    self.assertEqual(0, ts.nanos)

  def testEndpointWithHost(self):
    self.mox.StubOutWithMock(os, 'getenv')
    os.getenv('DATASTORE_HOST').AndReturn('ignored')
    os.getenv('__DATASTORE_URL_OVERRIDE').AndReturn(None)
    os.getenv('DATASTORE_EMULATOR_HOST').AndReturn(None)
    self.mox.ReplayAll()
    endpoint = get_project_endpoint_from_env(project_id='bar',
                                             host='a.b.c')
    self.assertEqual('https://a.b.c/v1/projects/bar',
                     endpoint)
    self.mox.VerifyAll()

  def testEndpointWithEmulatorHostAndHost(self):
    self.mox.StubOutWithMock(os, 'getenv')
    os.getenv('DATASTORE_HOST').AndReturn('ignored')
    os.getenv('__DATASTORE_URL_OVERRIDE').AndReturn(None)
    os.getenv('DATASTORE_EMULATOR_HOST').AndReturn('localhost:1234')
    self.mox.ReplayAll()
    endpoint = get_project_endpoint_from_env(project_id='bar')
    self.assertEqual('http://localhost:1234/v1/projects/bar',
                     endpoint)
    self.mox.VerifyAll()

  def testEndpointWithEmulatorHost(self):
    self.mox.StubOutWithMock(os, 'getenv')
    os.getenv('DATASTORE_HOST').AndReturn('ignored')
    os.getenv('__DATASTORE_URL_OVERRIDE').AndReturn(None)
    os.getenv('DATASTORE_EMULATOR_HOST').AndReturn('localhost:1234')
    self.mox.ReplayAll()
    endpoint = get_project_endpoint_from_env(project_id='bar',
                                             host='a.b.c')
    # DATASTORE_EMULATOR_HOST wins.
    self.assertEqual('http://localhost:1234/v1/projects/bar',
                     endpoint)
    self.mox.VerifyAll()

  def testEndpointWithEmulatorHostAndProject(self):
    self.mox.StubOutWithMock(os, 'getenv')
    os.getenv('DATASTORE_PROJECT_ID').AndReturn('bar')
    os.getenv('DATASTORE_HOST').AndReturn('ignored')
    os.getenv('__DATASTORE_URL_OVERRIDE').AndReturn(None)
    os.getenv('DATASTORE_EMULATOR_HOST').AndReturn('localhost:1234')
    self.mox.ReplayAll()
    endpoint = get_project_endpoint_from_env()
    self.assertEqual('http://localhost:1234/v1/projects/bar',
                     endpoint)
    self.mox.VerifyAll()

  def testEndpointWithProject(self):
    self.mox.StubOutWithMock(os, 'getenv')
    os.getenv('DATASTORE_PROJECT_ID').AndReturn('bar')
    os.getenv('DATASTORE_HOST').AndReturn('ignored')
    os.getenv('__DATASTORE_URL_OVERRIDE').AndReturn(None)
    os.getenv('DATASTORE_EMULATOR_HOST').AndReturn(None)
    self.mox.ReplayAll()
    endpoint = get_project_endpoint_from_env()
    self.assertEqual('https://datastore.googleapis.com/v1/projects/bar',
                     endpoint)
    self.mox.VerifyAll()

  def testEndpointWithNoProjectId(self):
    self.assertRaisesRegexp(
        ValueError,
        'project_id was not provided.*',
        get_project_endpoint_from_env)

  def testEndpointWithUrlOverride(self):
    self.mox.StubOutWithMock(os, 'getenv')
    os.getenv('DATASTORE_HOST').AndReturn('ignored')
    os.getenv('__DATASTORE_URL_OVERRIDE').AndReturn(
        'http://prom-qa/datastore/v1beta42')
    self.mox.ReplayAll()
    endpoint = get_project_endpoint_from_env(project_id='bar')
    self.assertEqual('http://prom-qa/datastore/v1beta42/projects/bar',
                     endpoint)
    self.mox.VerifyAll()


if __name__ == '__main__':
  unittest.main()
