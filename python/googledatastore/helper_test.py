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
import tempfile
import unittest


import httplib2
import mox
from oauth2client import client
from oauth2client import gce
import pytz

import googledatastore as datastore
from googledatastore import connection
from googledatastore import helper


class DatastoreHelperTest(unittest.TestCase):

  def testSetKeyPath(self):
    key = datastore.Key()
    helper.add_key_path(key, 'Foo', 1, 'Bar', 'bar')
    self.assertEquals(2, len(key.path_element))
    self.assertEquals('Foo', key.path_element[0].kind)
    self.assertEquals(1, key.path_element[0].id)
    self.assertEquals('Bar', key.path_element[1].kind)
    self.assertEquals('bar', key.path_element[1].name)

  def testIncompleteKey(self):
    key = datastore.Key()
    helper.add_key_path(key, 'Foo')
    self.assertEquals(1, len(key.path_element))
    self.assertEquals('Foo', key.path_element[0].kind)
    self.assertEquals(0, key.path_element[0].id)
    self.assertEquals('', key.path_element[0].name)

  def testInvalidKey(self):
    key = datastore.Key()
    self.assertRaises(TypeError, helper.add_key_path, key, 'Foo', 1.0)

  def testPropertyValues(self):
    blob_key = datastore.Value()
    blob_key.blob_key_value = 'blob-key'
    property_dict = collections.OrderedDict(
        a_string=u'a',
        a_blob='b',
        a_boolean=True,
        a_integer=1,
        a_long=2L,
        a_double=1.0,
        a_timestamp_microseconds=datetime.datetime.now(),
        a_key=datastore.Key(),
        a_entity=datastore.Entity(),
        a_blob_key=blob_key,
        many_integer=[1, 2, 3])
    entity = datastore.Entity()
    helper.add_properties(entity, property_dict)
    d = dict((prop.name, helper.get_value(prop.value))
             for prop in entity.property)
    self.assertDictEqual(d, property_dict)

  def testLongValueNotTruncated(self):
    value = datastore.Value()
    try:
      helper.set_value(value, 1 << 63)
      self.fail('expected ValueError')
    except ValueError:
      pass

  def testAddPropertyValuesBlindlyAdd(self):
    entity = datastore.Entity()
    helper.add_properties(entity, {'a': 1})
    helper.add_properties(entity, {'a': 2})
    self.assertEquals(2, len(entity.property))

  def testEmptyValues(self):
    v = datastore.Value()
    self.assertEquals(None, helper.get_value(v))

  def testSetPropertyOverwrite(self):
    property = datastore.Property()
    helper.set_property(property, 'a', 1, indexed=False)
    helper.set_property(property, 'a', 'a')
    self.assertEquals('a', helper.get_value(property.value))
    self.assertEquals(True, property.value.indexed)

  def testIndexedPropagation_Literal(self):
    value = datastore.Value()

    helper.set_value(value, 'a')
    self.assertEquals(False, value.HasField('indexed'))
    helper.set_value(value, 'a', False)
    self.assertEquals(True, value.HasField('indexed'))
    self.assertEquals(False, value.indexed)
    helper.set_value(value, 'a', True)
    self.assertEquals(False, value.HasField('indexed'))
    self.assertEquals(True, value.indexed)

  def testIndexedPropagation_Value(self):
    value = datastore.Value()
    helper.set_value(value, datastore.Value())
    self.assertEquals(False, value.HasField('indexed'))

    helper.set_value(value, datastore.Value(), False)
    self.assertEquals(True, value.HasField('indexed'))
    self.assertEquals(False, value.indexed)
    helper.set_value(value, copy.deepcopy(value))
    self.assertEquals(True, value.HasField('indexed'))
    self.assertEquals(False, value.indexed)

    helper.set_value(value, datastore.Value(), True)
    self.assertEquals(False, value.HasField('indexed'))
    self.assertEquals(True, value.indexed)
    value.indexed = True
    helper.set_value(value, copy.deepcopy(value))
    self.assertEquals(True, value.HasField('indexed'))
    self.assertEquals(True, value.indexed)

  def testIndexedPropagation_List(self):
    value = datastore.Value()
    helper.set_value(value, ['a'])
    self.assertEquals(False, value.HasField('indexed'))
    self.assertEquals(False, value.list_value[0].HasField('indexed'))

    helper.set_value(value, ['a'], True)
    self.assertEquals(False, value.HasField('indexed'))
    self.assertEquals(False, value.list_value[0].HasField('indexed'))
    self.assertEquals(True, value.list_value[0].indexed)

    helper.set_value(value, ['a'], False)
    self.assertEquals(False, value.HasField('indexed'))
    self.assertEquals(True, value.list_value[0].HasField('indexed'))
    self.assertEquals(False, value.list_value[0].indexed)

  def testSetValueBadType(self):
    value = datastore.Value()
    self.assertRaises(TypeError, helper.set_value, value, 'a', object())
    self.assertRaises(TypeError, helper.set_value, value, object(), None)

  def testSetPropertyIndexed(self):
    property = datastore.Property()
    helper.set_property(property, 'a', 1)
    self.assertEquals(False, property.value.HasField('indexed'))
    helper.set_property(property, 'a', 1, indexed=True)
    self.assertEquals(False, property.value.HasField('indexed'))
    self.assertEquals(True, property.value.indexed)
    helper.set_property(property, 'a', 1, indexed=False)
    self.assertEquals(True, property.value.HasField('indexed'))
    self.assertEquals(False, property.value.indexed)

  def testQuery(self):
    q = datastore.Query()
    helper.set_kind(q, 'Foo')
    self.assertEquals('Foo', q.kind[0].name)
    helper.add_property_orders(q, '-bar', 'foo')
    self.assertEquals(datastore.PropertyOrder.DESCENDING,
                      q.order[0].direction)
    self.assertEquals('bar', q.order[0].property.name)
    self.assertEquals(datastore.PropertyOrder.ASCENDING,
                      q.order[1].direction)
    self.assertEquals('foo', q.order[1].property.name)
    helper.add_projection(q, '__key__', 'bar')
    self.assertEquals('__key__', q.projection[0].property.name)
    self.assertEquals('bar', q.projection[1].property.name)

  def testFilter(self):
    f = datastore.Filter()
    helper.set_composite_filter(
        f,
        datastore.CompositeFilter.AND,
        helper.set_property_filter(datastore.Filter(),
                            'foo', datastore.PropertyFilter.EQUAL, u'bar'),
        helper.set_property_filter(datastore.Filter(),
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

class DatastoreEnvHelperTest(unittest.TestCase):

  def setUp(self):
    self.mox = mox.Mox()
    self.certificate = tempfile.NamedTemporaryFile(delete=False)
    self.certificate.write('not-a-secret-key')
    self.certificate.close()

  def tearDown(self):
    os.unlink(self.certificate.name)
    self.mox.UnsetStubs()
    self.mox.ResetAll()

  def testGetCredentialsFromEnvJwt(self):
    self.mox.StubOutWithMock(os, 'getenv')
    self.mox.StubOutWithMock(client, 'SignedJwtAssertionCredentials')
    credentials = self.mox.CreateMockAnything()
    os.getenv('DATASTORE_SERVICE_ACCOUNT').AndReturn('foo@bar.com')
    os.getenv('DATASTORE_PRIVATE_KEY_FILE').AndReturn(self.certificate.name)
    client.SignedJwtAssertionCredentials('foo@bar.com',
                                         'not-a-secret-key',
                                         connection.SCOPE).AndReturn(
                                             credentials)
    self.mox.ReplayAll()
    self.assertIs(credentials, helper.get_credentials_from_env())
    self.mox.VerifyAll()

  def testGetCredentialsFromEnvCompute(self):
    self.mox.StubOutWithMock(gce, 'AppAssertionCredentials')
    credentials = self.mox.CreateMockAnything()
    gce.AppAssertionCredentials(connection.SCOPE).AndReturn(credentials)
    credentials.authorize(mox.IsA(httplib2.Http))
    credentials.refresh(mox.IsA(httplib2.Http))
    self.mox.ReplayAll()
    self.assertIs(credentials, helper.get_credentials_from_env())
    self.mox.VerifyAll()

  def testGetCredentialsFromEnvLocal(self):
    self.mox.StubOutWithMock(gce, 'AppAssertionCredentials')
    credentials = self.mox.CreateMockAnything()
    gce.AppAssertionCredentials(connection.SCOPE).AndReturn(credentials)
    credentials.authorize(mox.IsA(httplib2.Http))
    credentials.refresh(mox.IsA(httplib2.Http)).AndRaise(
        httplib2.HttpLib2Error())
    self.mox.ReplayAll()
    self.assertIs(None, helper.get_credentials_from_env())
    self.mox.VerifyAll()

  def testGetDatastoreFromEnv(self):
    self.mox.StubOutWithMock(os, 'getenv')
    os.getenv('DATASTORE_DATASET').AndReturn('my-dataset-id')
    self.mox.ReplayAll()
    self.assertEquals('my-dataset-id', helper.get_dataset_from_env())
    self.mox.VerifyAll()

  def testGetDatastoreFromEnvCompute(self):
    self.mox.StubOutWithMock(httplib2, 'Http')
    http = self.mox.CreateMockAnything()
    httplib2.Http().AndReturn(http)
    http.request('http://metadata/computeMetadata/v1/project/project-id',
                 headers={'X-Google-Metadata-Request': 'True'}).AndReturn(
                     (self.mox.CreateMockAnything(), 'my-dataset-id'))
    self.mox.ReplayAll()
    self.assertEquals('my-dataset-id', helper.get_dataset_from_env())
    self.mox.VerifyAll()

  def testGetDatastoreFromEnvNone(self):
    self.mox.StubOutWithMock(httplib2, 'Http')
    http = self.mox.CreateMockAnything()
    httplib2.Http().AndReturn(http)
    http.request('http://metadata/computeMetadata/v1/project/project-id',
                 headers={'X-Google-Metadata-Request': 'True'}).AndRaise(
                     httplib2.HttpLib2Error())
    self.mox.ReplayAll()
    self.assertEquals(None, helper.get_dataset_from_env())
    self.mox.VerifyAll()

  def testDatetimeTimezone(self):
    dt_secs = 10000000
    dt = datetime.datetime.fromtimestamp(dt_secs,
                                         pytz.timezone('US/Pacific'))
    # We should fail if the datetime has a timezone set.
    self.assertRaises(TypeError, helper.to_timestamp_usec, dt)
    dt = dt.astimezone(pytz.utc)
    # Even if the timezone is set to UTC, we should still fail since storing a
    # datetime with UTC will be read from Datastore as a naive datetime.
    self.assertRaises(TypeError, helper.to_timestamp_usec, dt)

    dt = dt.replace(tzinfo=None)
    self.assertEqual(dt_secs * 1000000L, helper.to_timestamp_usec(dt))


if __name__ == '__main__':
  unittest.main()
