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
"""googledatastore helper."""

import calendar
import datetime
import logging
import os

import httplib2
from oauth2client import client
from google.cloud.proto.datastore.v1 import entity_pb2
from google.cloud.proto.datastore.v1 import query_pb2

__all__ = [
    'get_credentials_from_env',
    'get_project_endpoint_from_env',
    'add_key_path',
    'add_properties',
    'set_property',
    'set_value',
    'get_value',
    'get_property_dict',
    'set_kind',
    'add_property_orders',
    'add_projection',
    'set_property_filter',
    'set_composite_filter',
    'to_timestamp',
    'from_timestamp',
]

SCOPE = 'https://www.googleapis.com/auth/datastore'
GOOGLEAPIS_HOST = 'datastore.googleapis.com'
GOOGLEAPIS_URL = 'https://%s' % GOOGLEAPIS_HOST
API_VERSION = 'v1'

# Value types for which their proto value is the user value type.
__native_value_types = frozenset(['string_value',
                                  'blob_value',
                                  'boolean_value',
                                  'integer_value',
                                  'double_value',
                                  'key_value',
                                  'entity_value'])

_DATASTORE_PROJECT_ID_ENV = 'DATASTORE_PROJECT_ID'
_DATASTORE_EMULATOR_HOST_ENV = 'DATASTORE_EMULATOR_HOST'
_DATASTORE_SERVICE_ACCOUNT_ENV = 'DATASTORE_SERVICE_ACCOUNT'
_DATASTORE_PRIVATE_KEY_FILE_ENV = 'DATASTORE_PRIVATE_KEY_FILE'
_DATASTORE_URL_OVERRIDE_ENV = '__DATASTORE_URL_OVERRIDE'
_DATASTORE_USE_STUB_CREDENTIAL_FOR_TEST_ENV = (
    '__DATASTORE_USE_STUB_CREDENTIAL_FOR_TEST')
# Deprecated
_DATASTORE_HOST_ENV = 'DATASTORE_HOST'


def get_credentials_from_env():
  """Get credentials from environment variables.

  Preference of credentials is:
  - No credentials if DATASTORE_EMULATOR_HOST is set.
  - Google APIs Signed JWT credentials based on
  DATASTORE_SERVICE_ACCOUNT and DATASTORE_PRIVATE_KEY_FILE
  environments variables
  - Google Application Default
  https://developers.google.com/identity/protocols/application-default-credentials

  Returns:
    credentials or None.

  """
  if os.getenv(_DATASTORE_USE_STUB_CREDENTIAL_FOR_TEST_ENV):
    logging.info('connecting without credentials because %s is set.',
                 _DATASTORE_USE_STUB_CREDENTIAL_FOR_TEST_ENV)
    return None
  if os.getenv(_DATASTORE_EMULATOR_HOST_ENV):
    logging.info('connecting without credentials because %s is set.',
                 _DATASTORE_EMULATOR_HOST_ENV)
    return None
  if (os.getenv(_DATASTORE_SERVICE_ACCOUNT_ENV)
      and os.getenv(_DATASTORE_PRIVATE_KEY_FILE_ENV)):
    with open(os.getenv(_DATASTORE_PRIVATE_KEY_FILE_ENV), 'rb') as f:
      key = f.read()
    credentials = client.SignedJwtAssertionCredentials(
        os.getenv(_DATASTORE_SERVICE_ACCOUNT_ENV), key, SCOPE)
    logging.info('connecting using private key file.')
    return credentials
  try:
    credentials = client.GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(SCOPE)
    logging.info('connecting using Google Application Default Credentials.')
    return credentials
  except client.ApplicationDefaultCredentialsError, e:
    logging.error('Unable to find any credentials to use. '
                  'If you are running locally, make sure to set the '
                  '%s environment variable.', _DATASTORE_EMULATOR_HOST_ENV)
    raise e


def get_project_endpoint_from_env(project_id=None, host=None):
  """Get Datastore project endpoint from environment variables.

  Args:
    project_id: The Cloud project, defaults to the environment
        variable DATASTORE_PROJECT_ID.
    host: The Cloud Datastore API host to use.

  Returns:
    the endpoint to use, for example
    https://datastore.googleapis.com/v1/projects/my-project

  Raises:
    ValueError: if the wrong environment variable was set or a project_id was
        not provided.
  """
  project_id = project_id or os.getenv(_DATASTORE_PROJECT_ID_ENV)
  if not project_id:
    raise ValueError('project_id was not provided. Either pass it in '
                     'directly or set DATASTORE_PROJECT_ID.')
  # DATASTORE_HOST is deprecated.
  if os.getenv(_DATASTORE_HOST_ENV):
    logging.warning('Ignoring value of environment variable DATASTORE_HOST. '
                    'To point datastore to a host running locally, use the '
                    'environment variable DATASTORE_EMULATOR_HOST')

  url_override = os.getenv(_DATASTORE_URL_OVERRIDE_ENV)
  if url_override:
    return '%s/projects/%s' % (url_override, project_id)

  localhost = os.getenv(_DATASTORE_EMULATOR_HOST_ENV)
  if localhost:
    return ('http://%s/%s/projects/%s'
            % (localhost, API_VERSION, project_id))

  host = host or GOOGLEAPIS_HOST
  return 'https://%s/%s/projects/%s' % (host, API_VERSION, project_id)


def add_key_path(key_proto, *path_elements):
  """Add path elements to the given datastore.Key proto message.

  Args:
    key_proto: datastore.Key proto message.
    *path_elements: list of ancestors to add to the key.
        (kind1, id1/name1, ..., kindN, idN/nameN), the last 2 elements
        represent the entity key, if no terminating id/name: they key
        will be an incomplete key.

  Raises:
    TypeError: the given id or name has the wrong type.

  Returns:
    the same datastore.Key.

  Usage:
    >>> add_key_path(key_proto, 'Kind', 'name')  # no parent, with name
    datastore.Key(...)
    >>> add_key_path(key_proto, 'Kind2', 1)  # no parent, with id
    datastore.Key(...)
    >>> add_key_path(key_proto, 'Kind', 'name', 'Kind2', 1)  # parent, complete
    datastore.Key(...)
    >>> add_key_path(key_proto, 'Kind', 'name', 'Kind2')  # parent, incomplete
    datastore.Key(...)
  """
  for i in range(0, len(path_elements), 2):
    pair = path_elements[i:i+2]
    elem = key_proto.path.add()
    elem.kind = pair[0]
    if len(pair) == 1:
      return  # incomplete key
    id_or_name = pair[1]
    if isinstance(id_or_name, (int, long)):
      elem.id = id_or_name
    elif isinstance(id_or_name, basestring):
      elem.name = id_or_name
    else:
      raise TypeError(
          'Expected an integer id or string name as argument %d; '
          'received %r (a %s).' % (i + 2, id_or_name, type(id_or_name)))
  return key_proto


def add_properties(entity_proto, property_dict, exclude_from_indexes=None):
  """Add values to the given datastore.Entity proto message.

  Args:
    entity_proto: datastore.Entity proto message.
    property_dict: a dictionary from property name to either a python object or
        datastore.Value.
    exclude_from_indexes: if the value should be exclude from indexes. None
        leaves indexing as is (defaults to False if value is not a Value
        message).

  Usage:
    >>> add_properties(proto, {'foo': u'a', 'bar': [1, 2]})

  Raises:
    TypeError: if a given property value type is not supported.
  """
  for name, value in property_dict.iteritems():
    set_property(entity_proto.properties, name, value, exclude_from_indexes)


def set_property(property_map, name, value, exclude_from_indexes=None):
  """Set property value in the given datastore.Property proto message.

  Args:
    property_map: a string->datastore.Value protobuf map.
    name: name of the property.
    value: python object or datastore.Value.
    exclude_from_indexes: if the value should be exclude from indexes. None
        leaves indexing as is (defaults to False if value is not a Value message).

  Usage:
    >>> set_property(property_proto, 'foo', u'a')

  Raises:
    TypeError: if the given value type is not supported.
  """
  set_value(property_map[name], value, exclude_from_indexes)


def set_value(value_proto, value, exclude_from_indexes=None):
  """Set the corresponding datastore.Value _value field for the given arg.

  Args:
    value_proto: datastore.Value proto message.
    value: python object or datastore.Value. (unicode value will set a
        datastore string value, str value will set a blob string value).
        Undefined behavior if value is/contains value_proto.
    exclude_from_indexes: if the value should be exclude from indexes. None
        leaves indexing as is (defaults to False if value is not a Value
        message).

  Raises:
    TypeError: if the given value type is not supported.
  """
  value_proto.Clear()

  if isinstance(value, (list, tuple)):
    for sub_value in value:
      set_value(value_proto.array_value.values.add(), sub_value,
                exclude_from_indexes)
    return  # do not set indexed for a list property.

  if isinstance(value, entity_pb2.Value):
    value_proto.MergeFrom(value)
  elif isinstance(value, unicode):
    value_proto.string_value = value
  elif isinstance(value, str):
    value_proto.blob_value = value
  elif isinstance(value, bool):
    value_proto.boolean_value = value
  elif isinstance(value, (int, long)):
    value_proto.integer_value = value
  elif isinstance(value, float):
    value_proto.double_value = value
  elif isinstance(value, datetime.datetime):
    to_timestamp(value, value_proto.timestamp_value)
  elif isinstance(value, entity_pb2.Key):
    value_proto.key_value.CopyFrom(value)
  elif isinstance(value, entity_pb2.Entity):
    value_proto.entity_value.CopyFrom(value)
  else:
    raise TypeError('value type: %r not supported' % (value,))

  if exclude_from_indexes is not None:
    value_proto.exclude_from_indexes = exclude_from_indexes


def get_value(value_proto):
  """Gets the python object equivalent for the given value proto.

  Args:
    value_proto: datastore.Value proto message.

  Returns:
    the corresponding python object value. timestamps are converted to
    datetime, and datastore.Value is returned for blob_key_value.
  """
  field = value_proto.WhichOneof('value_type')
  if field in __native_value_types:
      return getattr(value_proto, field)
  if field == 'timestamp_value':
    return from_timestamp(value_proto.timestamp_value)
  if field == 'array_value':
    return [get_value(sub_value)
            for sub_value in value_proto.array_value.values]
  return None


def get_property_dict(entity_proto):
  """Convert datastore.Entity to a dict of property name -> datastore.Value.

  Args:
    entity_proto: datastore.Entity proto message.

  Usage:
    >>> get_property_dict(entity_proto)
    {'foo': {string_value='a'}, 'bar': {integer_value=2}}

  Returns:
    dict of entity properties.
  """
  return dict((p.key, p.value) for p in entity_proto.property)


def set_kind(query_proto, kind):
  """Set the kind constraint for the given datastore.Query proto message."""
  del query_proto.kind[:]
  query_proto.kind.add().name = kind


def add_property_orders(query_proto, *orders):
  """Add ordering constraint for the given datastore.Query proto message.

  Args:
    query_proto: datastore.Query proto message.
    orders: list of propertype name string, default to ascending
    order and set descending if prefixed by '-'.

  Usage:
    >>> add_property_orders(query_proto, 'foo')  # sort by foo asc
    >>> add_property_orders(query_proto, '-bar')  # sort by bar desc
  """
  for order in orders:
    proto = query_proto.order.add()
    if order[0] == '-':
      order = order[1:]
      proto.direction = query_pb2.PropertyOrder.DESCENDING
    else:
      proto.direction = query_pb2.PropertyOrder.ASCENDING
    proto.property.name = order


def add_projection(query_proto, *projection):
  """Add projection properties to the given datatstore.Query proto message."""
  for p in projection:
    proto = query_proto.projection.add()
    proto.property.name = p


def set_property_filter(filter_proto, name, op, value):
  """Set property filter contraint in the given datastore.Filter proto message.

  Args:
    filter_proto: datastore.Filter proto message
    name: property name
    op: datastore.PropertyFilter.Operation
    value: property value

  Returns:
    the same datastore.Filter.

  Usage:
    >>> set_property_filter(filter_proto, 'foo',
    ...   datastore.PropertyFilter.EQUAL, 'a')  # WHERE 'foo' = 'a'
  """
  filter_proto.Clear()
  pf = filter_proto.property_filter
  pf.property.name = name
  pf.op = op
  set_value(pf.value, value)
  return filter_proto


def set_composite_filter(filter_proto, op, *filters):
  """Set composite filter contraint in the given datastore.Filter proto message.

  Args:
    filter_proto: datastore.Filter proto message
    op: datastore.CompositeFilter.Operation
    filters: vararg list of datastore.Filter

  Returns:
   the same datastore.Filter.

  Usage:
    >>> set_composite_filter(filter_proto, datastore.CompositeFilter.AND,
    ...   set_property_filter(datastore.Filter(), ...),
    ...   set_property_filter(datastore.Filter(), ...)) # WHERE ... AND ...
  """
  filter_proto.Clear()
  cf = filter_proto.composite_filter
  cf.op = op
  for f in filters:
    cf.filters.add().CopyFrom(f)
  return filter_proto


_EPOCH = datetime.datetime.utcfromtimestamp(0)
_MICROS_PER_SECOND = 1000000L
_NANOS_PER_MICRO = 1000L


def micros_from_timestamp(timestamp):
  """Convert protobuf Timestamp to microseconds from utc epoch."""
  return (timestamp.seconds * _MICROS_PER_SECOND
          + int(timestamp.nanos / _NANOS_PER_MICRO))


def from_timestamp(timestamp):
  """Convert a protobuf Timestamp to datetime."""
  return _EPOCH + datetime.timedelta(
      microseconds=micros_from_timestamp(timestamp))


def micros_to_timestamp(micros, timestamp):
  """Convert microseconds from utc epoch to google.protobuf.timestamp.

  Args:
    micros: a long, number of microseconds since utc epoch.
    timestamp: a google.protobuf.timestamp.Timestamp to populate.
  """
  seconds = long(micros / _MICROS_PER_SECOND)
  micro_remainder = micros % _MICROS_PER_SECOND
  timestamp.seconds = seconds
  timestamp.nanos = micro_remainder * _NANOS_PER_MICRO


def to_timestamp(dt, timestamp):
  """Convert datetime to google.protobuf.Timestamp.

  Args:
    dt: a timezone naive datetime.
    timestamp: a google.protobuf.Timestamp to populate.

  Raises:
    TypeError: if a timezone aware datetime was provided.
  """
  if dt.tzinfo:
    # this is an "aware" datetime with an explicit timezone. Throw an error.
    raise TypeError('Cannot store a timezone aware datetime. '
                    'Convert to UTC and store the naive datetime.')
  timestamp.seconds = calendar.timegm(dt.timetuple())
  timestamp.nanos = dt.microsecond * _NANOS_PER_MICRO
