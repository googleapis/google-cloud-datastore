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
from oauth2client import gce
from googledatastore import connection
from googledatastore.connection import datastore_v1_pb2

__all__ = [
    'get_credentials_from_env',
    'add_key_path',
    'add_property_values',
    'set_property',
    'set_value',
    'get_value',
    'get_property_value',
    'get_property_values',
    'set_kind',
    'add_property_orders',
    'add_projection',
    'set_property_filter',
    'set_composite_filter',
    'to_timestamp_usec',
    'from_timestamp_usec',
]


def get_credentials_from_env():
  """Get datastore credentials from DATASTORE_* environment variables.

  Try and fallback on the following credentials in that order:
  - Compute Engine service account
  - Google APIs Signed JWT credentials based on
  DATASTORE_SERVICE_ACCOUNT and DATASTORE_PRIVATE_KEY_FILE
  environments variables
  - No credentials (development server)

  Returns:
    datastore credentials.

  """
  try:
    # Use Compute Engine credentials to connect to the datastore service. Note
    # that the corresponding service account should be an admin of the
    # datastore application.
    credentials = gce.AppAssertionCredentials(connection.SCOPE)
    http = httplib2.Http()
    credentials.authorize(http)
    # force first credentials refresh to detect if we are running on
    # Compute Engine.
    credentials.refresh(http)
    logging.info('connect using compute credentials')
    return credentials
  except (client.AccessTokenRefreshError, httplib2.HttpLib2Error):
    # If not running on Google Compute fallback on using Google APIs
    # Console Service Accounts (signed JWT). Note that the corresponding
    # service account should be an admin of the datastore application.
    if (os.getenv('DATASTORE_SERVICE_ACCOUNT')
        and os.getenv('DATASTORE_PRIVATE_KEY_FILE')):
      with open(os.getenv('DATASTORE_PRIVATE_KEY_FILE'), 'rb') as f:
        key = f.read()
      credentials = client.SignedJwtAssertionCredentials(
          os.getenv('DATASTORE_SERVICE_ACCOUNT'), key, connection.SCOPE)
      logging.info('connect using DatastoreSignedJwtCredentials')
      return credentials
  # Fallback on no credentials if no DATASTORE_ environments variables
  # are defined. Note that it will only authorize call to the
  # development server.
  logging.info('connect using no credentials')
  return None


def add_key_path(key_proto, *path_elements):
  """Add path elements to the given datastore.Key proto message.

  Args:
    key_proto: datastore.Key proto message.
    path_elements: list of ancestors to add to the key.
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
    elem = key_proto.path_element.add()
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


def add_property_values(entity_proto, **kwargs):
  """Add values to the given datastore.Entity proto message.

  Args:
    entity_proto: datastore.Entity proto message.
    kwargs: keywords property_name=value arguments.

  Note:
    - indexing/meaning preferences are not set by this function.

  Usage:
    >>> add_property_values(proto, foo="a", bar=2)
  """
  for name, value in kwargs.iteritems():
    set_property(entity_proto.property.add(), name, value)


def set_property(property_proto, name, value, indexed=True):
  """Set property value in the given datastore.Property proto message.

  Args:
    property_proto: datastore.Property proto message.
    name: name of the property.
    value: python object or datastore.Value.
    indexed: if the property should be indexed or not.

  Usage:
    >>> set_property(property_proto, 'foo', 'a', True) #  foo='a', str, indexed
  """
  property_proto.Clear()
  property_proto.name = name
  if isinstance(value, list):
    property_proto.multi = True
    for v in value:
      set_value(property_proto.value.add(), v, indexed)
  else:
    set_value(property_proto.value.add(), value, indexed)


def set_value(value_proto, value, indexed=True):
  """Set the corresponding datastore.Value _value field for the given arg.

  Args:
    value_proto: datastore.Value proto message.
    value: python object or datastore.Value. (unicode value will set a
    datastore string value, str value will set a blob string value).

  Raises:
    TypeError: the given value type is not supported.
  """
  value_proto.Clear()
  if isinstance(value, unicode):
    value_proto.string_value = value
  elif isinstance(value, str):
    value_proto.blob_value = value
  elif isinstance(value, bool):
    value_proto.boolean_value = value
  elif isinstance(value, int):
    value_proto.integer_value = value
  elif isinstance(value, float):
    value_proto.double_value = value
  elif isinstance(value, datetime.datetime):
    value_proto.timestamp_microseconds_value = to_timestamp_usec(value)
  elif isinstance(value, datastore_v1_pb2.Key):
    value_proto.key_value.CopyFrom(value)
  elif isinstance(value, datastore_v1_pb2.Entity):
    value_proto.entity_value.CopyFrom(value)
  elif isinstance(value, datastore_v1_pb2.Value):
    value_proto.CopyFrom(value)
  else:
    raise TypeError('value type: %r not supported' % (value,))
  if value_proto.indexed != bool(indexed):
    value_proto.indexed = indexed


def get_value(value_proto):
  """Gets the python object equivalent for the given value proto.

  Args:
    value_proto: datastore.Value proto message.

  Returns:
    corresponding python object value. (timestamp are converted to
    datetime, and datastore.Value is returned for
    blob_key_value).

  """
  for f in ('string_value',
            'blob_value',
            'boolean_value',
            'integer_value',
            'double_value',
            'key_value',
            'entity_value'):
    if value_proto.HasField(f):
      return getattr(value_proto, f)
  if value_proto.HasField('timestamp_microseconds_value'):
    return from_timestamp_usec(value_proto.timestamp_microseconds_value)
  if value_proto.HasField('blob_key_value'):
    return value_proto
  return None


def get_property_value(property_proto):
  """Gets the python object equivalent for the given property proto's value.

  Args:
    property_proto: datastore.Property proto message.

  Returns:
    datastore.Value proto message.
  """
  if property_proto.multi:
    return [get_value(v) for v in property_proto.value]
  assert len(property_proto.value) == 1
  return get_value(property_proto.value[0])


def get_property_values(entity_proto):
  """Convert datastore.Entity proto to a dict of property name -> python object.

  Args:
    entity_proto: datastore.Entity proto message.

  Usage:
    >>> get_property_values(entity_proto)
    {'foo': 'a', 'bar': 2}

  Returns:
    dict of entity properties.
  """
  return dict((p.name, get_property_value(p)) for p in entity_proto.property)


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
      proto.direction = datastore_v1_pb2.PropertyOrder.DESCENDING
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
  pf.operator = op
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
  cf.operator = op
  for f in filters:
    cf.filter.add().CopyFrom(f)
  return filter_proto


_EPOCH = datetime.datetime.utcfromtimestamp(0)


def from_timestamp_usec(timestamp):
  """Convert microsecond timestamp to datetime."""
  return _EPOCH + datetime.timedelta(microseconds=timestamp)


def to_timestamp_usec(dt):
  """Convert datetime to microsecond timestamp."""
  if dt.tzinfo:
    # this is an "aware" datetime with an explicit timezone. convert to UTC.
    dt = value.astimezone(UTC)
  return long(calendar.timegm(dt.timetuple()) * 1000000L) + dt.microsecond
