"""MessageProperty -- a property storing ProtoRPC Message objects."""

# TODO:
# - elaborate docstrings
# - code review

from protorpc import messages
from protorpc import remote

from . import model
from . import utils

__all__ = ['MessageProperty', 'EnumProperty']

# TODO: Use ProtoRPC's global Protocols instance once it is in the SDK.
_protocols_registry = remote.Protocols.new_default()
_default_protocol = 'protobuf'


class EnumProperty(model.IntegerProperty):
  """Enums are represented in the datastore as integers.

  While this is less user-friendly in the Datastore viewer, it matches
  the representation of enums in the protobuf serialization (although
  not in JSON), and it allows renaming enum values without requiring
  changes to values already stored in the Datastore.
  """
  # TODO: Consider making the int-vs-str decision an option.

  _enum_type = None

  # Insert enum_type as an initial positional argument.
  _attributes = ['_enum_type'] + model.IntegerProperty._attributes
  _positional = 1 + model.IntegerProperty._positional

  @utils.positional(1 + _positional)
  def __init__(self, enum_type, name=None, default=None, choices=None, **kwds):
    self._enum_type = enum_type
    if default is not None:
      self._validate(default)
    if choices is not None:
      map(self._validate, choices)
    super(EnumProperty, self).__init__(name, default=default,
                                       choices=choices, **kwds)

  def _validate(self, value):
    if not isinstance(value, self._enum_type):
      raise TypeError('Expected a %s instance, got %r instead' %
                      (self._enum_type.__name__, value))

  def _to_base_type(self, enum):
    return enum.number

  def _from_base_type(self, val):
    return self._enum_type(val)


def _analyze_indexed_fields(indexed_fields):
  result = {}
  for field_name in indexed_fields:
    if not isinstance(field_name, basestring):
      raise TypeError('Field names must be strings; got %r' % (field_name,))
    if '.' not in field_name:
      if field_name in result:
        raise ValueError('Duplicate field name %s' % field_name)
      result[field_name] = None
    else:
      head, tail = field_name.split('.', 1)
      if head not in result:
        result[head] = [tail]
      elif result[head] is None:
        raise ValueError('Field name %s conflicts with ancestor %s' %
                         (field_name, head))
      else:
        result[head].append(tail)
  return result


def _make_model_class(message_type, indexed_fields, **props):
  analyzed = _analyze_indexed_fields(indexed_fields)
  for field_name, sub_fields in analyzed.iteritems():
    if field_name in props:
      # TODO: Solve this without reserving 'blob_'.
      raise ValueError('field name %s is reserved' % field_name)
    try:
      field = message_type.field_by_name(field_name)
    except KeyError:
      raise ValueError('Message type %s has no field named %s' %
                       (message_type.__name__, field_name))
    if isinstance(field, messages.MessageField):
      if not sub_fields:
        raise ValueError(
          'MessageField %s cannot be indexed, only sub-fields' % field_name)
      sub_model_class = _make_model_class(field.type, sub_fields)
      prop = model.StructuredProperty(sub_model_class, field_name,
                                      repeated=field.repeated)
    else:
      if sub_fields is not None:
        raise ValueError(
          'Unstructured field %s cannot have indexed sub-fields' % field_name)
      if isinstance(field, messages.EnumField):
        prop = EnumProperty(field.type, field_name, repeated=field.repeated)
      elif isinstance(field, messages.BytesField):
        prop = model.BlobProperty(field_name,
                                  repeated=field.repeated, indexed=True)
      else:
        # IntegerField, FloatField, BooleanField, StringField.
        prop = model.GenericProperty(field_name, repeated=field.repeated)
    props[field_name] = prop
  return model.MetaModel('_%s__Model' % message_type.__name__,
                         (model.Model,), props)


class MessageProperty(model.StructuredProperty):

  _message_type = None
  _indexed_fields = ()
  _protocol_name = None
  _protocol_impl = None

  # *Replace* first positional argument with _message_type, since the
  # _modelclass attribute is synthetic.
  _attributes = ['_message_type'] + model.StructuredProperty._attributes[1:]

  @utils.positional(1 + model.StructuredProperty._positional)
  def __init__(self, message_type, name=None,
               indexed_fields=None, protocol=None, **kwds):
    if not (isinstance(message_type, type) and
            issubclass(message_type, messages.Message)):
      raise TypeError('MessageProperty argument must be a Message subclass')
    self._message_type = message_type
    if indexed_fields is not None:
      # TODO: Check they are all strings naming fields
      self._indexed_fields = tuple(indexed_fields)
    # NOTE: Otherwise the class default i.e. (), prevails.
    if protocol is None:
      protocol = _default_protocol
    self._protocol_name = protocol
    self._protocol_impl = _protocols_registry.lookup_by_name(protocol)
    blob_prop = model.BlobProperty('__%s__' % self._protocol_name)
    message_class = _make_model_class(message_type, self._indexed_fields,
                                      blob_=blob_prop)
    super(MessageProperty, self).__init__(message_class, name, **kwds)

  def _validate(self, msg):
    if not isinstance(msg, self._message_type):
      raise TypeError('Expected a %s instance for %s property',
                      self._message_type.__name__,
                      self._code_name or self._name)

  def _to_base_type(self, msg):
    ent = _message_to_entity(msg, self._modelclass)
    ent.blob_ = self._protocol_impl.encode_message(msg)
    return ent

  def _from_base_type(self, ent):
    blob = ent.blob_
    if blob is not None:
      protocol = self._protocol_impl
    else:
      protocol = None
      for name in _protocols_registry.names:
        key = '__%s__' % name
        if key in ent._values:
          blob = ent._values[key]
          if isinstance(blob, model._BaseValue):
            blob = blob.b_val
          protocol = _protocols_registry.lookup_by_name(name)
          break
    if blob is None or protocol is None:
      return None
    msg = protocol.decode_message(self._message_type, blob)
    return msg


# Helper for _to_base_type().
def _message_to_entity(msg, modelclass):
  ent = modelclass()
  for prop_name, prop in modelclass._properties.iteritems():
    if prop._code_name == 'blob_':  # TODO: Devise a cleaner test.
      continue  # That's taken care of later.
    value = getattr(msg, prop_name)
    if value is not None and isinstance(prop, model.StructuredProperty):
      if prop._repeated:
        value = [_message_to_entity(v, prop._modelclass) for v in value]
      else:
        value = _message_to_entity(value, prop._modelclass)
    setattr(ent, prop_name, value)
  return ent
