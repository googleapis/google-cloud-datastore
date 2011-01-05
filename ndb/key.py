"""Key class and associated stuff.

TODO: docstrings, style
"""

import base64
import os

from google.appengine.datastore import entity_pb

from google.appengine.datastore import datastore_rpc

positional = datastore_rpc._positional

class Key(object):
  """An immutable datastore key.

  Long constructor forms:
    Key(pairs=[(kind, idorname), (kind, idorname), ...])
    Key(flat=[kind, idorname, kind, idorname, ...])
    Key(reference=<reference>)
    Key(serialized=<serialized reference>)
    Key(urlsafe=<urlsafe base64 encoded serialized reference>)

  Short constructor form:
    Key(kind, idorname, ...)  # Same as Key(flat=[kind, idorname, ...])

  Backdoor constructor form:
    Key(<dict>)  # If X is a doct, Key(X) == Key(**X)

  TODO: namespace, appid, parent
  """

  __slots__ = ['__reference']

  def __new__(cls, *_args, **kwargs):
    if _args:
      if len(_args) == 1 and isinstance(_args[0], dict):
        # For pickling only: one positional argument is allowed,
        # giving a dict specifying the keyword arguments.
        assert not kwargs
        kwargs = _args[0]
      else:
        assert 'flat' not in kwargs
        kwargs['flat'] = _args
    self = super(Key, cls).__new__(cls)
    self.__reference = _ConstructReference(cls, **kwargs)
    return self

  def __repr__(self):
    args = []
    for item in self._flat():
      if isinstance(item, unicode):
        item = item.encoded('utf8')
      if isinstance(item, basestring):
        args.append(repr(item))
      else:
        args.append(str(item))
    return 'Key(%s)' % ', '.join(args)

  __str__ = __repr__

  def __hash__(self):
    return hash(tuple(self._pairs()))

  def __eq__(self, other):
    if not isinstance(other, Key):
      return NotImplemented
    # TODO: app, namespace
    return tuple(self._pairs()) == tuple(other._pairs())

  def __ne__(self, other):
    if not isinstance(other, Key):
      return NotImplemented
    return not self.__eq__(other)

  def __getstate__(self):
    return ({'pairs': tuple(self._pairs())},)

  def __setstate__(self, state):
    assert len(state) == 1
    kwargs = state[0]
    assert isinstance(kwargs, dict)
    self.__reference = _ConstructReference(self.__class__, **kwargs)

  def __getnewargs__(self):
    # TODO: app, namespace
    return ({'pairs': tuple(self._pairs())},)

  def pairs(self):
    return list(self._pairs())

  def _pairs(self):
    for elem in self.__reference.path().element_list():
      kind = elem.type()
      if elem.has_id():
        idorname = elem.id()
      else:
        idorname = elem.name()
      yield (kind, idorname)

  def flat(self):
    return list(self._flat())

  def _flat(self):
    for kind, idorname in self._pairs():
      yield kind
      yield idorname

  def kind(self):
    kind = None
    for elem in self.__reference.path().element_list():
      kind = elem.type()
    return kind

  def reference(self):
    # TODO: In order to guarantee immutability, this must make a copy.
    # But most uses are from internal code which won't touch the
    # result and prefers to skip the copy.  What to do about this
    # moral dilemma?  Currently everybody uses k._Key__reference,
    # which seems the worst possible outcome.
    return _ReferenceFromReference(self.__reference)

  def serialized(self):
    return self.__reference.Encode()

  def urlsafe(self):
    # This is 3-4x faster than urlsafe_b64decode()
    urlsafe = base64.b64encode(self.__reference.Encode())
    return urlsafe.rstrip('=').replace('+', '-').replace('/', '_')

  # Datastore API using the default context.
  # These use local import since otherwise they'd be recursive imports.

  def get(self):
    return self.get_async().get_result()

  def get_async(self):
    from ndb import tasklets
    return tasklets.get_context().get(self)

  def delete(self):
    return self.delete_async().get_result()

  def delete_async(self):
    from ndb import tasklets
    return tasklets.get_context().delete(self)

@positional(1)
def _ConstructReference(cls, pairs=None, flat=None,
                        reference=None, serialized=None, urlsafe=None):
  assert cls is Key
  howmany = (bool(pairs) + bool(flat) +
             bool(reference) + bool(serialized) + bool(urlsafe))
  assert howmany == 1
  if flat or pairs:
    if flat:
      assert len(flat) % 2 == 0
      pairs = [(flat[i], flat[i+1]) for i in xrange(0, len(flat), 2)]
    assert pairs
    reference = _ReferenceFromPairs(pairs)
  else:
    if urlsafe:
      serialized = _DecodeUrlSafe(urlsafe)
    if serialized:
      reference = _ReferenceFromSerialized(serialized)
    assert reference.path().element_size()
    # TODO: assert that each element has a type and either an id or a name
    if not serialized:
      reference = _ReferenceFromReference(reference)
  if not reference.app():
    reference.set_app(_DefaultAppId())
  return reference

def _ReferenceFromPairs(pairs, reference=None):
  if reference is None:
    reference = entity_pb.Reference()
  path = reference.mutable_path()
  last = False
  for kind, idorname in pairs:
    assert not last, 'incomplete entry must be last'
    assert isinstance(kind, basestring)
    if isinstance(kind, unicode):
      kind = kind.encode('utf8')
    assert 1 <= len(kind) <= 500
    elem = path.add_element()
    elem.set_type(kind)
    if isinstance(idorname, (int, long)):
      assert 1 <= idorname < 2**63
      elem.set_id(idorname)
    elif isinstance(idorname, basestring):
      if isinstance(idorname, unicode):
        idorname = idorname.encode('utf8')
      assert 1 <= len(idorname) <= 500
      elem.set_name(idorname)
    elif idorname is None:
      elem.set_id(0)
      last = True
    else:
      assert False, 'bad idorname (%r)' % (idorname,)
  return reference

def _ReferenceFromReference(reference):
  new_reference = entity_pb.Reference()
  new_reference.CopyFrom(reference)
  return new_reference

def _ReferenceFromSerialized(serialized):
  assert isinstance(serialized, basestring)
  if isinstance(serialized, unicode):
    serialized = serialized.encode('utf8')
  return entity_pb.Reference(serialized)

def _DecodeUrlSafe(urlsafe):
  assert isinstance(urlsafe, basestring)
  if isinstance(urlsafe, unicode):
    urlsafe = urlsafe.encode('utf8')
  mod = len(urlsafe) % 4
  if mod:
    urlsafe += '=' * (4 - mod)
  # This is 3-4x faster than urlsafe_b64decode()
  return base64.b64decode(urlsafe.replace('-', '+').replace('_', '/'))

def _DefaultAppId():
  return os.getenv('APPLICATION_ID', '_')
