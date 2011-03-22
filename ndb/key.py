"""The Key class, and associated utilities.

A Key encapsulates the following pieces of information, which together
uniquely designate a (possible) entity in the App Engine datastore:

- an application id (a string)
- a namespace (a string)
- a list of one or more (kind, id) pairs where kind is a string and id
  is either a string or an integer.

The appication id must always be part of the key, but since most
applications can only access their own entities, it defaults to the
current application id and you rarely need to worry about it.  It must
not be empty.

The namespace designates a top-level partition of the key space for a
particular application.  If you've never heard of namespaces, you can
safely ignore this feature.

Most of the action is in the (kind, id) pairs.  A key must have at
least one (kind, id) pair.  The last (kind, id) pair gives the kind
and the id of the entity that the key refers to, the others merely
specify a 'parent key'.

The kind is a string giving the name of the model class used to
represent the entity.  (In more traditional databases this would be
the table name.)  A model class is a Python class derived from
ndb.Model; see the documentation for ndb/model.py.  Only the class
name itself is used as the kind.  This means all your model classes
must be uniquely named within one application.  You can override this
on a per-class basis.

The id is either a string or an integer.  When the id is a string, the
application is in control of how it assigns ids: For example, if you
could use an email address as the id for Account entities.

To use integer ids, you must let the datastore choose a uniqe id for
an entity when it is first inserted into the datastore.  You can set
the id to None to represent the key for an entity that hasn't yet been
inserted into the datastore.  The final key (including the assigned
id) will be returned after the entity is successfully inserted into
the datastore.

A key for which the id of the last (kind, id) pair is set to None is
called an incomplete key.  Such keys can only be used to insert
entities into the datastore.

A key with exactly one (kind, id) pair is called a toplevel key or a
root key.  Toplevel keys are also used as entity groups, which play a
role in transaction management.

If there is more than one (kind, id) pair, all but the last pair
represent the 'ancestor path', also known as the key of the 'parent
entity'.

Other constraints:

- Kinds and string ids must not be empty and must be at most 500 bytes
  long (after UTF-8 encoding, if given as Python unicode objects).

- Integer ids must be at least 1 and less than 2**63.

For more info about namespaces, see
http://code.google.com/appengine/docs/python/multitenancy/overview.html.
The namespace defaults to the 'default namespace' selected by the
namespace manager.  To explicitly select the empty namespace pass
namespace=''.
"""

__author__ = 'guido@google.com (Guido van Rossum)'

# TODO: Change asserts to better exceptions.

import base64
import os

from google.appengine.api import datastore_errors
from google.appengine.api import namespace_manager
from google.appengine.datastore import datastore_rpc
from google.appengine.datastore import entity_pb

__all__ = ['Key']


class Key(object):
  """An immutable datastore key.

  For flexibility and convenience, multiple constructor signatures are
  supported.

  The primary way to construct a key is using positional arguments:
  - Key(kind1, id1, kind2, id2, ...).

  This is shorthand for either of the following two longer forms:
  - Key(pairs=[(kind1, id1), (kind2, id2), ...])
  - Key(flat=[kind1, id1, kind2, id2, ...])

  Either of the above constructor forms can additional pass in another
  key using parent=<key>.  The (kind, id) pairs of the parent key are
  inserted before the (kind, id) pairs passed explicitly.

  You can also construct a Key from a 'url-safe' encoded string:
  - Key(urlsafe=<string>)

  For esoteric purposes the following constructors exist:
  - Key(reference=<reference>) -- passing in a low-level Reference object
  - Key(serialized=<string>) -- passing in a serialized low-level Reference
  - Key(<dict>) -- for unpickling, the same as Key(**<dict>)

  The 'url-safe' string is really a websafe-base64-encoded serialized
  Reference, but it's best to think of it as just an opaque unique
  string.

  Additional constructor keyword arguments:
  - app=<string> -- specify the application id
  - namespace=<string> -- specify the namespace

  If a Reference is passed (using one of reference, serialized or
  urlsafe), the args and namespace keywords must match what is already
  present in the Reference (after decoding if necessary).  The parent
  keyword cannot be combined with a Refence in any form.


  Keys are immutable, which means that a Key object cannot be modified
  once it has been created.  This is enforced by the implementation as
  well as Python allows.

  For access to the contents of a key, the following methods and
  operations are supported:

  - repr(key), str(key) -- return a string representation resembling
    the shortest constructor form, omitting the app and namespace
    unless they differ from the default value.

  - key1 == key2, key1 != key2 -- comparison for equality between Keys.

  - hash(key) -- a hash value sufficient for storing Keys in a dict.

  - key.pairs() -- a list of (kind, id) pairs.

  - key.flat() -- a list of flattened kind and id values, i.e.
    [kind1, id1, kind2, id2, ...].

  - key.app() -- the application id.

  - key.id() -- the string or integer id in the last (kind, id) pair,
    or None if the key is incomplete.

  - key.string_id() -- the string id in the last (kind, id) pair,
    or None if the key has an integer id or is incomplete.

  - key.integer_id() -- the integer id in the last (kind, id) pair,
    or None if the key has a string id or is incomplete.

  - key.namespace() -- the namespace.

  - key.kind() -- a shortcut for key.pairs()[-1][0].

  - key.parent() -- a Key constructed from all but the last (kind, id)
    pairs.

  - key.urlsafe() -- a websafe-base64-encoded serialized Reference.

  - key.serialized() -- a serialized Reference.

  - key.reference() -- a Reference object.  Since Reference objects are
    mutable, this returns a brand new Reference object.

  - key._reference() -- the Reference object contained in the Key.
    The caller promises not to mutate it.

  - key._pairs() -- an iterator, equivalent to iter(key.pairs()).

  - key._flat() -- an iterator, equivalent to iter(key.flat()).

  Keys also support interaction with the datastore; these methods are
  the only ones that engage in any kind of I/O activity.  For Future
  objects, see the document for ndb/tasklets.py.

  - key.get() -- return the entity for the Key.

  - key.get_async() -- return a Future whose eventual result is
    the entity for the Key.

  - key.delete() -- delete the entity for the Key.

  - key.delete_async() -- asynchronously delete the entity for the Key.

  Keys may be pickled.

  Subclassing Key is best avoided; it would be hard to get right.
  """

  __slots__ = ['__reference']

  def __new__(cls, *_args, **kwargs):
    """Constructor.  See the class docstring for arguments."""
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
    """String representation, used by str() and repr().

    We produce a short string that conveys all relevant information,
    suppressing app and namespace when they are equal to the default.
    """
    # TODO: Instead of "Key('Foo', 1)" perhaps return "Key(Foo, 1)" ?
    args = []
    for item in self._flat():
      if not item:
        args.append('None')
      elif isinstance(item, basestring):
        assert isinstance(item, str)  # No unicode should make it here.
        args.append(repr(item))
      else:
        args.append(str(item))
    if self.app() != _DefaultAppId():
      args.append('app=%r' % self.app())
    if self.namespace() != _DefaultNamespace():
      args.append('namespace=%r' % self.namespace())
    return 'Key(%s)' % ', '.join(args)

  __str__ = __repr__

  def __hash__(self):
    """Hash value, for use in dict lookups."""
    # This ignores app and namespace, which is fine since hash()
    # doesn't need to return a unique value -- it only needs to ensure
    # that the hashes of equal keys are equal, not the other way
    # around.
    return hash(tuple(self._pairs()))

  def __eq__(self, other):
    """Equality comparison operation."""
    if not isinstance(other, Key):
      return NotImplemented
    return (tuple(self._pairs()) == tuple(other._pairs()) and
            self.app() == other.app() and
            self.namespace() == other.namespace())

  def __ne__(self, other):
    """The opposite of __eq__."""
    if not isinstance(other, Key):
      return NotImplemented
    return not self.__eq__(other)

  def __getstate__(self):
    """Private API used for pickling."""
    return ({'pairs': tuple(self._pairs()),
             'app': self.app(),
             'namespace': self.namespace()},)

  def __setstate__(self, state):
    """Private API used for pickling."""
    assert len(state) == 1
    kwargs = state[0]
    assert isinstance(kwargs, dict)
    self.__reference = _ConstructReference(self.__class__, **kwargs)

  def __getnewargs__(self):
    """Private API used for pickling."""
    return ({'pairs': tuple(self._pairs()),
             'app': self.app(),
             'namespace': self.namespace()},)

  def parent(self):
    """Return a Key constructed from all but the last (kind, id) pairs.

    If there is only one (kind, id) pair, return None.
    """
    pairs = self.pairs()
    if len(pairs) <= 1:
      return None
    return Key(pairs=pairs[:-1], app=self.app(), namespace=self.namespace())

  def root(self):
    """Return the root key.  This is either self or the highest parent."""
    pairs = self.pairs()
    if len(pairs) <= 1:
      return self
    return Key(pairs=pairs[:1], app=self.app(), namespace=self.namespace())

  def namespace(self):
    """Return the namespace."""
    return self.__reference.name_space()

  def app(self):
    """Return the application id."""
    return self.__reference.app()

  def id(self):
    """Return the string or integer id in the last (kind, id) pair, if any.

    Returns:
      A string or integer id, or None if the key is incomplete.
    """
    elem = self.__reference.path().element(-1)
    return elem.name() or elem.id() or None

  def string_id(self):
    """Return the string id in the last (kind, id) pair, if any.

    Returns:
      A string id, or None if the key has an integer id or is incomplete.
    """
    elem = self.__reference.path().element(-1)
    return elem.name() or None

  def integer_id(self):
    """Return the integer id in the last (kind, id) pair, if any.

    Returns:
      An integer id, or None if the key has a string id or is incomplete.
    """
    elem = self.__reference.path().element(-1)
    return elem.id() or None

  def pairs(self):
    """Return a list of (kind, id) pairs."""
    return list(self._pairs())

  def _pairs(self):
    """Iterator yielding (kind, id) pairs."""
    for elem in self.__reference.path().element_list():
      kind = elem.type()
      if elem.has_id():
        idorname = elem.id()
      else:
        idorname = elem.name()
      if not idorname:
        idorname = None
      yield (kind, idorname)

  def flat(self):
    """Return a list of alternating kind and id values."""
    return list(self._flat())

  def _flat(self):
    """Iterator yielding alternating kind and id values."""
    for kind, idorname in self._pairs():
      yield kind
      yield idorname

  def kind(self):
    """Return the kind of the entity referenced.

    This is the kind from the last (kind, id) pair.
    """
    return self.__reference.path().element(-1).type()

  def reference(self):
    """Return a copy of the Reference object for this Key.

    This is a entity_pb.Reference instance -- a protocol buffer class
    used by the lower-level API to the datastore.
    """
    return _ReferenceFromReference(self.__reference)

  def _reference(self):
    """Return the Reference object for this Key.

    This is a backdoor API for internal use only.  The caller should
    not mutate the return value.
    """
    return self.__reference

  def serialized(self):
    """Return a serialized Reference object for this Key."""
    return self.__reference.Encode()

  def urlsafe(self):
    """Return a url-safe string encoding this Key's Reference.

    This string is compatible with other APIs and languages and with
    the strings used to represent Keys in GQL and in the App Engine
    Admin Console.
    """
    # This is 3-4x faster than urlsafe_b64decode()
    urlsafe = base64.b64encode(self.__reference.Encode())
    return urlsafe.rstrip('=').replace('+', '-').replace('/', '_')

  # Datastore API using the default context.
  # These use local import since otherwise they'd be recursive imports.

  def get(self):
    """Synchronously get the entity for this Key.

    Return None if there is no such entity.
    """
    return self.get_async().get_result()

  def get_async(self):
    """Return a Future whose result is the entity for this Key.

    If no such entity exists, a Future is still returned, and the
    Future's eventual return result be None.
    """
    from ndb import tasklets
    return tasklets.get_context().get(self)

  def delete(self):
    """Synchronously delete the entity for this Key.

    This is a no-op if no such entity exists.
    """
    return self.delete_async().get_result()

  def delete_async(self):
    """Schedule deletion of the entity for this Key.

    This returns a Future, whose result becomes available once the
    deletion is complete.  If no such entity exists, a Future is still
    returned.  In all cases the Future's result is None (i.e. there is
    no way to tell whether the entity existed or not).
    """
    from ndb import tasklets
    return tasklets.get_context().delete(self)


# The remaining functions in this module are private.

@datastore_rpc._positional(1)
def _ConstructReference(cls, pairs=None, flat=None,
                        reference=None, serialized=None, urlsafe=None,
                        app=None, namespace=None, parent=None):
  """Construct a Reference; the signature is the same as for Key."""
  assert cls is Key
  howmany = (bool(pairs) + bool(flat) +
             bool(reference) + bool(serialized) + bool(urlsafe))
  assert howmany == 1
  if flat or pairs:
    if flat:
      assert len(flat) % 2 == 0
      pairs = [(flat[i], flat[i+1]) for i in xrange(0, len(flat), 2)]
    assert pairs
    if parent is not None:
      if not isinstance(parent, Key):
        raise datastore_errors.BadValueError(
            'Expected Key instance, got %r' % parent)
      pairs[:0] = parent.pairs()
      if app:
        assert app == parent.app(), (app, parent.app())
      else:
        app = parent.app()
      if namespace is not None:
        assert namespace == parent.namespace(), (namespace,
                                                 parent.namespace())
      else:
        namespace = parent.namespace()
    reference = _ReferenceFromPairs(pairs, app=app, namespace=namespace)
  else:
    # You can't combine parent= with reference=, serialized= or urlsafe=.
    assert parent is None
    if urlsafe:
      serialized = _DecodeUrlSafe(urlsafe)
    if serialized:
      reference = _ReferenceFromSerialized(serialized)
    assert reference.path().element_size()
    # TODO: assert that each element has a type and either an id or a name
    if not serialized:
      reference = _ReferenceFromReference(reference)
    # You needn't specify app= or namespace= together with reference=,
    # serialized= or urlsafe=, but if you do, their values must match
    # what is already in the reference.
    if app is not None:
      assert app == reference.app(), (app, reference.app())
    if namespace is not None:
      assert namespace == reference.name_space(), (namespace,
                                                   reference.name_space())
  return reference


def _ReferenceFromPairs(pairs, reference=None, app=None, namespace=None):
  """Construct a Reference from a list of pairs.

  If a Reference is passed in as the second argument, it is modified
  in place.  The app and namespace are set from the corresponding
  keyword arguments, with the customary defaults.
  """
  if reference is None:
    reference = entity_pb.Reference()
  path = reference.mutable_path()
  last = False
  for kind, idorname in pairs:
    if last:
      raise datastore_errors.BadArgumentError(
          'Incomplete Key entry must be last')
    if not isinstance(kind, basestring):
      if isinstance(kind, type):
        # Late import to avoid cycles.
        from ndb.model import Model
        modelclass = kind
        assert issubclass(modelclass, Model), repr(modelclass)
        kind = modelclass.GetKind()
      assert isinstance(kind, basestring), (repr(modelclass), repr(kind))
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
  # An empty app id means to use the default app id.
  if not app:
    app = _DefaultAppId()
  # Always set the app id, since it is mandatory.
  reference.set_app(app)
  # An empty namespace overrides the default namespace.
  if namespace is None:
    namespace = _DefaultNamespace()
  # Only set the namespace if it is not empty.
  if namespace:
    reference.set_name_space(namespace)
  return reference


def _ReferenceFromReference(reference):
  """Copy a Reference."""
  new_reference = entity_pb.Reference()
  new_reference.CopyFrom(reference)
  return new_reference


def _ReferenceFromSerialized(serialized):
  """Construct a Reference from a serialized Reference."""
  assert isinstance(serialized, basestring)
  if isinstance(serialized, unicode):
    serialized = serialized.encode('utf8')
  return entity_pb.Reference(serialized)


def _DecodeUrlSafe(urlsafe):
  """Decode a url-safe base64-encoded string.

  This returns the decoded string.
  """
  assert isinstance(urlsafe, basestring)
  if isinstance(urlsafe, unicode):
    urlsafe = urlsafe.encode('utf8')
  mod = len(urlsafe) % 4
  if mod:
    urlsafe += '=' * (4 - mod)
  # This is 3-4x faster than urlsafe_b64decode()
  return base64.b64decode(urlsafe.replace('-', '+').replace('_', '/'))


def _DefaultAppId():
  """Return the default application id.

  This is taken from the APPLICATION_ID environment variable.
  """
  return os.getenv('APPLICATION_ID', '_')


def _DefaultNamespace():
  """Return the default namespace.

  This is taken from the namespace manager.
  """
  return namespace_manager.get_namespace()
