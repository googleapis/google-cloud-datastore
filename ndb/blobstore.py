"""NDB version of google.appengine.ext.blobstore."""

from google.appengine.api.blobstore import blobstore  # Internal version
from google.appengine.ext.blobstore import blobstore as ext_blobstore

from . import model

__all__ = ['BLOB_INFO_KIND',
           'BLOB_KEY_HEADER',
           'BLOB_MIGRATION_KIND',
           'BLOB_RANGE_HEADER',
           'BlobFetchSizeTooLargeError',
           'BlobInfo',
           'BlobInfoParseError',
           'BlobKey',
           'BlobNotFoundError',
           'BlobKeyProperty',
           'BlobReader',
           'DataIndexOutOfRangeError',
           'PermissionDeniedError',
           'Error',
           'InternalError',
           'MAX_BLOB_FETCH_SIZE',
           'UPLOAD_INFO_CREATION_HEADER',
           'create_rpc',
           'create_upload_url',
           'create_upload_url_async',
           'delete',
           'delete_async',
           'fetch_data',
           'fetch_data_async',
           'get',
           'parse_blob_info']

Error = blobstore.Error
InternalError = blobstore.InternalError
BlobFetchSizeTooLargeError = blobstore.BlobFetchSizeTooLargeError
BlobNotFoundError = blobstore.BlobNotFoundError
_CreationFormatError = blobstore._CreationFormatError
DataIndexOutOfRangeError = blobstore.DataIndexOutOfRangeError
PermissionDeniedError = blobstore.PermissionDeniedError
BlobInfoParseError = ext_blobstore.BlobInfoParseError

BlobKey = blobstore.BlobKey
create_rpc = blobstore.create_rpc
create_upload_url = blobstore.create_upload_url
create_upload_url_async = blobstore.create_upload_url_async
delete = blobstore.delete
delete_async = blobstore.delete_async

BLOB_INFO_KIND = blobstore.BLOB_INFO_KIND
BLOB_MIGRATION_KIND = blobstore.BLOB_MIGRATION_KIND
BLOB_KEY_HEADER = blobstore.BLOB_KEY_HEADER
BLOB_RANGE_HEADER = blobstore.BLOB_RANGE_HEADER
MAX_BLOB_FETCH_SIZE = blobstore.MAX_BLOB_FETCH_SIZE
UPLOAD_INFO_CREATION_HEADER = blobstore.UPLOAD_INFO_CREATION_HEADER

BlobKeyProperty = model.BlobKeyProperty


class BlobInfo(model.Model):
  """Information about blobs in Blobstore.

  Class methods:
  - query(): like ext.blobstore.BlobInfo.all(), but returns an NDB query
  - get(): like ext.blobstore.BlobInfo.get(), but for a single key only
  - get_multi(): ditto for multiple keys
  - get_async(), get_multi_async(): async version of get() and get_multi()
  - _get_kind(): like ext.blobstore.Blobinfo.kind()
  - There is no equivalent fot gql()

  Class properties:
  - _properties: like ext.blobstore.Blobinfo.properties()

  Instance methods:
  - delete(): delete this blob
  - delete_async(): async version of delete()
  - key(): return the BlobKey() for this blob
  - open(): return a BlobReader instance for this blob

  Instance properties:
  - content_type
  - creation
  - filename
  - size
  - md5_hash

  Because BlobInfo instances are immutable anyway, leave caching on.
  """

  @classmethod
  def _get_kind(cls):
    return BLOB_INFO_KIND  # __BlobInfo__

  content_type = model.StringProperty()
  creation = model.DateTimeProperty()
  filename = model.StringProperty()
  size = model.IntegerProperty()
  md5_hash = model.StringProperty()

  @classmethod
  def get(cls, blobkey):
    return cls.get_async(blobkey).get_result()

  @classmethod
  def get_async(cls, blobkey):
    assert isinstance(blobkey, (BlobKey, basestring))  # TODO: Another error
    return cls.get_by_id_async(str(blobkey))
  
  @classmethod
  def get_multi(cls, blobkeys):
    return [fut.get_result() for fut in cls.get_multi_async(blobkeys)]

  @classmethod
  def get_multi_async(cls, blobkeys):
    for blobkey in blobkeys:
      assert isinstance(blobkey, (BlobKey, basestring))  # TODO: Another error
    blobkeystrs = map(str, blobkeys)
    keys = [model.Key(BLOB_INFO_KIND, id) for id in blobkeystrs]
    return model.get_multi_async(keys)

  def _put_async(self):
    """Cheap way to make BlobInfo entities read-only."""
    assert False  # TODO: Another error
  put_async = _put_async

  def key(self):
    return BlobKey(self._key.id())  # Cache this?

  def delete(self):
    return self.delete_async().get_result()

  def delete_async(self):
    return blobstore.delete_async(self.key())

  def open(self, *args, **kwds):
    return BlobReader(self, *args, **kwds)


get = BlobInfo.get


def parse_blob_info(field_storage):
  """Parse a BlobInfo record from file upload field_storage."""
  if field_storage is None:
    return None

  field_name = field_storage.name

  def get_value(dct, name):
    value = dct.get(name, None)
    if value is None:
      raise BlobInfoParseError(
          'Field %s has no %s.' % (field_name, name))
    return value

  filename = get_value(field_storage.disposition_options, 'filename')
  blob_key_str = get_value(field_storage.type_options, 'blob-key')
  blob_key = BlobKey(blob_key_str)

  upload_content = email.message_from_file(field_storage.file)
  content_type = get_value(upload_content, 'content-type')
  size = get_value(upload_content, 'content-length')
  creation_string = get_value(upload_content, UPLOAD_INFO_CREATION_HEADER)
  md5_hash_encoded = get_value(upload_content, 'content-md5')
  md5_hash = base64.urlsafe_b64decode(md5_hash_encoded)

  try:
    size = int(size)
  except (TypeError, ValueError):
    raise BlobInfoParseError(
        '%s is not a valid value for %s size.' % (size, field_name))

  try:
    creation = blobstore._parse_creation(creation_string, field_name)
  except blobstore._CreationFormatError, err:
    raise BlobInfoParseError(str(err))

  return BlobInfo(id=blob_key_str,
                  content_type=content_type,
                  creation=creation,
                  filename=filename,
                  size=size,
                  md5_hash=md5_hash,
                  )


def fetch_data(blob, start_index, end_index, rpc=None):
  rpc = fetch_data_async(blob, start_index, end_index, rpc=rpc)
  return rpc.get_result()


def fetch_data_async(blob, start_index, end_index, rpc=None):
  if isinstance(blob, BlobInfo):
    blob = blob.key()
  return blobstore.fetch_data_async(blob, start_index, end_index, rpc=rpc)


class BlobReader(ext_blobstore.BlobReader):
  # Hack alert: this can access private attributes of the parent class
  # because it has the same class name.  (This is a Python feature.)

  @property
  def blob_info(self):
    if not self.__blob_info:
      self.__blob_info = BlobInfo.get(self.__blob_key)
    return self.__blob_info
