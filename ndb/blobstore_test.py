"""Tests for blobstore.py."""

import datetime
import pickle
import unittest

from google.appengine.api import namespace_manager
from google.appengine.api import datastore_types

from . import blobstore
from . import model
from . import tasklets
from . import test_utils


class BlobstoreTests(test_utils.NDBTest):

  def setUp(self):
    super(BlobstoreTests, self).setUp()
    self.testbed.init_blobstore_stub()

  the_module = blobstore

  def testConstants(self):
    # This intentionally hardcodes the values.  I'd like to know when
    # they change.
    self.assertEqual(blobstore.BLOB_INFO_KIND, '__BlobInfo__')
    self.assertEqual(blobstore.BLOB_MIGRATION_KIND, '__BlobMigration__')
    self.assertEqual(blobstore.BLOB_KEY_HEADER, 'X-AppEngine-BlobKey')
    self.assertEqual(blobstore.BLOB_RANGE_HEADER, 'X-AppEngine-BlobRange')
    self.assertEqual(blobstore.UPLOAD_INFO_CREATION_HEADER,
                     'X-AppEngine-Upload-Creation')
    self.assertEqual(blobstore.MAX_BLOB_FETCH_SIZE, 1015808)

  def testExceptions(self):
    self.assertTrue(issubclass(blobstore.Error, Exception))
    self.assertTrue(issubclass(blobstore.InternalError, blobstore.Error))
    self.assertTrue(issubclass(blobstore.BlobFetchSizeTooLargeError,
                               blobstore.Error))
    self.assertTrue(issubclass(blobstore.BlobNotFoundError, blobstore.Error))
    self.assertTrue(issubclass(blobstore.DataIndexOutOfRangeError,
                               blobstore.Error))
    self.assertTrue(issubclass(blobstore.PermissionDeniedError,
                               blobstore.Error))
    self.assertTrue(issubclass(blobstore.BlobInfoParseError, blobstore.Error))

  def create_blobinfo(self, blobkey):
    """Handcraft a dummy BlobInfo."""
    b = blobstore.BlobInfo(key=model.Key(blobstore.BLOB_INFO_KIND, blobkey),
                           content_type='text/plain',
                           creation=datetime.datetime(2012, 1, 24, 8, 15, 0),
                           filename='hello.txt',
                           size=42,
                           md5_hash='xxx')
    model.Model._put_async(b).check_success()
    return b

  def testBlobInfo(self):
    b = self.create_blobinfo('dummy')
    self.assertEqual(b._get_kind(), blobstore.BLOB_INFO_KIND)
    self.assertEqual(b.key(), blobstore.BlobKey('dummy'))
    self.assertEqual(b.content_type, 'text/plain')
    self.assertEqual(b.creation, datetime.datetime(2012, 1, 24, 8, 15, 0))
    self.assertEqual(b.filename, 'hello.txt')
    self.assertEqual(b.md5_hash, 'xxx')

  def testBlobInfo_PutErrors(self):
    b = self.create_blobinfo('dummy')
    self.assertRaises(Exception, b.put)
    self.assertRaises(Exception, b.put_async)
    self.assertRaises(Exception, model.put_multi, [b])
    self.assertRaises(Exception, model.put_multi_async, [b])

  def testBlobInfo_Get(self):
    b = self.create_blobinfo('dummy')
    c = blobstore.BlobInfo.get(b.key())
    self.assertEqual(c, b)
    self.assertTrue(c is not b)
    c = blobstore.BlobInfo.get('dummy')
    self.assertEqual(c, b)
    self.assertTrue(c is not b)

  def testBlobInfo_GetAsync(self):
    b = self.create_blobinfo('dummy')
    cf = blobstore.BlobInfo.get_async(b.key())
    self.assertTrue(isinstance(cf, tasklets.Future))
    c = cf.get_result()
    self.assertEqual(c, b)
    self.assertTrue(c is not b)
    df = blobstore.BlobInfo.get_async(str(b.key()))
    self.assertTrue(isinstance(df, tasklets.Future))
    d = df.get_result()
    self.assertEqual(d, b)
    self.assertTrue(d is not b)

  def testBlobInfo_GetMulti(self):
    b = self.create_blobinfo('b')
    c = self.create_blobinfo('c')
    d, e = blobstore.BlobInfo.get_multi([b.key(), str(c.key())])
    self.assertEqual(d, b)
    self.assertEqual(e, c)

  def testBlobInfo_GetMultiAsync(self):
    b = self.create_blobinfo('b')
    c = self.create_blobinfo('c')
    df, ef = blobstore.BlobInfo.get_multi_async([str(b.key()), c.key()])
    self.assertTrue(isinstance(df, tasklets.Future))
    self.assertTrue(isinstance(ef, tasklets.Future))
    d, e = df.get_result(), ef.get_result()
    self.assertEqual(d, b)
    self.assertEqual(e, c)

  def testBlobInfo_Delete(self):
    b = self.create_blobinfo('dummy')
    c = blobstore.get(b._key.id())
    self.assertEqual(c, b)
    b.delete()
    d = blobstore.get(b.key())
    self.assertEqual(d, None)

  def testBlobInfo_DeleteAsync(self):
    b = self.create_blobinfo('dummy')
    df = b.delete_async()
    self.assertTrue(isinstance(df, tasklets.Future), df)
    df.get_result()
    d = blobstore.get(b.key())
    self.assertEqual(d, None)

  def testBlobstore_Get(self):
    b = self.create_blobinfo('dummy')
    c = blobstore.get(b.key())
    self.assertEqual(c, b)
    self.assertTrue(c is not b)
    c = blobstore.get('dummy')
    self.assertEqual(c, b)
    self.assertTrue(c is not b)

  def testBlobstore_GetAsync(self):
    b = self.create_blobinfo('dummy')
    cf = blobstore.get_async(b.key())
    self.assertTrue(isinstance(cf, tasklets.Future))
    c = cf.get_result()
    self.assertEqual(c, b)
    self.assertTrue(c is not b)
    cf = blobstore.get_async('dummy')
    c = cf.get_result()
    self.assertEqual(c, b)
    self.assertTrue(c is not b)


def main():
  unittest.main()


if __name__ == '__main__':
  main()
