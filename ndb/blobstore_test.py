"""Tests for blobstore.py."""

import datetime
import pickle
import unittest

from google.appengine.api import namespace_manager
from google.appengine.api import datastore_types
from google.appengine.api.blobstore import blobstore_stub
from google.appengine.api.blobstore import dict_blob_storage

from . import blobstore
from . import model
from . import tasklets
from . import test_utils


class BlobstoreTests(test_utils.NDBTest):

  def setUp(self):
    super(BlobstoreTests, self).setUp()
    storage = dict_blob_storage.DictBlobStorage()
    bs_stub = blobstore_stub.BlobstoreServiceStub(storage)
    self.testbed._register_stub('blobstore', bs_stub)

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

  def testBlobInfo_Get_Function(self):
    b = self.create_blobinfo('dummy')
    c = blobstore.get(b.key())
    self.assertEqual(c, b)
    self.assertTrue(c is not b)
    c = blobstore.get('dummy')
    self.assertEqual(c, b)
    self.assertTrue(c is not b)

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
    c = blobstore.BlobInfo.get_async(b.key()).get_result()
    self.assertEqual(c, b)
    self.assertTrue(c is not b)
    c = blobstore.BlobInfo.get_async(str(b.key())).get_result()
    self.assertEqual(c, b)
    self.assertTrue(c is not b)

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
    b.delete_async().get_result()
    d = blobstore.get(b.key())
    self.assertEqual(d, None)
  


def main():
  unittest.main()


if __name__ == '__main__':
  main()
