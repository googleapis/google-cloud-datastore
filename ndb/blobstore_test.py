"""Tests for blobstore.py."""

import pickle
import unittest

from google.appengine.api import namespace_manager
from google.appengine.api import datastore_types

from . import blobstore
from . import model
from . import test_utils


class BlobStoreTests(test_utils.NDBTest):

  def setUp(self):
    super(BlobStoreTests, self).setUp()


def main():
  unittest.main()


if __name__ == '__main__':
  main()
