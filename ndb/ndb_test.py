"""Run all unittests."""

__author__ = 'Beech Horn'

import sys

try:
  import ndb
  from ndb.google_test_imports import unittest
  location = 'ndb'
except ImportError:
  import google3.third_party.apphosting.python.ndb
  from google3.third_party.apphosting.python.ndb.google_test_imports import unittest
  location = 'google3.third_party.apphosting.python.ndb'

def load_tests(loader, standard_tests, pattern):
  mods = ['context', 'eventloop', 'key', 'metadata',
          'msgprop', 'model', 'polymodel',
          'prospective_search', 'query', 'stats', 'tasklets', 'blobstore']
  test_mods = ['%s_test' % name for name in mods]
  ndb = __import__(location, fromlist=test_mods, level=1)

  for mod in [getattr(ndb, name) for name in test_mods]:
    for name in set(dir(mod)):
      if name.endswith('Tests'):
        test_module = getattr(mod, name)
        standard_tests.addTests(loader.loadTestsFromTestCase(test_module))

  return standard_tests


if __name__ == '__main__':
  unittest.main()
