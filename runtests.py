"""Alternate way of running the unittests, for Python 2.5 or Windows."""

__author__ = 'Beech Horn'

import unittest


def suite():
  mods = ['context', 'eventloop', 'key', 'model', 'query', 'tasklets', 'thread']
  test_mods = ['%s_test' % name for name in mods]
  ndb = __import__('ndb', fromlist=test_mods, level=1)

  loader = unittest.TestLoader()
  suite = unittest.TestSuite()

  for mod in [getattr(ndb, name) for name in test_mods]:
    for name in set(dir(mod)):
      if name.endswith('Tests'):
        test_module = getattr(mod, name)
        tests = loader.loadTestsFromTestCase(test_module)
        suite.addTests(tests)

  return suite


def main():
  unittest.TextTestRunner(verbosity=1).run(suite())


if __name__ == '__main__':
  main()
