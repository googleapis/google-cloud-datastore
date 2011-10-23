"""Benchmark for Key comparison."""

import cProfile
import os
import pstats
import sys

from ndb import key
from ndb import utils

# Hack: replace os.environ with a plain dict.  This is to make the
# benchmark more similar to the production environment, where
# os.environ is also a plain dict.  In the environment where we run
# the benchmark, however, it is a UserDict instance, which makes the
# benchmark run slower -- but we don't want to measure this since it
# doesn't apply to production.
os.environ = dict(os.environ)


def bench1(n):
  """Benchmark Key comparison and hashing."""
  a = key.Key('Foo', 42, 'Bar', 1, 'Hopla', 'lala')
  b = key.Key('Foo', 42, 'Bar', 1, 'Hopla', 'lala')
  assert a is not b
  assert a == b
  for _ in xrange(n):
    a == b
    hash(a)


def bench2(n):
  """Benchmark Key creation."""
  for _ in xrange(n):
    key.Key('Foo', 42, 'Bar', 1, 'Hopla', 'lala')


def bench3(n):
  """Benchmark Key creation with parent."""
  p = key.Key('Foo', 42, 'Bar', 1)
  for _ in xrange(n):
    key.Key('Hopla', 'lala', parent=p)


def bench(n):
  """Toplevel benchmark function."""
  return bench3(n)


def main():
  utils.tweak_logging()  # Interpret -v and -q flags.
  n = 10000
  for arg in sys.argv[1:]:
    try:
      n = int(arg)
      break
    except Exception:
      pass
  prof = cProfile.Profile()
  prof = prof.runctx('bench(%d)' % n, globals(), locals())
  stats = pstats.Stats(prof)
  stats.strip_dirs()
  stats.sort_stats('time')  # 'time', 'cumulative' or 'calls'
  stats.print_stats(20)  # Arg: how many to print (optional)
  # Uncomment (and tweak) the following calls for more details.
  # stats.print_callees(10)
  # stats.print_callers(10)


if __name__ == '__main__':
  main()
