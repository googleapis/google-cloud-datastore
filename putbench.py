"""Benchmark for put_multi().

Run this using 'make x CUSTOM=putbench FLAGS=-n'.
Use FLAGS=-o to get the corresponding profile for the old db package.
"""

import cProfile
import os
import pstats
import sys
import time

# Pay no attention to the testbed behind the curtain.
from google.appengine.ext import testbed
tb = testbed.Testbed()
tb.activate()
tb.init_datastore_v3_stub()
tb.init_memcache_stub()

from google.appengine.ext import db

import ndb

N = 1000


class Person(db.Model):
  a0 = db.StringProperty(default='a0')
  a1 = db.StringProperty(default='a1')
  a2 = db.StringProperty(default='a2')
  a3 = db.StringProperty(default='a3')
  a4 = db.StringProperty(default='a4')
  a5 = db.StringProperty(default='a5')
  a6 = db.StringProperty(default='a6')
  a7 = db.StringProperty(default='a7')
  a8 = db.StringProperty(default='a8')
  a9 = db.StringProperty(default='a9')

OldPerson = Person


class Person(ndb.Model):
  a0 = ndb.StringProperty(default='a0')
  a1 = ndb.StringProperty(default='a1')
  a2 = ndb.StringProperty(default='a2')
  a3 = ndb.StringProperty(default='a3')
  a4 = ndb.StringProperty(default='a4')
  a5 = ndb.StringProperty(default='a5')
  a6 = ndb.StringProperty(default='a6')
  a7 = ndb.StringProperty(default='a7')
  a8 = ndb.StringProperty(default='a8')
  a9 = ndb.StringProperty(default='a9')

NewPerson = Person


def put_old(people):
  keys = db.put(people)


def put_new(people):
  keys = ndb.put_multi(people, use_cache=False, use_memcache=False)


def timer(func, people):
  t0 = time.time()
  func(people)
  t1 = time.time()
  print '%.3f seconds' % (t1-t0)


def main(k=0):
  if k > 0:
    return main(k-1)
  try:
    n = int(sys.argv[-1])
  except:
    n = N
  if '-o' in sys.argv and '-n' not in sys.argv:
    people = [OldPerson() for i in xrange(n)]
    func = put_old
  elif '-n' in sys.argv and '-o' not in sys.argv:
    people = [NewPerson() for i in xrange(n)]
    func = put_new
  else:
    sys.stderr.write('Usage: $0 (-o|-n)\n')
    sys.exit(2)
  prof = cProfile.Profile()
  prof = prof.runctx('timer(func, people)', globals(), locals())
  stats = pstats.Stats(prof)
  stats.strip_dirs()
  stats.sort_stats('calls')  # 'time', 'cumulative', 'file' or 'calls'
  stats.print_stats(20)  # Arg: how many to print (optional)
  # Uncomment (and tweak) the following calls for more details.
  # stats.print_callees(100)
  # stats.print_callers(100)


if __name__ == '__main__':
  main(9)
