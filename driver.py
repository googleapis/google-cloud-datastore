#!/usr/bin/env python

"""Hit the fibo test site with many simultaneous requests."""

import threading
import time
import urllib
import sys

url = 'https://fibo.ndbdemo.prom.corp.google.com/fibo'


def main():
  num = 10
  try:
    num = int(sys.argv[1])
  except Exception:
    pass
  threads = []
  for i in range(num):
    t = threading.Thread(target=one_thread, args=(i, num,))
    t.start()
    threads.append(t)
  for t in threads:
    t.join()


def one_thread(i, num):
  print '%d: starting' % i
  t0 = time.time()
  ans = urllib.urlopen('%s?num=%d' % (url, num)).read()
  t1 = time.time()
  print '%d: %s (overall time %.3f seconds)' % (i, ans.strip(), t1-t0)


if __name__ == '__main__':
  main()
