"""A web app to test multi-threading."""

import logging
import sys
import threading
import time

from google.appengine.api import memcache
from google.appengine.ext import webapp
from google.appengine.ext.webapp import util

import ndb


@ndb.tasklet
def fibonacci(n):
  """A recursive Fibonacci to exercise task switching."""
  if n <= 1:
    raise ndb.Return(n)
  a, b = yield fibonacci(n-1),fibonacci(n-2)
  raise ndb.Return(a + b)


class FibonacciMemo(ndb.Model):
  arg = ndb.IntegerProperty()
  value = ndb.IntegerProperty()


@ndb.tasklet
def memoizing_fibonacci(n):
  """A memoizing recursive Fibonacci to exercise RPCs."""
  if n <= 1:
    raise ndb.Return(n)
  key = ndb.Key(FibonacciMemo, str(n))
  memo = yield key.get_async(ndb_should_cache=False)
  if memo is not None:
    assert memo.arg == n
    logging.info('memo hit: %d -> %d', n, memo.value)
    raise ndb.Return(memo.value)
  logging.info('memo fail: %d', n)
  a = yield memoizing_fibonacci(n-1)
  b = yield memoizing_fibonacci(n-2)
  ans = a + b
  memo = FibonacciMemo(key=key, arg=n, value=ans)
  logging.info('memo write: %d -> %d', n, memo.value)
  yield memo.put_async(ndb_should_cache=False)
  raise ndb.Return(ans)


TRUE_VALUES = frozenset(['1', 'on', 't', 'true', 'y', 'yes'])


class FiboHandler(webapp.RequestHandler):

  @ndb.toplevel
  def get(self):
    num = 10
    try:
      num = int(self.request.get('num'))
    except Exception:
      pass
    if self.request.get('reset') in TRUE_VALUES:
      logging.info('flush')
      yield ndb.delete_multi_async(x.key for x in FibonacciMemo.query())
    t0 = time.time()
    if self.request.get('memo') in TRUE_VALUES:
      memo_type = 'memoizing '
      ans = yield memoizing_fibonacci(num)
    else:
      memo_type = ''
      ans = yield fibonacci(num)
    t1 = time.time()
    self.response.out.write('%sfibonacci(%d) == %d # computed in %.3f\n' %
                            (memo_type, num, ans, t1-t0))


urls = [
  ('/fibo', FiboHandler),
  ]

app = webapp.WSGIApplication(urls)


def main():
  util.run_wsgi_app(app)


if __name__ == '__main__':
  main()
