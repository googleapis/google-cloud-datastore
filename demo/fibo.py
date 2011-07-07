"""A web app to test multi-threading."""

import sys
import threading
import time

from google.appengine.ext import webapp

from ndb import tasklets, context


@tasklets.tasklet
def fibonacci(n):
  """A recursive Fibonacci to exercise task switching."""
  if n <= 1:
    raise tasklets.Return(n)
  a = yield fibonacci(n-1)
  b = yield fibonacci(n-2)
  raise tasklets.Return(a + b)


class FiboHandler(webapp.RequestHandler):

  @context.toplevel
  def get(self):
    num = 10
    try:
      num = int(self.request.get('num'))
    except Exception:
      pass
    t0 = time.time()
    ans = yield fibonacci(num)
    t1 = time.time()
    self.response.out.write('fibonacci(%d) == %d # computed in %.3f\n' %
                            (num, ans, t1-t0))


urls = [
  ('/fibo', FiboHandler),
  ]

app = webapp.WSGIApplication(urls)
