#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Basic test for NdbDjangoMiddleware."""

__author__ = 'James A. Morrison'

import logging

from google.appengine.ext import webapp
from google.appengine.ext.webapp import util

from ndb import model, tasklets
from ndb import django_middleware


class Greeting(model.Model):
    message = model.StringProperty()


def _HandleMessage(handler):
  k = handler.request.get('key')
  g = None
  if k:
    g = model.Key(urlsafe=k).get()
  else:
    g = Greeting()
    g.message = '1234'
    g.put()
  handler.response.out.write(g.key.urlsafe())


class TestMiddleware(webapp.RequestHandler):
  def get(self):
    self.handler(_HandleMessage)

  def handler(self, f):
    m = django_middleware.NdbDjangoMiddleware()
    try:
      m.process_request(self.request)
      f(self)
      m.process_response(self.request, self.response)
    except Exception, e:
      logging.exception('boo')
      m.process_exception(self.request, e)


urls = [('/django_middleware', TestMiddleware)]
app = webapp.WSGIApplication(urls)


def main():
  util.run_wsgi_app(app)


if __name__ == '__main__':
  main()
