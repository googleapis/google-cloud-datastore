#
# Copyright 2008 Google Inc. All Rights Reserved.
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

import cgi
import urllib

from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app

import ndb


class Greeting(ndb.Model):
  """Models an individual Guestbook entry with content and date."""
  content = ndb.StringProperty()
  date = ndb.DateTimeProperty(auto_now_add=True)

  @classmethod
  def QueryBook(cls, ancestor_key):
    return cls.query(ancestor=ancestor_key).order(-cls.date)

class MainPage(webapp.RequestHandler):
  def get(self):
    self.response.out.write('<html><body>')
    guestbook_name = self.request.get('guestbook_name')
    ancestor_key = ndb.Key("Book", guestbook_name or "*fakebook*")
    greetings = Greeting.QueryBook(ancestor_key)

    for greeting in greetings:
      self.response.out.write('<blockquote>%s</blockquote>' %
                              cgi.escape(greeting.content))

    self.response.out.write("""
          <form action="/sign?%s" method="post">
            <div><textarea name="content" rows="3" cols="60"></textarea></div>
            <div><input type="submit" value="Sign Guestbook"></div>
          </form>
          <hr>
          <form>Guestbook name: <input value="%s" name="guestbook_name">
          <input type="submit" value="switch"></form>
        </body>
      </html>""" % (urllib.urlencode({'guestbook_name': guestbook_name}),
                          cgi.escape(guestbook_name)))

class Guestbook(webapp.RequestHandler):
  def post(self):
    # We set the parent key on each 'Greeting' to ensure each guestbook's
    # greetings are in the same entity group.
    guestbook_name = self.request.get('guestbook_name')
    greeting = Greeting(parent=ndb.Key("Book",
                                       guestbook_name or "*fakebook*"),
                        content = self.request.get('content'))
    greeting.put()
    self.redirect('/?' + urllib.urlencode({'guestbook_name': guestbook_name}))


application = webapp.WSGIApplication([
  ('/', MainPage),
  ('/sign', Guestbook)
])


def main():
  run_wsgi_app(application)


if __name__ == '__main__':
  main()
