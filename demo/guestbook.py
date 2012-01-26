import cgi
import urllib

from ndb import model
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app


class Greeting(model.Model):
  """Models an individual Guestbook entry with content and date."""
  content = model.StringProperty()
  date = model.DateTimeProperty(auto_now_add=True)

  @classmethod
  def QueryBook(cls, ancestor_key):
    return cls.query(ancestor=ancestor_key).order(-cls.date)

class MainPage(webapp.RequestHandler):
  def get(self):
    self.response.out.write('<html><body>')
    guestbook_name = self.request.get('guestbook_name')
    ancestor_key = model.Key("Book", guestbook_name or "*fakebook*")
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
    greeting = Greeting(parent=model.Key("Book", guestbook_name or "*fakebook*"),
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
