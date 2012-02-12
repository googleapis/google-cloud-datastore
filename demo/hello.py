import os

from google.appengine.ext import webapp
from google.appengine.ext.webapp import util

import ndb

class Greeting(ndb.Model):
    message = ndb.StringProperty()
    userid = ndb.IntegerProperty()  # Not used here, but later

class HomePage(webapp.RequestHandler):
    def get(self):
        msg = Greeting.get_or_insert('hello', message='Hello world')
        self.response.out.write(msg.message)

urls = [('/.*', HomePage)]
app = ndb.toplevel(webapp.WSGIApplication(urls))

def main():
    util.run_wsgi_app(app)

if __name__ == '__main__':
    main()
