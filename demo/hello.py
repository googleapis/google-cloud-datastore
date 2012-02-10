import os

from google.appengine.ext import webapp
from google.appengine.ext.webapp import util

from ndb import model, tasklets

class Greeting(model.Model):
    message = model.StringProperty()
    userid = model.IntegerProperty()  # Not used here, but later

class HomePage(webapp.RequestHandler):
    def get(self):
        msg = Greeting.get_or_insert('hello', message='Hello world')
        self.response.out.write(msg.message)

urls = [('/.*', HomePage)]
app = tasklets.toplevel(webapp.WSGIApplication(urls).__call__)

def main():
    util.run_wsgi_app(app)

if __name__ == '__main__':
    main()
