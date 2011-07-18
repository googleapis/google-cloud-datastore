import os

from google.appengine.ext import webapp
from google.appengine.ext.webapp import util

from ndb import model

class Greeting(model.Model):
    message = model.StringProperty()
    userid = model.IntegerProperty()  # Not used here, but later

def fix_user_id():
    email = os.getenv('USER_EMAIL')
    if email and not os.getenv('USER_ID'):
        os.environ['USER_ID'] = email

class HomePage(webapp.RequestHandler):
    def get(self):
        fix_user_id()
        msg = Greeting.get_or_insert('hello', message='Hello world')
        self.response.out.write(msg.message)

urls = [('/.*', HomePage)]
app = webapp.WSGIApplication(urls)

def main():
    util.run_wsgi_app(app)

if __name__ == '__main__':
    main()
