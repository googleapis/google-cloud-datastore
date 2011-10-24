from google.appengine.ext import webapp
from google.appengine.ext.webapp import util

from ndb.model import *
from ndb.query import *

class M(Model):
 name = StringProperty()
 age = IntegerProperty()

class RQ(webapp.RequestHandler):
 def get(self):
   if self.request.get('i'):
     ms = [M(name=str(i), age=i % 10) for i in range(1000)]
     put_multi(ms)
     self.response.out.write('Initialized')
     self.response.out.write('<p><a href="/intest">Run test</a>')
     return
   bs = 100
   if self.request.get('bs'):
     bs = int(self.request.get('bs'))
   values = range(10)
   q = M.query(M.age.IN(values)).order(M.age, M.key)
   ms = q.fetch(batch_size=bs, prefetch_size=bs)  # <------------
   self.response.out.write('Got %d results' % len(ms))
   self.response.out.write('<p><a href="/_ah/stats">Appstats</a>')

urls = [
 ('/.*', RQ),
 ]

app = webapp.WSGIApplication(urls)

def main():
 util.run_wsgi_app(app)

if __name__ == '__main__':
 main()
