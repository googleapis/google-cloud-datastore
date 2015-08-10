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

from google.appengine.ext import webapp
from google.appengine.ext.webapp import util

from ndb import *

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
