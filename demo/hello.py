#
# Copyright 2008 The ndb Authors. All Rights Reserved.
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
