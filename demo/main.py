import cgi
import logging
import time

from google.appengine.ext import webapp
from google.appengine.ext.webapp import util

from ndb import model
from core import datastore_query

form = """
<script>
function focus() {
  textarea = document.getElementById('body');
  textarea.focus();
}
</script>
<body onload=focus()>
<form method=POST action=/>
<textarea id=body name=body rows=6 cols=60></textarea>
<input type=submit>
</form>
</body>
"""

class Message(model.Model):
  """A guestbook message."""

  body = model.StringProperty()
  when = model.IntegerProperty()

class HomePage(webapp.RequestHandler):

  def get(self):
    self.response.out.write(form)
    order = datastore_query.PropertyOrder(
      'when',
      datastore_query.PropertyOrder.DESCENDING)
    query = datastore_query.Query(kind=Message.GetKind(), order=order)
    for batch in query.run(model.conn):
      for result in batch.results:
        self.response.out.write('<hr>%s<p>%s</p>' %
                                (time.ctime(result.when),
                                 cgi.escape(result.body)))

  def post(self):
    body = self.request.get('body')
    logging.info('body=%.100r', body)
    body = body.rstrip()
    if body:
      msg = Message()
      msg.body = body
      msg.when = int(time.time())
      msg.put()
      logging.info('key=%r', msg.key)
    self.redirect('/')

urls = [
  ('/', HomePage),
  ]

app = webapp.WSGIApplication(urls)

def main():
  util.run_wsgi_app(app)

if __name__ == '__main__':
  main()
