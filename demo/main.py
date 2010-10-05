import logging

from google.appengine.ext import webapp
from google.appengine.ext.webapp import util

from ndb.model import Model
from ndb.key import Key

class Message(Model):
  """A guestbook message."""

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

class HomePage(webapp.RequestHandler):

  def get(self):
    self.response.out.write(form)

  def post(self):
    body = self.request.get('body')
    logging.info('body=%.100r', body)
    msg = Message()
    msg.setvalue('body', body)
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
