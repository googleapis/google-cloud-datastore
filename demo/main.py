import cgi
import logging
import time

from google.appengine.api import users
from google.appengine.datastore import entity_pb
from google.appengine.ext import webapp
from google.appengine.ext.webapp import util

from ndb import model
from core import datastore_query

HOME_PAGE = """
<script>
function focus() {
  textarea = document.getElementById('body');
  textarea.focus();
}
</script>
<body onload=focus()>
  Logged in as <a href="/account">%(email)s</a> |
  <a href="%(login)s">login</a> |
  <a href="%(logout)s">logout</a>

  <form method=POST action=/>
    <!-- TODO: XSRF protection -->
    <textarea id=body name=body rows=6 cols=60></textarea>
    <input type=submit>
  </form>
</body>
"""

ACCOUNT_PAGE = """
<body>
  Logged in as <a href="/account">%(email)s</a> |
  <a href="%(logout)s">logout</a>

  <form method=POST action=/account>
    <!-- TODO: XSRF protection -->
    Email: %(email)s
    <input type=submit name=%(action)s value="%(action)s Account">
    <input type=submit name=delete value="Delete Account">
    <a href=/>back to home page</a>
  </form>
</body>
"""

class Account(model.Model):
  """A user."""

  email = model.StringProperty()
  userid = model.StringProperty()

def GetAccountByUser(user, create=False):
  """Find an account."""
  assert isinstance(user, users.User)
  assert user.user_id() is not None
  prop = entity_pb.Property()
  prop.set_name(Account.userid.name)
  prop.set_multiple(False)
  pval = prop.mutable_value()
  pval.set_stringvalue(user.user_id())
  pred = datastore_query.PropertyFilter('=', prop)
  query = datastore_query.Query(kind=Account.GetKind(),
                                filter_predicate=pred)
  for batch in query.run(model.conn):
    for result in batch.results:
      assert isinstance(result, Account)
      return result
  if not create:
    return None
  account = Account(email=user.email(),
                    userid=user.user_id())
  account.put()
  return account

class Message(model.Model):
  """A guestbook message."""

  body = model.StringProperty()
  when = model.IntegerProperty()
  acct = model.KeyProperty()  # Account

class HomePage(webapp.RequestHandler):

  def get(self):
    order = datastore_query.PropertyOrder(
      'when',
      datastore_query.PropertyOrder.DESCENDING)
    query = datastore_query.Query(kind=Message.GetKind(), order=order)
    rpc = query.run_async(
      model.conn,
      query_options=datastore_query.QueryOptions(batch_size=2))

    user = users.get_current_user()
    email = None
    account = None
    if user is not None:
      email = user.email()
      account = GetAccountByUser(user)
    values = {'email': email,
              'login': users.create_login_url('/'),
              'logout': users.create_logout_url('/'),
              }
    self.response.out.write(HOME_PAGE % values)

    while rpc is not None:
      batch = rpc.get_result()
      logging.info("batch: %r; results = %r", batch, len(batch.results))
      rpc = batch.next_batch_async()
      for result in batch.results:
        author = 'None'
        account = None
        if result.acct is not None:
          account = Account.get(result.acct)
          if account is None:
            author = 'withdrawn'
          else:
            author = account.email or str(account)
        bodylines = []
        for line in map(cgi.escape, result.body.splitlines()):
          if not line:
            bodylines.append('<p>')
          else:
            bodylines.append(line)
        body = '\n'.join(bodylines)
        self.response.out.write('<hr>%s @ %s<p>%s</p>' %
                                (cgi.escape(author),
                                 time.ctime(result.when),
                                 body))

  def post(self):
    user = users.get_current_user()
    account = None
    if user is not None:
      account = GetAccountByUser(user, create=True)
    body = self.request.get('body')
    logging.info('body=%.100r', body)
    body = body.rstrip()
    if body:
      msg = Message()
      msg.body = body
      msg.when = int(time.time())
      if account is not None:
        msg.acct = account.key
      msg.put()
      logging.info('key=%r', msg.key)
    self.redirect('/')

class AccountPage(webapp.RequestHandler):

  def get(self):
    user = users.get_current_user()
    if user is None:
      self.redirect(users.create_login_url('/account'))
      return
    email = user.email()
    account = GetAccountByUser(user)
    action = 'Create'
    if account is not None:
      action = 'Update'
    values = {'email': email,
              'action': action,
              'login': users.create_login_url('/account'),
              'logout': users.create_logout_url('/'),
              }
    self.response.out.write(ACCOUNT_PAGE % values)

  def post(self):
    user = users.get_current_user()
    if user is None:
      self.redirect(users.create_login_url('/account'))
      return
    if self.request.get('delete'):
      account = GetAccountByUser(user)
      if account is not None:
        account.delete()
      self.redirect('/account')
      return
    account = GetAccountByUser(user, create=True)
    self.redirect('/account')

urls = [
  ('/', HomePage),
  ('/account', AccountPage),
  ]

app = webapp.WSGIApplication(urls)

def main():
  util.run_wsgi_app(app)

if __name__ == '__main__':
  main()
