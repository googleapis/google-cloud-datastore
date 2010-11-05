"""A simple guestbook app to test parts of NDB end-to-end."""

import cgi
import logging
import sys
import time

from google.appengine.api import users
from google.appengine.datastore import entity_pb
from google.appengine.ext import webapp
from google.appengine.ext.webapp import util

from core import datastore_query
from core import datastore_rpc
from core import monkey

import bpt
from ndb import context
from ndb import eventloop
from ndb import model
from ndb import tasks

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
    <input type=text id=body name=body size=60>
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

class Message(model.Model):
  """A guestbook message."""

  body = model.StringProperty()
  when = model.IntegerProperty()
  userid = model.StringProperty()

def WaitForRpcs():
  eventloop.run()

@tasks.task
def AsyncGetAccountByUser(context, user, create=False):
  """Find an account."""
  assert isinstance(user, users.User), '%r is not a User' % user
  assert user.user_id() is not None, 'user_id is None'
  prop = entity_pb.Property()
  prop.set_name(Account.userid.name)
  prop.set_multiple(False)
  pval = prop.mutable_value()
  pval.set_stringvalue(user.user_id())
  pred = datastore_query.PropertyFilter('=', prop)
  query = datastore_query.Query(kind=Account.GetKind(),
                                filter_predicate=pred)
  options = datastore_query.QueryOptions(limit=1)

  def callback(entity):
    return entity  # TODO: This should be the default callback?
  fut1, fut2 = context.map_query(query, callback, options)
  res1, res2 = yield fut1, fut2
  assert len(res1) == res2
  if res1:
    account = res1[0]
    assert isinstance(account, Account)
    raise tasks.Return(account)

  if not create:
    return  # None

  account = Account(key=model.Key(flat=['Account', user.user_id()]),
                    email=user.email(),
                    userid=user.user_id())
  # Write to datastore asynchronously.
  context.put(account)  # Don't wait
  raise tasks.Return(account)

def GetAccountByUser(context, user, create=False):
  fut = AsyncGetAccountByUser(context, user, create)
  return fut.get_result()

class HomePage(webapp.RequestHandler):

  def get(self):
    ctx = context.Context()
    user = users.get_current_user()
    email = None
    if user is not None:
      email = user.email()
    values = {'email': email,
              'login': users.create_login_url('/'),
              'logout': users.create_logout_url('/'),
              }
    self.response.out.write(HOME_PAGE % values)
    self.rest = []
    order = datastore_query.PropertyOrder(
      'when',
      datastore_query.PropertyOrder.DESCENDING)
    query = datastore_query.Query(kind=Message.GetKind(), order=order)
    options = datastore_query.QueryOptions(batch_size=13, limit=50)
    self.todo = []
    fut1, fut2 = ctx.map_query(query, self._result_callback, options)
    if user is not None:
      GetAccountByUser(ctx, user)  # XXX unused?
    WaitForRpcs()
    fut1.check_success()
    fut2.check_success()
    while self.todo:
      self._flush_todo()
      WaitForRpcs()
    self.rest.sort()
    for key, text in self.rest:
      self.response.out.write(text)

  def _result_callback(self, result):
    # Callback called for each query result.  Updates self.todo.
    if result.userid is not None:
      key = model.Key(flat=['Account', result.userid])
      self.todo.append((key, result))
      if len(self.todo) >= 7:
        self._flush_todo()
    else:
      self.rest.append((-result.when,
                        'Anonymous / %s &mdash; %s<br>' %
                        (time.ctime(result.when),
                         cgi.escape(result.body))))

  def _flush_todo(self):
    logging.info('flushing %d todo entries', len(self.todo))
    entities = []
    keys = []
    for key, entity in self.todo:
      keys.append(key)
      entities.append(entity)
    del self.todo[:len(keys)]
    def AccountsCallBack(rpc):  # Closure over entities.
      accounts = rpc.get_result()
      uidmap = {}
      for account in accounts:
        if account is not None:
          uidmap[account.userid] = account
      for result in entities:
        account = uidmap.get(result.userid)
        if account is None:
          author = 'Withdrawn'
        else:
          author = account.email
        self.rest.append((-result.when,
                          '%s / %s &mdash; %s<br>' %
                          (cgi.escape(author),
                           time.ctime(result.when),
                           cgi.escape(result.body))))
    rpc = model.conn.async_get(
      datastore_rpc.Configuration(on_completion=AccountsCallBack),
      keys)
    eventloop.queue_rpc(rpc, rpc.check_success)

  def post(self):
    ctx = context.Context()
    body = self.request.get('body')
    if not body.strip():
      self.redirect('/')
      return
    user = users.get_current_user()
    logging.info('body=%.100r', body)
    body = body.rstrip()
    if body:
      msg = Message()
      msg.body = body
      msg.when = int(time.time())
      if user is not None:
        msg.userid = user.user_id()
      # Write to datastore asynchronously.
      rpc = model.conn.async_put(None, [msg])
      eventloop.queue_rpc(rpc, rpc.check_success)
      if user is not None:
        # Check that the account exists and create it if necessary.
        GetAccountByUser(ctx, user, create=True)
    self.redirect('/')
    WaitForRpcs()

class AccountPage(webapp.RequestHandler):

  def get(self):
    ctx = context.Context()
    user = users.get_current_user()
    if user is None:
      self.redirect(users.create_login_url('/account'))
      return
    email = user.email()
    account = GetAccountByUser(ctx, user)
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
    ctx = context.Context()
    user = users.get_current_user()
    if user is None:
      self.redirect(users.create_login_url('/account'))
      return
    if self.request.get('delete'):
      account = GetAccountByUser(ctx, user)
      if account is not None:
        ctx.delete(account.key).check_success()
      self.redirect('/account')
      return
    account = GetAccountByUser(ctx, user, create=True)
    assert isinstance(account, Account), account
    WaitForRpcs()
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
