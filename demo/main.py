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
def MapQuery(query, entity_callback, connection, options=None):
  count = 0
  rpc = query.run_async(connection, options)
  while rpc is not None:
    batch = yield rpc
    rpc = batch.next_batch_async(options)
    logging.info('batch with %d results, next_rpc=%r', len(batch.results), rpc)
    for entity in batch.results:
      try:
        entity_callback(entity)
      except (StopIteration, GeneratorExit):
        raise  # Don't log these
      except Exception:
        logging.exception('entity callback %s raised', entity_callback.__name__)
        raise
    count += len(batch.results)
  raise tasks.Return(count)

def MapQueryToGenerator(query, generator, connection, options=None):
  # TODO: Make this into a task.
  assert tasks.is_generator(generator), '%r is not a generator' % generator
  # "Prime" the generator.  (TODO: does PEP 380 explain this?)
  generator.next()
  our_future = tasks.Future()
  def wrap_send(val):
    try:
      value = generator.send(val)
    except StopIteration, err:
      result = tasks.get_return_value(err)
      our_future.set_result(result)
      raise
    except Exception:
      t, v, tb = sys.exc_info()
      our_future.set_exception(v, tb)
      raise
    else:
      return value
  map_future = MapQuery(query, wrap_send, connection, options)

  def callback(fut):
    # When map_future completes, extract a return value from the
    # generator and set it as our own future's result.
    assert fut is map_future, (fut, map_future)
    if fut.get_exception():
      logging.exception('map_future: raised %r', fut)
    # TODO: Use pep380.gclose().
    try:
      value = generator.throw(GeneratorExit)
    except StopIteration, err:
      value = tasks.get_return_value(err)
      our_future.set_result(value)
    except GeneratorExit:
      if not our_future.done():
        our_future.set_result(None)
    except Exception, err:
      _, _, tb = sys.exc_info()
      our_future.set_exception(err, tb)
    else:
      our_future.set_exception(RuntimeError(
        'Throwing GeneratorExit into it did not stop the generator'))

  map_future.add_done_callback(callback)
  return our_future

@tasks.task
def AsyncGetAccountByUser(user, create=False):
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

  hit_future = tasks.Future()

  def result_callback(result):
    assert isinstance(result, Account), '%r is not an Account [1]' % result
    if not hit_future.done():
      hit_future.set_result(result)

  def accumulator():
    while True:
      result = yield
      assert isinstance(result, Account), '%r is not an Account [2]' % result
      raise tasks.Return(result)

  f = MapQueryToGenerator(query, accumulator(), model.conn)
  result = yield f

  if result is not None:
    raise tasks.Return(result)

  if not create:
    return  # None

  account = Account(key=model.Key(flat=['Account', user.user_id()]),
                    email=user.email(),
                    userid=user.user_id())
  # Write to datastore asynchronously.
  rpc = model.conn.async_put(None, [account])
  eventloop.queue_rpc(rpc, rpc.check_success)
  raise tasks.Return(account)

def GetAccountByUser(user, create=False):
  fut = AsyncGetAccountByUser(user, create)
  return fut.get_result()

class HomePage(webapp.RequestHandler):

  def get(self):
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
    fut = MapQuery(query, self._result_callback, model.conn, options)

    if user is not None:
      GetAccountByUser(user)
    WaitForRpcs()
    fut.check_success()
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
        GetAccountByUser(user, create=True)
    self.redirect('/')
    WaitForRpcs()

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
