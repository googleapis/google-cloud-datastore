"""A simple guestbook app to test parts of NDB end-to-end."""

import cgi
import logging
import re
import sys
import time

from google.appengine.api import urlfetch
from google.appengine.api import users
from google.appengine.datastore import entity_pb
from google.appengine.ext import webapp
from google.appengine.ext.webapp import util

from core import datastore_query
from core import datastore_rpc
from core import monkey

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
  Nickname: <a href="/account">%(nickname)s</a> |
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
  Nickname: <a href="/account">%(nickname)s</a> |
  <a href="%(logout)s">logout</a>

  <form method=POST action=/account>
    <!-- TODO: XSRF protection -->
    Email: %(email)s<br>
    New nickname:
    <input type=text name=nickname size=20 value=%(proposed_nickname)s><br>
    <input type=submit name=%(action)s value="%(action)s Account">
    <input type=submit name=delete value="Delete Account">
    <a href=/>back to home page</a>
  </form>
</body>
"""


class Account(model.Model):
  """User account."""

  email = model.StringProperty()
  userid = model.StringProperty()
  nickname = model.StringProperty()


class Message(model.Model):
  """Guestbook message."""

  body = model.StringProperty()
  when = model.FloatProperty()
  userid = model.StringProperty()


class UrlSummary(model.Model):
  """Metadata about a URL."""

  MAX_AGE = 60

  url = model.StringProperty()
  title = model.StringProperty()
  when = model.FloatProperty()


def account_key(userid):
  return model.Key(flat=['Account', userid])


def get_account(ctx, userid):
  """Return a Future for an Account."""
  return ctx.get(account_key(userid))


@tasks.task
def get_nickname(ctx, userid):
  """Return a Future for a nickname from an account."""
  account = yield get_account(ctx, userid)
  if not account:
    nickname = 'Unregistered'
  else:
    nickname = account.nickname or account.email
  raise tasks.Return(nickname)


class HomePage(webapp.RequestHandler):

  @context.toplevel
  def get(self):
    nickname = 'Anonymous'
    user = users.get_current_user()
    if user is not None:
      nickname = yield get_nickname(self.ctx, user.user_id())
    values = {'nickname': nickname,
              'login': users.create_login_url('/'),
              'logout': users.create_logout_url('/'),
              }
    self.response.out.write(HOME_PAGE % values)
    query, options = self._make_query()
    futs, count = yield self.ctx.map_query(query, self._hp_callback, options)
    assert len(futs) == count
    pairs = [f.get_result() for f in futs]
    pairs.sort()
    for key, text in pairs:
      self.response.out.write(text)

  def _make_query(self):
    order = datastore_query.PropertyOrder(
      'when',
      datastore_query.PropertyOrder.DESCENDING)
    query = datastore_query.Query(kind=Message.GetKind(), order=order)
    options = datastore_query.QueryOptions(batch_size=13, limit=43)
    return query, options

  @tasks.task
  def _hp_callback(self, message):
    nickname = 'Anonymous'
    if message.userid:
      nickname = yield get_nickname(self.ctx, message.userid)
    # Check if there's an URL.
    body = message.body
    m = re.search(r'(?i)\bhttps?://\S+[^\s.,;\]\}\)]', body)
    if not m:
      escbody = cgi.escape(body)
    else:
      url = m.group()
      pre = body[:m.start()]
      post = body[m.end():]
      title = ''
      key = model.Key(flat=[UrlSummary.GetKind(), url])
      summary = yield self.ctx.get(key)
      if not summary or summary.when < time.time() - UrlSummary.MAX_AGE:
        rpc = urlfetch.create_rpc(deadline=0.5)
        urlfetch.make_fetch_call(rpc, url,allow_truncated=True)
        t0 = time.time()
        result = yield rpc
        t1 = time.time()
        logging.warning('url=%r, status=%r, dt=%.3f',
                        url, result.status_code, t1-t0)
        if result.status_code == 200:
          bodytext = result.content
          m = re.search(r'(?i)<title>([^<]+)</title>', bodytext)
          if m:
            title = m.group(1).strip()
          summary = UrlSummary(key=key, url=url, title=title,
                               when=time.time())
          yield self.ctx.put(summary)
      hover = ''
      if summary.title:
        hover = ' title="%s"' % summary.title
      escbody = (cgi.escape(pre) +
                 '<a%s href="%s">' % (hover, cgi.escape(url)) +
                 cgi.escape(url) + '</a>' + cgi.escape(post))
    text = '%s - %s - %s<br>' % (cgi.escape(nickname),
                                 time.ctime(message.when),
                                 escbody)
    raise tasks.Return((-message.when, text))

  @context.toplevel
  def post(self):
    # TODO: XSRF protection.
    body = self.request.get('body', '').strip()
    if body:
      userid = None
      user = users.get_current_user()
      if user:
        userid = user.user_id()
      message = Message(body=body, when=time.time(), userid=userid)
      yield self.ctx.put(message)  # Synchronous.
    self.redirect('/')


class AccountPage(webapp.RequestHandler):

  @context.toplevel
  def get(self):
    user = users.get_current_user()
    if not user:
      self.redirect(users.create_login_url('/account'))
      return
    email = user.email()
    action = 'Create'
    account, nickname = yield (get_account(self.ctx, user.user_id()),
                               get_nickname(self.ctx, user.user_id()))
    if account is not None:
      action = 'Update'
    if account:
      proposed_nickname = account.nickname or account.email
    else:
      proposed_nickname = email
    values = {'email': email,
              'nickname': nickname,
              'proposed_nickname': proposed_nickname,
              'login': users.create_login_url('/'),
              'logout': users.create_logout_url('/'),
              'action': action,
              }
    self.response.out.write(ACCOUNT_PAGE % values)

  @context.toplevel
  def post(self):
    # TODO: XSRF protection.
    user = users.get_current_user()
    if not user:
      self.redirect(users.create_login_url('/account'))
      return
    account = yield get_account(self.ctx, user.user_id())
    if self.request.get('delete'):
      if account:
        yield self.ctx.delete(account.key)
      self.redirect('/account')
      return
    if not account:
      account = Account(key=account_key(user.user_id()),
                        email=user.email(), userid=user.user_id())
    nickname = self.request.get('nickname')
    if nickname:
      account.nickname = nickname
    yield self.ctx.put(account)
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
