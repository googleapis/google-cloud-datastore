"""A simple guestbook app to test parts of NDB end-to-end."""

import cgi
import logging
import os
import re
import sys
import time

from google.appengine.api import urlfetch
from google.appengine.api import users
from google.appengine.datastore import entity_pb
from google.appengine.ext import webapp
from google.appengine.ext.webapp import util

from google.appengine.datastore import datastore_query
from google.appengine.datastore import datastore_rpc

from ndb import context
from ndb import eventloop
from ndb import model
from ndb import tasklets

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


def get_account(userid):
  """Return a Future for an Account."""
  return account_key(userid).get_async()


@tasklets.tasklet
def get_nickname(userid):
  """Return a Future for a nickname from an account."""
  account = yield get_account(userid)
  if not account:
    nickname = 'Unregistered'
  else:
    nickname = account.nickname or account.email
  raise tasklets.Return(nickname)


class HomePage(webapp.RequestHandler):

  @context.toplevel
  def get(self):
    nickname = 'Anonymous'
    user = users.get_current_user()
    if user is not None:
      nickname = yield get_nickname(user.user_id())
    values = {'nickname': nickname,
              'login': users.create_login_url('/'),
              'logout': users.create_logout_url('/'),
              }
    self.response.out.write(HOME_PAGE % values)
    qry, options = self._make_query()
    pairs = yield qry.map_async(self._hp_callback, options=options)
    for key, text in pairs:
      self.response.out.write(text)

  def _make_query(self):
    qry = Message.query().order(-Message.when)
    options = datastore_query.QueryOptions(batch_size=13, limit=43)
    return qry, options

  @tasklets.tasklet
  def _hp_callback(self, message):
    nickname = 'Anonymous'
    if message.userid:
      nickname = yield get_nickname(message.userid)
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
      summary = yield key.get_async()
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
          yield summary.put_async()
      hover = ''
      if summary.title:
        hover = ' title="%s"' % summary.title
      escbody = (cgi.escape(pre) +
                 '<a%s href="%s">' % (hover, cgi.escape(url)) +
                 cgi.escape(url) + '</a>' + cgi.escape(post))
    text = '%s - %s - %s<br>' % (cgi.escape(nickname),
                                 time.ctime(message.when),
                                 escbody)
    raise tasklets.Return((-message.when, text))

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
      yield message.put_async()
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
    account, nickname = yield (get_account(user.user_id()),
                               get_nickname(user.user_id()))
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
    @tasklets.tasklet
    def helper():
      user = users.get_current_user()
      if not user:
        self.redirect(users.create_login_url('/account'))
        return
      account = yield get_account(user.user_id())
      if self.request.get('delete'):
        if account:
          yield account.key.delete_async()
        self.redirect('/account')
        return
      if not account:
        account = Account(key=account_key(user.user_id()),
                          email=user.email(), userid=user.user_id())
      nickname = self.request.get('nickname')
      if nickname:
        account.nickname = nickname
      yield account.put_async()
      self.redirect('/account')
    yield model.transaction_async(helper)


urls = [
  ('/', HomePage),
  ('/account', AccountPage),
  ]

app = webapp.WSGIApplication(urls)


def main():
  util.run_wsgi_app(app)


if __name__ == '__main__':
  main()
