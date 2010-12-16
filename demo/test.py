"""Quick hack to (a) demo the synchronous APIs and (b) dump all records."""

import time

from demo.main import model, context, Message, Account, account_key


def setup():
  ctx = context.Context()
  context.set_default_context(ctx)


def main():
  conn = setup()
  print 'Content-type: text/plain'
  print
  qry = Message.all().order_by_desc('when')
  for msg in qry:
    print msg.userid, time.ctime(msg.when), repr(msg.body)
    if msg.userid is not None:
      act = account_key(msg.userid).get()
      if act is None:
        print '  * Bad account'
      else:
        print '  * Account', act.nickname, act.email


if __name__ == '__main__':
  main()
