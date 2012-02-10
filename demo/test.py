"""Quick hack to (a) demo the synchronous APIs and (b) dump all records."""

import time

import ndb

from demo.main import Message, Account, account_key


class LogRecord(ndb.Model):
  timestamp = ndb.FloatProperty()


@ndb.toplevel
def main():
  print 'Content-type: text/plain'
  print
  qry = Message.query().order(-Message.when)
  for msg in qry:
    print time.ctime(msg.when or 0), repr(msg.body)
    if msg.userid is None:
      print '  * Anonymous'
    else:
      act = account_key(msg.userid).get()
      if act is None:
        print '  * Bad account', msg.userid
      else:
        print '  * Account', act.nickname, act.email, msg.userid
  log = LogRecord(timestamp=time.time())
  log.put_async()


if __name__ == '__main__':
  main()
