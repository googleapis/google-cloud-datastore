"""Temporary monkeypatch (until 1.4.0) for apiproxy_stub_map.py."""

from google.appengine.api import apiproxy_stub_map
from google.appengine.runtime import apiproxy_errors

class UserRPC(apiproxy_stub_map.UserRPC):

  @classmethod
  def wait_any(cls, rpcs):
    assert iter(rpcs) is not rpcs, 'rpcs must be a collection, not an iterator'
    finished, running = cls.__check_one(rpcs)
    if finished is not None:
      return finished
    if running is None:
      return None
    try:
      cls.__local.may_interrupt_wait = True
      try:
        running.__rpc.Wait()
      except apiproxy_errors.InterruptedError, err:
        err.rpc._RPC__exception = None
        err.rpc._RPC__traceback = None
    finally:
      cls.__local.may_interrupt_wait = False
    finished, runnning = cls.__check_one(rpcs)
    return finished

apiproxy_stub_map.UserRPC.wait_any = UserRPC.__dict__['wait_any']
