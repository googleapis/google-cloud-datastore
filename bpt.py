# Helper to set a breakpoint in App Engine.  Requires Python >= 2.5.

import os
import pdb
import sys

class MyPdb(pdb.Pdb):

  def default(self, line):
    # Save/set + restore stdin/stdout around self.default() call.
    # (This is only needed for Python 2.5.)
    save_stdout = sys.stdout
    save_stdin = sys.stdin
    try:
      sys.stdin = self.stdin
      sys.stdout = self.stdout
      return pdb.Pdb.default(self, line)
    finally:
      sys.stdout = save_stdout
      sys.stdin = save_stdin

  def do_vars(self, arg):
    for name, value in sorted(self.curframe.f_locals.iteritems()):
      print >>self.stdout, name, '=', repr(value)
  do_v = do_vars


def BREAKPOINT():
  os_mod = os.open.func_globals['os']
  os_open = os_mod.open
  os_fdopen = os_mod.fdopen
  tty = '/dev/tty'
  stdin_fd = os_open(tty, 0)
  stdout_fd = os_open(tty, 1)
  stdin = os_fdopen(stdin_fd, 'r')
  stdout = os_fdopen(stdout_fd, 'w')
  p = MyPdb(None, stdin, stdout)
  p.set_trace(sys._getframe(1))
