# Helper to set a breakpoint in App Engine.  Requires Python >= 2.5.

import os
import pdb

def BREAKPOINT():
  os_mod = os.open.func_globals['os']
  os_open = os_mod.open
  os_fdopen = os_mod.fdopen
  tty = '/dev/tty'
  stdin_fd = os_open(tty, 0)
  stdout_fd = os_open(tty, 1)
  stdin = os_fdopen(stdin_fd, 'r')
  stdout = os_fdopen(stdout_fd, 'w')
  p = pdb.Pdb(None, stdin, stdout)
  p.set_trace()
