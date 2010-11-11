import os
import sys

def wrapping(wrapped):
  # A decorator to decorate a decorator's wrapper.  Following the lead
  # of Twisted and Monocle, this is supposed to make debugging heavily
  # decorated code easier.  We'll see...
  # TODO: Evaluate; so far it hasn't helped (nor hurt).
  def wrapping_wrapper(wrapper):
    wrapper.__name__ = wrapped.__name__
    wrapper.__doc__ = wrapped.__doc__
    wrapper.__dict__.update(wrapped.__dict__)
    return wrapper
  return wrapping_wrapper

def get_stack(limit=10):
  # Return a list of strings showing where the current frame was called.
  frame = sys._getframe(1)  # Always skip get_stack() itself.
  lines = []
  while len(lines) < limit and frame is not None:
    locals = frame.f_locals
    ndb_debug = locals.get('__ndb_debug__')
    if ndb_debug != 'SKIP':
      line = code_info(frame.f_code, frame.f_lineno)
      if ndb_debug is not None:
        line += ' # ' + str(ndb_debug)
      lines.append(line)
    frame = frame.f_back
  return lines

def func_info(func, lineno=None):
  code = func.func_code
  return code_info(code, lineno)

def code_info(code, lineno=None):
  funcname = code.co_name
  # TODO: Be cleverer about stripping filename,
  # e.g. strip based on sys.path.
  filename = os.path.basename(code.co_filename)
  if lineno is None:
    lineno = code.co_firstlineno
  return '%s(%s:%s)' % (funcname, filename, lineno)
