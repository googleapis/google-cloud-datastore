"""Unused PEP 380 emulation."""

import sys

from ndb.tasklets import is_generator, Return, get_return_value

def gwrap(func):
  """Decorator to emulate PEP 380 behavior.

  Inside a generator function wrapped in @gwrap, 'yield g', where g is
  a generator object, is equivalent to 'for x in g: yield x', except
  that 'yield g' can also return a value, and that value is whatever g
  passed as the argument to StopIteration when it stopped.

  The idea is that once PEP 380 is implemented, you can drop @gwrap,
  replace 'yield g' with 'yield from g' and 'raise Return(x)' with
  'return x', and everything will work exactly the same as before.

  NOTE: This is not quite the same as @tasklet, which offers event loop
  integration.
  """
  def gwrap_wrapper(*args, **kwds):
    """The wrapper function that is actually returned by gwrap()."""
    # Call the wrapped function.  If it is a generator function, this
    # returns a generator object.  If it raises an exception, let it
    # percolate up unchanged.
    gen = func(*args, **kwds)

    # If that didn't return a generator object, pretend it was a
    # generator that yielded no items.
    if not is_generator(gen):
      if gen is None:
        return  # Don't bother creating a Return() if it returned None.
      raise Return(gen)

    # If this is an immediately recursive call to gwrap_wrapper(),
    # yield out the generator to let the outer call handle things.
    if sys._getframe(1).f_code is sys._getframe(0).f_code:
      result = yield gen
      raise Return(result)

    # The following while loop elaborates on "for x in g: yield x":
    #
    # 1. Pass values or exceptions received from yield back into g.
    #    That's just part of a truly transparent wrapper for a generator.
    #
    # 2. When x is a generator, loop over it in turn, using a stack to
    #    avoid excessive recursion.  That's part of emulating PEP 380
    #    so that "yield g" is interpreted as "yield from g" (which
    #    roughly means "for x in g: yield x").
    #
    # 3. Pass values and exceptions up that stack.  This is where my
    #    brain keeps hurting.

    to_send = None  # What to send into the top generator.
    to_throw = None  # What to throw into the top generator.
    stack = [gen]  # Stack of generators.
    while stack:
      # Throw or send something into the current generator.
      gen = stack[-1]
      try:
        if to_throw is not None:
          gen.throw(to_throw)
        else:
          to_yield = gen.send(to_send)

      except StopIteration, err:
        # The generator has no more items.  Pop it off the stack.
        stack.pop()
        if not stack:
          raise  # We're done.

        # Prepare to send this value into the next generator on the stack.
        to_send = get_return_value(err)
        to_throw = None
        continue

      except Exception, err:
        # The generator raised an exception.  Pop it off the stack.
        stack.pop()
        if not stack:
          raise  # We're done.

        # Prepare to throw this exception into the next generator on the stack.
        to_send = None
        to_throw = err
        continue

      else:
        # The generator yielded a value.
        to_throw = None
        to_send = None
        if not is_generator(to_yield):
          # It yielded some plain value.  Yield this outwards.
          # Whatever our yield returns or raises will be sent or thrown
          # into the current generator.
          # TODO: support "yield Return(...)" as an alternative for
          # "raise Return(...)"?  Monocle users would like that.
          try:
            # If the yield returns a value, prepare to send that into
            # the current generator.
            to_send = yield to_yield
          except (Exception, GeneratorExit), err:
            # The yield raised an exception.  Prepare to throw it into
            # the current generator.  (GeneratorExit sometimes inherits
            # from BaseException, but we do want to catch it.)
            to_throw = err

        else:
          # It yielded another generator.  Push it onto the stack.
          # Note that this new generator is (assumed to be) in the
          # "initial" state for generators, meaning that it hasn't
          # executed any code in the generator function's body yet.
          # In this state we may only call gen.next() or gen.send(None),
          # so it's a good thing that to_send and to_throw are None.
          stack.append(to_yield)

  return gwrap_wrapper

def gclose(gen):
  """Substitute for gen.close() that returns a value."""
  # TODO: Tweak the result of gwrap() to return an object that defines
  # a close method that works this way?
  assert is_generator(gen), '%r is not a generator' % g
  # Throw GeneratorExit until it obeys.
  while True:
    try:
      gen.throw(GeneratorExit)
    except StopIteration, err:
      return get_return_value(err)
    except GeneratorExit:
      return None
    # Note: other exceptions are passed out untouched.
