"""Tests for pep380.py."""

import unittest

from ndb import pep380

class PEP380Tests(unittest.TestCase):
  """Test cases to verify the equivalence of yielding a generator to PEP 380.

  E.g. in a pre-PEP-380 world, this:

    @gwrap
    def g1():
      x = yield g2(0, 10)
      y = yield g2(5, 20)
      yield (x, y)

    @gwrap  # Optional
    def g2(a, b):
      for i in range(a, b):
        yield i
      raise Return(b - a)

    def main():
      assert list(g1()) == range(0, 10) + range(5, 20) + [(10, 15)]

  should be equivalent to this in a PEP-380 world:

    def g1():
      x = yield from g2(0, 10)
      y = yield from g2(5, 20)
      yield x, y

    def g2(a, b):
      yield from range(a, b)  # Maybe?
      return b - a

    def main():
      assert list(g1()) == range(0, 10) + range(5, 20) + [(10, 15)]
  """

  def testBasics(self):
    @pep380.gwrap
    def g1(a, b, c, d):
      x = yield g2(a, b)
      y = yield g2(c, d)
      yield (x, y)
    @pep380.gwrap
    def g2(a, b):
      for i in range(a, b):
        yield i
      raise pep380.Return(b - a)
    actual = []
    for val in g1(0, 3, 5, 7):
      actual.append(val)
    expected = [0, 1, 2,  5, 6, (3, 2)]
    self.assertEqual(actual, expected)

  def testGClose(self):
    @pep380.gwrap
    def foo():
      total = 0
      try:
        while True:
          total += (yield)
      except GeneratorExit:
        raise pep380.Return(total)
    gen = foo()
    gen.next()
    gen.send(3)
    gen.send(2)
    val = pep380.gclose(gen)
    self.assertEqual(val, 5)
    gen = foo()
    gen.next()
    gen.send(3)
    val = pep380.gclose(gen)
    self.assertEqual(val, 3)

  def testGClose_Vanilla(self):
    def vanilla():
      yield 1
    v = vanilla()
    self.assertEqual(pep380.gclose(v), None)
    v = vanilla()
    v.next()
    self.assertEqual(pep380.gclose(v), None)
    v = vanilla()
    v.next()
    self.assertRaises(StopIteration, v.next)
    self.assertEqual(pep380.gclose(v), None)

  def testGClose_KeepTrying(self):
    def hard_to_get():
      for i in range(5):
        try:
          yield i
        except GeneratorExit:
          continue
      raise pep380.Return(42)
    gen = hard_to_get()
    self.assertEqual(gen.next(), 0)
    self.assertEqual(gen.next(), 1)
    self.assertEqual(pep380.gclose(gen), 42)

def main():
  unittest.main()

if __name__ == '__main__':
  main()
