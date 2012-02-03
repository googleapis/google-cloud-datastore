import readline
import startup

from ndb import *

def repl():
  while True:
    line = raw_input('gql> ')
    if line == '/':
      raise
    line = line.strip()
    if not line:
      continue

    try:
      q = gql(line)
    except Exception, err:
      print [1], err.__class__.__name__ + ':', err
      continue

    used = q.analyze()
    if used:
      positionals = [p for p in used if isinstance(p, (int, long))]
      if positionals != range(1, 1 + len(positionals)):
        print 'Not all positional arguments are used'
        continue
      args = []
      kwds = {}
      err = None
      for p in used:
        try:
          value = input('Parameter :%s = ' % p)
        except Exception, err:
          print [3], err.__class__.__name__ + ':', err
          break
        if isinstance(p, (int, long)):
          args.append(value)
        else:
          kwds[p] = value
      if err:
        continue
      q = q._bind(args, kwds)

    try:
      results = list(q)
    except Exception, err:
      print [5], err.__class__.__name__ + ':', err
      continue

    for i, result in enumerate(results):
      print '%2d.' % (i+1), result

def main():
  try:
    repl()
  except EOFError:
    print

if __name__ == '__main__':
  main()
