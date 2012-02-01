import startup

from ndb import *

while True:
  line = raw_input('gql> ')
  if line == '?':
    raise
  line = line.strip()
  if not line:
    continue
  try:
    q = gql(line)
  except Exception, err:
    print [1], err.__class__.__name__ + ':', err
    continue
  pdict = q.parameters
  args = []
  kwds = {}
  if pdict:
    pos = [p for p in sorted(pdict) if isinstance(p, (int, long))]
    if pos != range(1, 1 + len(pos)):
      print [2], 'Sorry, positional parameters are out of order.'
      continue
    err = None
    for p in sorted(pdict):
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
    try:
      q = q.bind(*args, **kwds)
    except Exception, err:
      print [4], err.__class__.__name__ + ':', err
      continue
  try:
    results = list(q)
  except Exception, err:
    print [5], err.__class__.__name__ + ':', err
    continue
  for i, result in enumerate(results):
    print '%2d.' % (i+1), result
