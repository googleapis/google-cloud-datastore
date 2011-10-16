"""Remove trailing whitespace from files in current path and sub directories."""

import os, glob

def scanpath(path):
  for filepath in glob.glob(os.path.join(path, '*')):
    if os.path.isdir(filepath):
      scanpath(filepath)
    else:
      trimwhitespace(filepath)

def trimwhitespace(filepath):
  if filepath.endswith('.pyc') or filepath.endswith('~'):
    return
  handle = open(filepath, 'rb')
  stripped = ''
  flag = False
  for line in handle.readlines():
    stripped_line = line.rstrip() + '\n'
    if line != stripped_line:
      flag = True
    stripped += stripped_line
  handle.close()
  if flag:
    print('FIX: %s' % filepath)
    overwritefile(filepath, stripped)
  else:
    print('OK: %s' % filepath)

def overwritefile(filepath, contents):
  handle = open(filepath, 'wb')
  handle.write(contents)
  handle.close()

if __name__ == '__main__':
  scanpath('.')
