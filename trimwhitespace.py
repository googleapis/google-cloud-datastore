"""Remove trailing whitespace from files in current path and sub directories."""

import os, glob

def scanpath(path):
  for filepath in glob.glob(os.path.join(path, '*')):
    if os.path.isdir(filepath):
      scanpath(filepath)
    else:
      trimwhitespace(filepath)

def trimwhitespace(filepath):
  handle = open(filepath, 'r')
  stripped = ''
  flag = False
  for line in handle.readlines():
    stripped_line = line.rstrip() + '\n'
    if line != stripped_line:
      flag = True
    stripped += stripped_line
  handle.close()
  if flag:
    overwritefile(filepath, stripped)

def overwritefile(filepath, contents):
  handle = open(filepath, 'w')
  handle.write(contents)
  handle.close()

if __name__ == '__main__':
  scanpath('.')
