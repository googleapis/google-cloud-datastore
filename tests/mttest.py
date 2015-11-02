#
# Copyright 2008 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A torture test to ferret out problems with multi-threading."""

import sys
import threading

from ndb import tasklets
from ndb import eventloop


def main():
  ##sys.stdout.write('_State.__bases__ = %r\n' % (eventloop._State.__bases__,))
  num = 10
  try:
    num = int(sys.argv[1])
  except Exception:
    pass
  threads = []
  for i in range(num):
    t = threading.Thread(target=one_thread, args=(i, num,))
    t.start()
    threads.append(t)
  for t in threads:
    t.join()


@tasklets.toplevel
def one_thread(i, num):
  ##sys.stdout.write('eventloop = 0x%x\n' % id(eventloop.get_event_loop()))
  x = yield fibonacci(num)
  sys.stdout.write('%d: %d --> %d\n' % (i, num, x))


@tasklets.tasklet
def fibonacci(n):
  """A recursive Fibonacci to exercise task switching."""
  if n <= 1:
    raise tasklets.Return(n)
  a = yield fibonacci(n - 1)
  b = yield fibonacci(n - 2)
  raise tasklets.Return(a + b)


if __name__ == '__main__':
  main()
