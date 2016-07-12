#
# Copyright 2008 The ndb Authors. All Rights Reserved.
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

"""Like google_imports.py, but for use by tests.

This imports the testbed package and some stubs.
"""

from . import google_imports

if google_imports.normal_environment:
  from google.appengine.datastore import cloud_datastore_v1_remote_stub
  from google.appengine.datastore import datastore_stub_util
  from google.appengine.ext import testbed
  import unittest
else:
  from google3.apphosting.datastore import cloud_datastore_v1_remote_stub
  from google3.apphosting.datastore import datastore_stub_util
  from google3.apphosting.ext import testbed
  from google3.testing.pybase import googletest as unittest

import unittest as real_unittest
