# Startup file for interactive prompt, used by "make python".

from ndb import utils
utils.tweak_logging()

import os

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_file_stub
from google.appengine.api import memcache
from google.appengine.api.memcache import memcache_stub

from ndb.model import *
from ndb.query import *

apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
ds_stub = datastore_file_stub.DatastoreFileStub('_', None)
apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', ds_stub)
mc_stub = memcache_stub.MemcacheServiceStub()
apiproxy_stub_map.apiproxy.RegisterStub('memcache', mc_stub)
os.environ['APPLICATION_ID'] = '_'

class Employee(Model):
  name = StringProperty()
  age = IntegerProperty()
  rank = IntegerProperty()

  @classmethod
  def demographic(cls, min_age, max_age):
    return cls.query().filter(AND(cls.age >= min_age, cls.age <= max_age))

  @classmethod
  def ranked(cls, rank):
    return cls.query(cls.rank == rank).order(cls.age)

class Manager(Employee):
  report = StructuredProperty(Employee, repeated=True)

reports = []
for (name, age, rank) in [('Joe', 21, 1), ('Jim', 30, 2), ('Jane', 23, 1)]:
  emp = Employee(name=name, age=age, rank=rank)
  reports.append(emp)
f1 = put_multi_async(reports)

boss = Manager(name='Fred', age=42, rank=4, report=reports)
f2 = boss.put_async()

f2.get_result()
for f in f1:
  f.get_result()
