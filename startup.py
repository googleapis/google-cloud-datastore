# Startup file for interactive prompt, used by "make python".

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
