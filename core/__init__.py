# Offer forward compatible imports of datastore_rpc and datastore_query.

import logging
import sys

try:
  from google.appengine.datastore import datastore_rpc
  from google.appengine.datastore import datastore_query
  logging.info('Imported official google datastore_{rpc,query}')
except ImportError:
  logging.warning('Importing local datastore_{rpc,query}')
  from . import datastore_rpc
  from . import datastore_query
  from . import monkey
