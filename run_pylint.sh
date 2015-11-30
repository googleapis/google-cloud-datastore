#!/bin/bash

# TODO(pcostello): If we move tests to their own directory we can avoid
# naming these explicitly.
TESTS=blobstore_test.py,context_test.py,eventloop_test.py,google_test_imports.py,key_test.py,metadata_test.py,model_test.py,msgprop_test.py,polymodel_test.py,prospective_search_test.py,query_test.py,rpc_test.py,stats_test.py,tasklets_test.py,test_utils.py,local_cloud_datastore_factory.py

NON_TESTS=blobstore.py,context.py,django_middleware.py,eventloop.py,google_imports.py,__init__.py,key.py,metadata.py,model.py,msgprop.py,polymodel.py,prospective_search.py,query.py,stats.py,tasklets.py

pylint --ignore=${NON_TESTS} --disable=invalid-name,W,I ndb
pylint --ignore=${TESTS} --disable=W,I,no-name-in-module ndb
