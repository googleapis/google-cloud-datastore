googledatastore
===============

Google Cloud Datastore Protobuf Buffers client.

Google Cloud Datastore is a fully managed, schemaless, non-relational
datastore accessible through Google APIs infrastructure. It provides
a rich set of query capabilities, supports atomic transactions, and
automatically scales up and down in response to load.

Usage
-----
.. code-block:: pycon

    >>> import googledatastore as datastore
    >>> datastore.set_options(dataset='dataset-id')
    >>> req = datastore.BeginTransactionRequest()
    >>> datastore.begin_transaction(req)
    <datastore.datastore_v1_pb2.BeginTransactionResponse object at ...>
    

Installation
------------
.. code-block:: bash

    $ pip install googledatastore

Documentation
-------------
https://developers.google.com/datastore

Contribute
----------
https://github.com/GoogleCloudPlatform/google-cloud-datastore

