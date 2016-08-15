googledatastore
===============

Google Cloud Datastore Protocol Buffer client.

Google Cloud Datastore is a fully managed, schemaless, non-relational
database accessible through Google APIs infrastructure. It provides
a rich set of query capabilities, supports atomic transactions, and
automatically scales up and down in response to load.

Usage
-----
.. code-block:: pycon

    >>> import googledatastore as datastore
    >>> datastore.set_options(project_id='project-id')
    >>> req = datastore.BeginTransactionRequest()
    >>> datastore.begin_transaction(req)
    <google.datastore.v1.datastore-pb2.BeginTransactionResponse ...>


Installation
------------
.. code-block:: bash

    $ pip install googledatastore

Documentation
-------------
https://cloud.google.com/datastore/docs/

Contribute
----------
https://github.com/GoogleCloudPlatform/google-cloud-datastore

