# Google Cloud Datastore

> Note: This repository contains low-level Java and Python client libraries for Google Cloud Datastore.
> For more idiomatic and usable client libraries in these languages, please visit the [Google Cloud Client Libraries for Java][26] and [Google Cloud Client Libraries for Python][27] repositories. You can also find the full list of supported client libraries in a variety of languages on the [Client Libraries page][24] of Cloud Datastore.

Cloud Datastore is a highly-scalable NoSQL database for your applications. Cloud Datastore automatically handles sharding and replication, providing you with a highly available and durable database that scales automatically to handle your applications' load. Cloud Datastore provides a myriad of capabilities such as ACID transactions, SQL-like queries, indexes and much more. For more information, see the [Cloud Datastore documentation][4].

This repository contains clients that are deliberately low-level and map directly to the underlying Datastore RPC model. They're designed to provide more flexibility to developers and higher level library implementers.

- [Python Proto Client Library and Samples][9]
- [Java Proto Client Library and Samples][10]
- [RPC API Reference][6]

## Samples

### Proto

- [Python][1]
- [Java][2]

## Client Libraries

You can learn more about client libraries for Cloud Datastore [here][24].

- [Python][18]:

```
pip install googledatastore
```

- Java (Maven):

```
<dependency>
  <groupId>com.google.cloud.datastore</groupId>
  <artifactId>datastore-v1-protos</artifactId>
  <version>1.5.0</version>
</dependency>
<dependency>
  <groupId>com.google.cloud.datastore</groupId>
  <artifactId>datastore-v1-proto-client</artifactId>
  <version>1.5.0</version>
</dependency>
```

## Documentation

For more information, see the [Cloud Datastore documentation][4].

## Filing Issues

1. For production issues and support options, see [Cloud Datastore support][25].
1. For bugs or feature requests, please first look at [existing issues][14].
1. When applicable, create a new [report][15].
1. For bugs, detail the steps to reproduce the problem and the affected version number.
1. For feature requests, articulate the use case you are trying solve and describe any current workaround(s).

## Contributing changes

- See [CONTRIB.md][7]

## Licensing

- See [LICENSE][8]

[1]: python/demos/trivial/adams.py
[2]: java/demos/src/main/java/com/google/datastore/v1/demos/trivial/Adams.java
[4]: https://cloud.google.com/datastore
[6]: https://cloud.google.com/datastore/reference/rpc
[7]: CONTRIB.md
[8]: LICENSE
[9]: python
[10]: java
[14]: https://github.com/GoogleCloudPlatform/google-cloud-datastore/issues
[15]: https://github.com/GoogleCloudPlatform/google-cloud-datastore/issues/new
[18]: https://pypi.python.org/pypi/googledatastore
[24]: https://cloud.google.com/datastore/docs/client-libraries
[25]: https://cloud.google.com/datastore/docs/support
[26]: https://github.com/GoogleCloudPlatform/gcloud-java
[27]: https://github.com/GoogleCloudPlatform/gcloud-python
