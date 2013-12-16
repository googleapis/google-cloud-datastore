# Google Cloud Datastore

Google Cloud Datastore is a fully managed, schemaless, non-relational
datastore accessible through Google APIs infrastructure. It provides
a rich set of query capabilities, supports atomic transactions, and
automatically scales up and down in response to load.

The API is deliberately low-level to map to the underlying Datastore RPC model and provide more flexibility to developers and higher level library implementers.

This repository contains the source code of samples and developer
resources related to Google Cloud Datastore:
- [Service and protocol buffers messages definition][6]
- [Python protocol buffers client library and samples][9]
- [Java protocol buffers Java client library and samples][10]
- [Node.js samples][11]

## Samples

### JSON
- [Node.js][3], [todo.js][24]
- [Ruby][21], [todos.rb][25]
- [[Add yours here][16]]

### Protobuf
- [Python][1]
- [Java][2]
- [[Add yours here][16]]

## Client libraries

### JSON

- [Node.js][17]:

```
npm install googleapis
```

- [Ruby (google-api-client)][23]

```
gem install google-api-client
```

- [Ruby (ActiveDatastore)][22]

```
gem install active_datastore
```

- [Dart][26]
- [[Add yours here][16]]

### Protobuf
- [Python][18] ([readthedocs][19]):

```
pip install googledatastore
```

- Maven/Java ([javadoc][20]):

```
<dependency>
  <groupId>com.google.apis</groupId>
  <artifactId>google-api-services-datastore-protobuf</artifactId>
  <version>v1beta2-rev1-2.1.0</version>
</dependency>
```

- [[Add yours here][16]]

## Documentation

- [Getting Started][4]
- [JSON API reference][5]
- [Protocol Buffers API reference][6]

## Filing Issues

1. For production issues and support, see [Google Cloud Platform Support packages][13].
1. For bugs or feature requests, please first look at [existing issues][14].
1. When applicable, create a new [report][15].
1. For bugs, detail the steps to reproduce the problem and the affected version number.
1. For feature requests, articulate the usecase you are trying solve and describe current workaround.
1. Make sure to annotate the issues with the appropriate labels.

## Contributing changes

- See [CONTRIB.md][7]

## Licensing

- See [LICENSE][8]

[1]: python/demos/trivial/adams.py
[2]: java/demos/src/main/java/com/google/api/services/datastore/demos/trivial/adams.java
[3]: https://github.com/GoogleCloudPlatform/google-cloud-datastore/blob/master/nodejs/demos/trivial/adams.js
[4]: https://developers.google.com/datastore
[5]: https://developers.google.com/datastore/docs/apis/v1beta2/
[6]: https://developers.google.com/datastore/docs/apis/v1beta2/proto
[7]: CONTRIB.md
[8]: LICENSE
[9]: python
[10]: java
[11]: nodejs
[13]: https://cloud.google.com/support/packages
[14]: https://github.com/GoogleCloudPlatform/google-cloud-datastore/issues
[15]: https://github.com/GoogleCloudPlatform/google-cloud-datastore/issues/new
[16]: https://github.com/GoogleCloudPlatform/google-cloud-datastore/fork
[17]: https://npmjs.org/package/googleapis
[18]: https://pypi.python.org/pypi/googledatastore
[19]: googledatastore.readthedocs.org
[20]: https://developers.google.com/datastore/docs/apis/javadoc/
[21]: ruby/demos/trivial/adams.rb
[22]: https://github.com/sudhirj/active_datastore
[23]: https://rubygems.org/gems/google-api-client
[24]: nodejs/demos/todos/todo.js
[25]: ruby/demos/todos/todos.rb
[26]: https://github.com/dart-google-apis/dart_datastore_v1beta1_api_client
