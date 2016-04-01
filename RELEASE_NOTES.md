# Release Notes

Starting with the v1beta3 release:

  - Release notes for the API and `gcd` tool can be found at:
    <https://cloud.google.com/datastore/release-notes>
  - Release notes are grouped by library.

## Java Client

### java-v1beta3-1.0.0-beta

  - Support for [Google Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials).
  - Fixes:
    https://github.com/GoogleCloudPlatform/google-cloud-datastore/issues/37
  - Environment variable `DATASTORE_DATASET_ID` is now `DATASTORE_PROJECT_ID`.
  - Environment variable `DATASTORE_HOST` is now `DATASTORE_EMULATOR_HOST` and
    its value no longer includes the URL scheme. For example:

      `DATASTORE_EMULATOR_HOST=localhost:8080`

    instead of:

      `DATASTORE_HOST=http://localhost:8080`
- `DatastoreHelper.getOptionsFromEnv()` now
    supports automatic detection of the project ID when running on
    Compute Engine.
  - The client and generated proto classes are now in the
    `com.google.datastore.v1beta3` Java package.
  - The Maven artifacts are now found under groupId `com.google.cloud.datastore`.
    The artifactIds are:
      - `datastore-v1beta3-proto-client-parent`
      - `datastore-v1beta3-proto-client`
      - `datastore-v1beta3-protos`
  - The versionIds no longer include the API version (e.g. `v1beta3`)
    since it is now part of the artifactId.
  - Updated `LocalDevelopmentDatastore` so it is in sync with the `gcd` tool.
    - Fixes:
      <https://github.com/GoogleCloudPlatform/google-cloud-datastore/issues/53>

## Python Client

### python-5.0.0-beta (v1beta3)

  - Support for [Google Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials).
  - Fixes:
    <https://github.com/GoogleCloudPlatform/google-cloud-datastore/issues/37>
  - Environment variable `DATASTORE_DATASET_ID` is now `DATASTORE_PROJECT_ID`.
  - Environment variable `DATASTORE_HOST` is now `DATASTORE_EMULATOR_HOST` and
    its value no longer includes the URL scheme. For example:

      `DATASTORE_EMULATOR_HOST=localhost:8080`

    instead of:

      `DATASTORE_HOST=http://localhost:8080`
- `helper.set_value()` now supports `long` values.
    - Fixes:
      <https://github.com/GoogleCloudPlatform/google-cloud-datastore/issues/49>

## Other

### v1beta3

- Remove v1beta2-based Ruby and Node.js demos.
- The protocol buffer definition of the API is now published at
  <https://github.com/google/googleapis>.

## Pre-v1beta3 Releases

### v1beta2-rev1-3.0.2

- Fix QuerySplitter to support small queries that do not need to be split.

### v1beta2-rev1-3.0.0

- Change to QuerySplitter interface.
  - This will not affect users of the interface, however is a breaking change for
    any implementers of the interface. This adds a method that allows
    specifying the PartitionId so that namespace queries can be split.
- Modify helper functions in the Python client library to support long integers.
  - Fixes:
    <https://github.com/GoogleCloudPlatform/google-cloud-datastore/issues/49>.
- Check environment variables before creating a Compute Engine credential in Python and Java client libraries.
  - Fixes:
    <https://github.com/GoogleCloudPlatform/google-cloud-datastore/issues/37>.
- Bring LocalDevelopmentDatastore into sync with the current `gcd` command line tool options.
- Renamed `DatastoreHelper.getOptionsfromEnv()` to `DatastoreHelper.getOptionsFromEnv()`.
- Assign a PEP 440-compatible identifier to the Python client library package.

### v1beta2-rev1-2.1.1
- Update to latest App Engine SDK version.
  - Fixes:
    <https://github.com/GoogleCloudPlatform/google-cloud-datastore/issues/35>.

### v1beta2-rev1-2.1.0

- Make Python client library thread-safe.
- Fix propagation of `indexed` param to list values in Python helper.
- Accept an optional HttpRequestInitializer in DatastoreOptions (Java).
- Demonstrate JWT service account support in Node.js demo app.

### v1beta2-rev1-2.0.0

- API changes:
  - `BlindWrite` method merged into `Commit`.
  - Added `list_value` to `Value` and changed `value` to a non-repeated field in `Property`.
  - In JSON API, string constants are now uppercase and underscore-separated
    instead of camel-cased (e.g. `LESS_THAN_OR_EQUAL` instead of
    `lessThanOrEqual`).
- GQL changes:
  - New synthetic literals: `BLOB`, `BLOBKEY`, `DATETIME`, `KEY`.
  - Support for `IS NULL`.
  - Fixed partition ID handling for binding arguments.
- Documentation changes:
  - All documentation has been updated to the v1beta2 API.
  - Getting started guide for Node.js now uses v0.4.5 of
    google-api-nodejs-client.
- Fixed partition ID handling for query requests that include an explicit
  partition ID.
- Fixed scopes in discovery document.
  - <https://github.com/GoogleCloudPlatform/google-cloud-datastore/issues/9>
- Fixed an issue where command line tool didn't work for some locales.
  - <https://github.com/GoogleCloudPlatform/google-cloud-datastore/issues/12>

### v1beta1-rev2-1.0.1

- GQL support.
- Metadata query support.
- Command line tool improvements.
  - Microsoft Windows support (`gcd.cmd`).
  - Testing mode.
  - More intuitive `update_indexes` command (renamed to `updateindexes`).
  - New `create` command and simplified `start` command.
  - Improved integration with existing App Engine applications.
- Ruby samples.
- Java helper for query splitting.

### v1beta1-rev1-1.0.0

- Initial release.
