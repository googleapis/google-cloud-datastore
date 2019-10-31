# Java Client

## v1-1.6.2

  - Version bumps for dependencies.

## v1-1.6.1

  - Version bumps for dependencies.

## v1-1.6.0

  - Switch to proto-google-cloud-datastore-v1. The `datastore-v1-protos` dependency is *deprecated* and will not be updated henceforth.
  - Cache project ID information when retrieved from Compute Engine metadata.
  - Use GzipFixingInputStream for error response content.

## v1-1.5.1

  - Include [#186](https://github.com/GoogleCloudPlatform/google-cloud-datastore/pull/186), which fixes HTTP connection handling on errors.

## v1-1.5.0

  - Include [ID reservation](https://cloud.google.com/datastore/docs/reference/rest/v1/projects/reserveIds).

## v1-1.4.1

  - Fix [google/google-http-java-client#367](https://github.com/google/google-http-java-client/issues/367)

## v1-1.4.0

  - Add method `host()` to `DatastoreOptions`.

## v1-1.3.0

  - Update `datastore-v1-protos` dependency to a version that does not package the `com.google.api`, `com.google.rpc`, or `com.google.type` classes.
  - Exclude `guava-jdk5` from dependencies.

## v1-1.2.0

  - Rename `LocalDevelopmentDatastore.*` classes to `DatastoreEmulator.*` and update them to use the Cloud Datastore emulator.
  - Update `datastore-v1-protos` dependency to a version that uses `protobuf-java` 3.0.0 and does not package the `com.google.protobuf` classes.

## v1-1.1.0

  - Update `DatastoreHelper.localHost` so it works with the Cloud Datastore Emulator.

## v1-1.0.0

  - Support for [Google Cloud Datastore API v1](https://cloud.google.com/datastore/reference/rpc/).
  - The client and generated proto classes are now in the
    `com.google.datastore.v1` Java package.
  - The Maven artifacts are still found under groupId `com.google.cloud.datastore`.
    The artifactIds are:
      - `datastore-v1-proto-client-parent`
      - `datastore-v1-proto-client`
      - `datastore-v1-protos`

## v1beta3-1.0.0-beta.2

  - Allow IP address to be used in local host option.

## v1beta3-1.0.0-beta.1
 
  - Use try-with-resource to close RPC streams.
  - Removed unnecessary logging information.

## v1beta3-1.0.0-beta

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
