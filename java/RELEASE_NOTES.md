# Java Client

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
