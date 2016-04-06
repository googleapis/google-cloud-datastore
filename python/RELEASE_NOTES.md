# Python Client

## python-5.0.0-beta (v1beta3)

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
