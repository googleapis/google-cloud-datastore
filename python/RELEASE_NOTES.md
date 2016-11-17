# Python Client

## 6.4.0

  - Removed unnecessary dependencies from `setup.py`.

## 6.3.0

  - Update required versions of `httplib2` and `oauth2client`.

## 6.2.0

  - Rename `local_cloud_datastore.py` to `datastore_emulator.py` and update it to use the Cloud Datastore emulator.

## 6.1.0

  - Update `helper.py` so it works with the Cloud Datastore Emulator.

## 6.0.0

  - Support for [Google Cloud Datastore API v1](https://cloud.google.com/datastore/reference/rpc/)

## 5.0.0-beta1

  - Upgraded proto library to 1.0.0.beta.2.

## 5.0.0-beta
 
  - Support for [Google Cloud Datastore API v1beta3](https://cloud.google.com/datastore/reference/rpc/)
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
