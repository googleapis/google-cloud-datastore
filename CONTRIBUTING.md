# Contributing

- **Please sign one of the contributor license agreements below.**
- Fork the repo, develop and test your code changes, add docs.
- Make sure that your commit messages clearly describe the changes.
- Send a pull request.

## Getting Started

You need to separately download and install the App Engine SDK.

Note: There is no guarantee older versions of the App Engine SDK will
work with the current version of ndb.

You can setup your environment by running:

    gcloud components update app-engine-python
    export GAE=$GCLOUD_PATH/platform/google_appengine

If you haven't downloaded `gcloud` yet, you can find it and instructions
about using it on the [Cloud SDK page][1].

## Code Structure

The code is structured into four subdirectories:

- ndb: This is the main code base.  Notable submodules are
  key.py, model.py, query.py, eventloop.py, tasklets.py, and context.py.
  For each module foo.py there's a corresponding foo_test.py which
  contains unit tests for that module.
- demo: This is where demo programs live.  Check out guestbook.py and
  main.py.
- samples: This is where sample code lives.
- tests: This is where additional ndb tests live.

The main directory contains some scripts and auxiliary files.

## Working on ndb

We accept contributions, so if you see something wrong file an issue or
send us a pull request! 

Note: Because this library is included in the App Engine python runtime,
we currently cannot accept any changes that break our existing API
(for now). Additionally, there are restrictions on adding any extra
third party dependencies. 

### Running Tests

Tests can be run using tox.

    tox -e py27

Lint can also be run using tox. This will fire off two pylint commands, one
for the main ndb library and one for the tests which have slightly relaxed
requirements.

    tox -e lint

to run coverage tests, run

    tox -e cover

## Contributor License Agreements
  
  Before we can accept your pull requests you'll need to sign a Contributor
  License Agreement (CLA):

  - **If you are an individual writing original source code** and **you own the
  intellectual property**, then you'll need to sign an [individual CLA][2].
  - **If you work for a company that wants to allow you to contribute your work**,
  then you'll need to sign a [corporate CLA][3].

  You can sign these electronically (just scroll to the bottom). After that,
  we'll be able to accept your pull requests.

  [1]: https://cloud.google.com/sdk/
  [2]: https://developers.google.com/open-source/cla/individual
  [3]: https://developers.google.com/open-source/cla/corporate
