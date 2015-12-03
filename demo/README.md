# ndb demo application

## Overview

This demo shows various ways to use ndb. The application consists of serveral
different handlers.
* hello.py: a simple hello world application.
* guestbook.py: a simple guestbook application.
* dataviewer.py: a page that allows you to query for entities in your Datastore
  using ndb GQL.
* main.py: home page and user handling including login/logout.
* fibo.py: calculate the fibonacci sequence, taking advantage of ndb tasklets.

This folder also has a demo for using ndb from outside of App Engine. You can use `shell.py` to interact with your Datastore using ndb through the Cloud Datastore API.

## Setup (demo application)

1. Copy ndb (the entire folder) into the demo subdirectory.
2. Deploy the demo application using the command:

           gcloud preview app deploy demo/app.yaml

    This will deploy the app to your default project, which you can configure using gcloud:

           gcloud config set project <project-id>

## Shell

1. First, ensure your environment is setup.

           export DATASTORE_PROJECT_ID=<project-id>
           export DATASTORE_USE_PROJECT_ID_AS_APP_ID=true

2. Then, make sure you have authenticated using gcloud.

           gcloud auth login

3. Finally, run the shell. From here you can write ndb code to interact with Datastore. Don't forget, this shell is interacting with the production Datastore of your <project-id>.

           python demo/shell.py

