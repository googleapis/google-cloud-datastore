# ndb demo applications

## Overview

There are three separate demos:

* app: a Google App Engine application with a set of example uses
* shell: an application that sets up the ndb environment then opens up a python shell for you to interact with.
* task_list: a basic application which runs inside Compute Engine.

## app (App Engine Application)

This demo shows various ways to use ndb. The application consists of serveral
different handlers.
* hello.py: a simple hello world application.
* guestbook.py: a simple guestbook application.
* dataviewer.py: a page that allows you to query for entities in your Datastore
  using ndb GQL.
* main.py: home page and user handling including login/logout.
* fibo.py: calculate the fibonacci sequence, taking advantage of ndb tasklets.

This folder also has a demo for using ndb from outside of App Engine. You can use `shell.py` to interact with your Datastore using ndb through the Cloud Datastore API.

### Setup

1. Copy ndb (the entire folder) into the demo subdirectory.
2. Deploy the demo application using the command:

           gcloud preview app deploy demo/app.yaml

    This will deploy the app to your default project, which you can configure using gcloud:

           gcloud config set project <project-id>

## shell

1. Follow the instructions below to setup ndb in GCE.

2. Run the shell. From here you can write ndb code to interact with Datastore. Don't forget, this shell is interacting with the production Datastore of your <project-id>.

           python demo/shell.py

## Task List

1. Follow the instructions below to setup ndb in GCE.

2. Run the task list application. Don't forget, this application is interacting with the production Datastore of your <project-id>.

           python demo/task_list.py

# Setup from your local machine or from Google Compute Engine

1. Install pip

           sudo apt-get update
           sudo apt-get install python-pip

2. Install the Google App Engine SDK and point ndb to the installation. If you installed `gcloud` to a different location then your App Engine SDK may be installed in a different location. 

           sudo gcloud components install app-engine-python
           export GAE=/usr/local/share/google/google-cloud-sdk/platform/google_appengine

3. Install ndb

           pip install --pre ndb

4. Setup your environment.

           export DATASTORE_PROJECT_ID=<project-id>
           export DATASTORE_USE_PROJECT_ID_AS_APP_ID=true

5. Finally, make sure you have authenticated using gcloud.

           gcloud auth login

