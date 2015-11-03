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

## Setup

1. Copy ndb (the entire folder) into the demo subdirectory.
2. Deploy the demo application using the command:

           gcloud preview app deploy demo/app.yaml

    This will deploy the app to your default project, which you can configure using gcloud:

           gcloud config set project <project-id>
