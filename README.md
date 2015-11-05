# Google Datastore ndb Client Library

[![Build Status](https://travis-ci.org/GoogleCloudPlatform/datastore-ndb-python.svg?branch=master)](https://travis-ci.org/GoogleCloudPlatform/datastore-ndb-python)
[![Coverage Status](https://coveralls.io/repos/GoogleCloudPlatform/datastore-ndb-python/badge.svg?branch=master&service=github)](https://coveralls.io/github/GoogleCloudPlatform/datastore-ndb-python?branch=master)

## Introduction

---
**Note:** As of Google App Engine SDK 1.6.4, ndb has reached status General Availability.  
    
Using ndb from outside of Google App Engine (without the use of Remote API) is currently a work in progress and has not been released.

---

ndb is a client library for use with [Google Cloud Datastore][0].
It was designed specifically to be used from within the 
[Google App Engine][1] Python runtime.

ndb is included in the Python runtime and is available through a
standard Python import.

    from google.appengine.ext import ndb

It is also possible to include ndb directly from this GitHub project.
This will allow application developers to manage their own dependencies. Note
however that ndb depends on the non-public Google Datastore App Engine RPC API. This means that there is no explicit support for older versions of ndb in the App Engine Python runtime.

## Overview

Learn how to use the ndb library by visiting the Google Cloud Platform 
[documentation][2].


[0]:https://cloud.google.com/datastore
[1]:https://cloud.google.com/appengine
[2]:https://cloud.google.com/appengine/docs/python/ndb/
