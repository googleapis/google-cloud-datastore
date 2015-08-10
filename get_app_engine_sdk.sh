#!/bin/bash

set -ev

if [[ -d cache ]]; then
  echo "Cache exists. Current contents:"
  ls -1F cache
else
  echo "Making cache directory."
  mkdir cache
fi

cd cache

if [[ -f google_appengine_1.9.17.zip ]]; then
  echo "App Engine SDK already downloaded. Doing nothing."
else
  wget https://storage.googleapis.com/appengine-sdks/featured/google_appengine_1.9.17.zip -nv
fi

if [[ -d google_appengine ]]; then
  echo "App Engine SDK already unzipped. Doing nothing."
else
  unzip -q google_appengine_1.9.17.zip
fi

echo "Cache contents after getting SDK:"
ls -1F 