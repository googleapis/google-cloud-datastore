#
# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
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