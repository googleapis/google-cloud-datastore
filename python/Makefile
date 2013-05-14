#
# Copyright 2013 Google Inc. All Rights Reserved.
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
PROTOC=protoc
ZIP=zip
VERSION=$(shell sed -ne "s/__version__ = '\(.*\)'/\1/p" setup.py)

all:
	${PROTOC} --proto_path=../proto ../proto/datastore_v1.proto --python_out=googledatastore
	python setup.py sdist --formats=gztar,zip
	sphinx-apidoc --full -o docs googledatastore googledatastore/*_test.py
	PYTHONPATH=.. make -C docs html
	mkdir -p build
	cp -R demos build/googledatastore-demos-${VERSION}
	cp -R docs/_build/html build/googledatastore-docs-${VERSION}
	cd build; zip -r googledatastore-demos-${VERSION}.zip googledatastore-demos-${VERSION}
	cd build; zip -r googledatastore-docs-${VERSION}.zip googledatastore-docs-${VERSION}
	cp build/googledatastore-*.zip dist
