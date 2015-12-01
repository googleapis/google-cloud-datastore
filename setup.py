#
# Copyright 2015 The ndb Authors. All Rights Reserved.
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

from setuptools import setup
from setuptools import find_packages


REQUIREMENTS = [
    # This requirement will eventually be 'googledatastore==4.0.0b1'.
    'testing-v1beta3-googledatastore==4.0.0b4',
]
TEST_REQUIREMENTS = [
    'portpicker',
]

setup(
    name='ndb',
    version='1.0.13b1',
    description='Google App Engine NDB',
    author='Patrick Costello',
    author_email='pcostello@google.com',
    scripts=[],
    url='https://github.com/GoogleCloudPlatform/datastore-ndb-python',
    packages=find_packages(),
    license='Apache 2.0',
    platforms='Posix; MacOS X; Windows',
    include_package_data=True,
    zip_safe=False,
    install_requires=REQUIREMENTS,
    tests_require=TEST_REQUIREMENTS,
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Internet',
    ]
)
