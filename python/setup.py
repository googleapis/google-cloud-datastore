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
from setuptools import setup

__version__ = '7.0.2'

setup(
    name='googledatastore',
    version=__version__,
    description='google cloud datastore protobuf client',
    long_description=open('README.rst').read(),
    author='Google Cloud Datastore Team',
    author_email='gcd-discuss@google.com',
    url='https://github.com/GoogleCloudPlatform/google-cloud-datastore',
    packages=['googledatastore'],
    package_dir={'googledatastore': 'googledatastore'},
    install_requires=[
        'httplib2>=0.9.1,<=0.20.1',
        'oauth2client>=2.0.1,<4.0.0',
        'proto-google-cloud-datastore-v1>=0.90.0',
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Topic :: Database',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],
)
