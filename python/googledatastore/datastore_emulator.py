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
"""Python wrapper for gcd.sh."""

__author__ = 'eddavisson@google.com (Ed Davisson)'


import logging
import os
import shutil
import socket
import subprocess
import tempfile
import time
import urllib
import zipfile

import httplib2
import portpicker

from googledatastore import connection


_DEFAULT_GCD_OPTIONS = ['--allow_remote_shutdown', '--testing']

class LocalCloudDatastoreFactory(object):
  """A factory for constructing LocalCloudDatastore objects."""

  def __init__(self, working_directory, gcd_zip, java=None):
    """Constructs a factory for building local datastore instances.

    Args:
      working_directory: path to a directory where temporary files will be
          stored
      gcd_zip: path to the gcd zip file
      java: path to a java executable

    Raises:
      ValueError: if gcd.sh cannot be located in the gcd zip file
    """
    self._working_directory = working_directory

    self._remote_datastores = {}

    # Extract GCD.
    zipped_file = zipfile.ZipFile(gcd_zip)
    self._gcd_dir = os.path.join(self._working_directory, 'gcd')
    os.mkdir(self._gcd_dir)
    zipped_file.extractall(self._gcd_dir)
    # Locate gcd.sh in the unzipped directory (it may be in a directory which
    # contains a version string).
    gcd_dirs = [d for d in os.listdir(self._gcd_dir)
                if os.path.isdir(os.path.join(self._gcd_dir, d))]
    for d in gcd_dirs:
      if d.startswith('gcd'):
        self._gcd_sh = os.path.join(self._gcd_dir, d, 'gcd.sh')
        break
    else:
      raise ValueError('could not find gcd.sh in zip file')
    os.chmod(self._gcd_sh, 0700)  # executable

    # Make GCD use our copy of Java.
    if java:
      os.environ['JAVA'] = java

  def Get(self, project_id):
    """Returns an existing local datastore instance for the provided project_id.

    If a local datastore instance doesn't yet exist, it creates one.
    """
    if project_id in self._remote_datastores:
      return self._remote_datastores[project_id]

    datastore = self.Create(project_id)
    self._remote_datastores[project_id] = datastore
    return datastore

  def Create(self, project_id, start_options=None, deadline=10):
    """Creates a local datastore instance.

    This method will wait for up to 'deadline' seconds for the datastore to
    start.

    Args:
      project_id: project ID
      start_options: a list of additional command-line options to pass to the
          gcd.sh start command
      deadline: number of seconds to wait for the datastore to respond

    Returns:
      a LocalCloudDatastore

    Raises:
      IOError: if the local datastore could not be started within the deadline
    """
    return LocalCloudDatastore(self._gcd_sh, self._working_directory,
                               project_id, deadline, start_options)

  def __del__(self):
    # Delete temp files.
    shutil.rmtree(self._gcd_dir)


class LocalCloudDatastore(object):
  """A local datastore (based on gcd.sh)."""

  def __init__(self, gcd_sh, working_directory, project_id, deadline,
               start_options):
    """Constructs a local datastore.

    Clients should use LocalCloudDatastoreFactory to construct
    LocalCloudDatastore instances.

    Args:
      gcd_sh: path to gcd.sh
      working_directory: directory file where temporary files will be stored
      project_id: project ID
      deadline: number of seconds to wait for the datastore to start
      start_options: a list of additional command-line options to pass to the
          gcd.sh start command

    Raises:
      IOError: if the datastore failed to start within the deadline
    """
    self._project_id = project_id
    self._gcd_sh = gcd_sh
    self._http = httplib2.Http()
    self.__running = False

    self._tmp_dir = tempfile.mkdtemp(dir=working_directory)
    self._project_directory = os.path.join(self._tmp_dir, self._project_id)
    p = subprocess.Popen([gcd_sh,
                          'create',
                          '--project_id=%s' % self._project_id,
                          self._project_directory])
    if p.wait() != 0:
      raise IOError('could not create project in directory: %s'
                    % self._project_directory)

    # Start GCD and wait for it to start responding to requests.
    port = portpicker.PickUnusedPort()
    self._host = 'http://localhost:%d' % port
    cmd = [self._gcd_sh, 'start', '--port=%d' % port]
    cmd.extend(_DEFAULT_GCD_OPTIONS)
    if start_options:
      cmd.extend(start_options)
    cmd.append(self._project_directory)
    subprocess.Popen(cmd)
    if not self._WaitForStartup(deadline):
      raise IOError('datastore did not respond within %ds' % deadline)
    endpoint = '%s/datastore/v1/projects/%s' % (self._host,
                                                self._project_id)
    self.__datastore = connection.Datastore(project_endpoint=endpoint)
    self.__running = True

  def GetDatastore(self):
    """Returns a googledatatsore.Datastore that is connected to the gcd tool."""
    return self.__datastore

  def _WaitForStartup(self, deadline):
    """Waits for the datastore to start.

    Args:
      deadline: deadline in seconds

    Returns:
      True if the instance responds within the deadline, False otherwise.
    """
    start = time.time()
    sleep = 0.05

    def Elapsed():
      return time.time() - start

    while True:
      try:
        response, _ = self._http.request(self._host)
        if response.status == 200:
          logging.info('local server responded after %f seconds', Elapsed())
          return True
      except socket.error:
        pass
      if Elapsed() >= deadline:
        # Out of time; give up.
        return False
      else:
        time.sleep(sleep)
        sleep *= 2

  def Clear(self):
    """Clears all data from the local datastore instance.

    Returns:
      True if the data was successfully cleared, False otherwise.
    """
    body = urllib.urlencode({'action': 'Clear Datastore'})
    headers = {'Content-type': 'application/x-www-form-urlencoded',
               'Content-length': str(len(body))}
    response, _ = self._http.request('%s/_ah/admin/datastore' % self._host,
                                     method='POST', headers=headers, body=body)
    if response.status == 200:
      return True
    else:
      logging.warning('failed to clear datastore; response was: %s', response)

  def Stop(self):
    if not self.__running:
      return
    logging.info('shutting down the datastore running at %s', self._host)
    # Shut down the datastore.
    headers = {'Content-length': '0'}
    response, _ = self._http.request('%s/_ah/admin/quit' % self._host,
                                     method='POST', headers=headers)
    if response.status != 200:
      logging.warning('failed to shut down datastore; response: %s', response)

    self.__running = False
    # Delete temp files.
    shutil.rmtree(self._tmp_dir)

  def __del__(self):
    # If the user forgets to call Stop()
    logging.warning('datastore shutting down due to '
                    'LocalCloudDatastore object deletion')
    self.Stop()
