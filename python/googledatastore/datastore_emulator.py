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
"""Python wrapper for the Cloud Datastore emulator."""

__author__ = 'eddavisson@google.com (Ed Davisson)'


import httplib
import logging
import os
import shutil
import socket
import subprocess
import tempfile
import time
import zipfile

from googledatastore import connection
import httplib2
import portpicker


_DEFAULT_EMULATOR_OPTIONS = ['--testing']


class DatastoreEmulatorFactory(object):
  """A factory for constructing DatastoreEmulator objects."""

  def __init__(self, working_directory, emulator_zip, java=None):
    """Constructs a factory for building datastore emulator instances.

    Args:
      working_directory: path to a directory where temporary files will be
          stored
      emulator_zip: path to the emulator zip file
      java: path to a java executable
    """
    self._working_directory = working_directory

    self._emulators = {}

    # Extract the emulator.
    zipped_file = zipfile.ZipFile(emulator_zip)
    if not os.path.isdir(self._working_directory):
      os.mkdir(self._working_directory)
    zipped_file.extractall(self._working_directory)

    self._emulator_dir = os.path.join(self._working_directory,
                                      'cloud-datastore-emulator')
    self._emulator_cmd = os.path.join(self._emulator_dir,
                                      'cloud_datastore_emulator')
    os.chmod(self._emulator_cmd, 0700)  # executable

    # Make the emulator use our copy of Java.
    if java:
      os.environ['JAVA'] = java

  def Get(self, project_id):
    """Returns an existing emulator instance for the provided project_id.

    If an emulator instance doesn't yet exist, it creates one.

    Args:
      project_id: project ID

    Returns:
      a DatastoreEmulator
    """
    if project_id in self._emulators:
      return self._emulators[project_id]

    emulator = self.Create(project_id)
    self._emulators[project_id] = emulator
    return emulator

  def Create(self, project_id, start_options=None, deadline=10):
    """Creates an emulator instance.

    This method will wait for up to 'deadline' seconds for the emulator to
    start.

    Args:
      project_id: project ID
      start_options: a list of additional command-line options to pass to the
          emulator 'start' command
      deadline: number of seconds to wait for the datastore to respond

    Returns:
      a DatastoreEmulator

    Raises:
      IOError: if the emulator could not be started within the deadline
    """
    return DatastoreEmulator(self._emulator_cmd, self._working_directory,
                             project_id, deadline, start_options)

  def __del__(self):
    # Delete temp files.
    shutil.rmtree(self._emulator_dir)


class DatastoreEmulator(object):
  """A Datastore emulator."""

  def __init__(self, emulator_cmd, working_directory, project_id, deadline,
               start_options):
    """Constructs a DatastoreEmulator.

    Clients should use DatastoreEmulatorFactory to construct DatastoreEmulator
    instances.

    Args:
      emulator_cmd: path to cloud_datastore_emulator
      working_directory: directory file where temporary files will be stored
      project_id: project ID
      deadline: number of seconds to wait for the datastore to start
      start_options: a list of additional command-line options to pass to the
          emulator 'start' command

    Raises:
      IOError: if the emulator failed to start within the deadline
    """
    self._project_id = project_id
    self._emulator_cmd = emulator_cmd
    self._http = httplib2.Http()
    self.__running = False

    self._tmp_dir = tempfile.mkdtemp(dir=working_directory)
    self._project_directory = os.path.join(self._tmp_dir, self._project_id)
    p = subprocess.Popen([emulator_cmd,
                          'create',
                          '--project_id=%s' % self._project_id,
                          self._project_directory])
    if p.wait() != 0:
      raise IOError('could not create project in directory: %s'
                    % self._project_directory)

    # Start the emulator and wait for it to start responding to requests.
    port = portpicker.PickUnusedPort()
    self._host = 'http://localhost:%d' % port
    cmd = [self._emulator_cmd, 'start', '--port=%d' % port]
    cmd.extend(_DEFAULT_EMULATOR_OPTIONS)
    if start_options:
      cmd.extend(start_options)
    cmd.append(self._project_directory)
    subprocess.Popen(cmd)
    if not self._WaitForStartup(deadline):
      raise IOError('emulator did not respond within %ds' % deadline)
    endpoint = '%s/v1/projects/%s' % (self._host, self._project_id)
    self.__datastore = connection.Datastore(project_endpoint=endpoint)
    self.__running = True

  def GetDatastore(self):
    """Returns a googledatatsore.Datastore that is connected to the emulator."""
    return self.__datastore

  def _WaitForStartup(self, deadline):
    """Waits for the emulator to start.

    Args:
      deadline: deadline in seconds

    Returns:
      True if the emulator responds within the deadline, False otherwise.
    """
    start = time.time()
    sleep = 0.05

    def Elapsed():
      return time.time() - start

    while True:
      try:
        response, _ = self._http.request(self._host)
        if response.status == 200:
          logging.info('emulator responded after %f seconds', Elapsed())
          return True
      except (socket.error, httplib.ResponseNotReady):
        pass
      if Elapsed() >= deadline:
        # Out of time; give up.
        return False
      else:
        time.sleep(sleep)
        sleep *= 2

  def Clear(self):
    """Clears all data from the emulator instance.

    Returns:
      True if the data was successfully cleared, False otherwise.
    """
    headers = {'Content-length': '0'}
    response, _ = self._http.request('%s/reset' % self._host, method='POST',
                                     headers=headers)
    if response.status == 200:
      return True
    else:
      logging.warning('failed to clear emulator; response was: %s', response)

  def Stop(self):
    """Stops the emulator instance."""
    if not self.__running:
      return
    logging.info('shutting down the emulator running at %s', self._host)
    headers = {'Content-length': '0'}
    response, _ = self._http.request('%s/shutdown' % self._host,
                                     method='POST', headers=headers)
    if response.status != 200:
      logging.warning('failed to shut down emulator; response: %s', response)

    self.__running = False
    # Delete temp files.
    shutil.rmtree(self._tmp_dir)

  def __del__(self):
    # If the user forgets to call Stop()
    logging.warning('emulator shutting down due to '
                    'DatastoreEmulator object deletion')
    self.Stop()
