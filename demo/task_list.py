#!/usr/bin/python

# Copyright 2015 Google Inc. All Rights Reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# [START all]
"""Sample for Cloud Datastore using the NDB client library.

This is a command-line task list manager.

From the command line, first setup the necessary environment variables.
export DATASTORE_PROJECT_ID=<my-project-id>
export DATASTORE_USE_PROJECT_ID_AS_APP_ID=true
"""

import ndb
# [START build_service]
# When running outside of App Engine, ndb caching policies should be adjusted.
# The local cache is never evicted, so it should be turned off for any long
# running processes. In order to use memcache, App Engine Remote API must be
# installed. Note that if you are running ndb both inside and outside of
# App Engine, your memcache policies *must* match. Otherwise, calling put may
# not invalidate your cache and App Engine ndb will get stale results.
ndb.get_context().set_cache_policy(False)
ndb.get_context().set_memcache_policy(False)
# [END build_service]


# [START add_entity]
# Define the model we will be using for the tasks.
class Task(ndb.Model):
  description = ndb.StringProperty()
  created = ndb.DateTimeProperty(auto_now_add=True)
  done = ndb.BooleanProperty(default=False)


def add_task(description):
  """Adds a new task to the Datastore.

  Args:
    description: A string description of the task.

  Returns:
    The key of the entity that was put.
  """
  return Task(description=description).put()
# [END add_entity]


# [START update_entity]
@ndb.transactional
def mark_done(task_id):
  """Marks a task as done.

  Args:
    task_id: The integer id of the task to update.

  Raises:
    ValueError: if the requested task doesn't exist.
  """
  task = Task.get_by_id(task_id)
  if task is None:
    raise ValueError('Task with id %d does not exist' % task_id)
  task.done = True
  task.put()
# [END update_entity]


# [START retrieve_entities]
def list_tasks():
  """Lists all the task entities in ascending order of completion time.

  Returns:
    A list of tasks.
  """
  # Querying the tasks without an ancestor is eventually consistent.
  return list(Task.query().order(Task.created))
# [END retrieve_entities]


# [START delete_entity]
def delete_task(task_id):
  """Deletes the given task.

  Args:
    task_id: The integer id of the task to delete.
  """
  ndb.Key('Task', task_id).delete()
# [END delete_entity]


# [START format_results]
def format_tasks(tasks):
  """Converts a list of tasks to a list of string representations.

  Args:
    tasks: A list of the tasks to convert.
  Returns:
    A list of string formatted tasks.
  """
  return ['%d : %s (%s)' % (task.key.id(),
                            task.description,
                            ('done' if task.done
                             else 'created %s' % task.created))
          for task in tasks]
# [END format_results]


def get_arg(cmds):
  """Accepts a split string command and validates its size.

  Args:
    cmds: A split command line as a list of strings.

  Returns:
    The string argument to the command.

  Raises:
    ValueError: If there is no argument.
  """
  if len(cmds) != 2:
    raise ValueError('%s needs an argument.' % cmds[0])
  return cmds[1]


def handle_command(command):
  """Accepts a string command and performs an action.

  Args:
    command: the command to run as a string.
  """
  try:
    cmds = command.split(None, 1)
    cmd = cmds[0]
    if cmd == 'new':
      add_task(get_arg(cmds))
    elif cmd == 'done':
      mark_done(int(get_arg(cmds)))
    elif cmd == 'list':
      for task in format_tasks(list_tasks()):
        print task
    elif cmd == 'delete':
      delete_task(int(get_arg(cmds)))
    else:
      print_usage()
  except Exception, e:  # pylint: disable=broad-except
    print e
    print_usage()


def print_usage():
  """Print the usage of our task list command."""
  print 'Usage:'
  print ''
  print '  new <description>  Adds a task with a description <description>'
  print '  done <task-id>     Marks a task as done'
  print '  list               Lists all tasks by creation time'
  print '  delete <task-id>   Deletes a task'
  print ''


def main():
  print 'Cloud Datastore Task List'
  print ''
  print_usage()
  while True:
    line = raw_input('> ')
    if not line:
      break
    handle_command(line)

if __name__ == '__main__':
  main()
# [END all]
