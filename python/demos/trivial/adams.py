#! /usr/bin/env python
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
"""Adams datastore demo.

Usage:
adams.py <PROJECT_ID>
"""
import logging
import sys

import googledatastore as datastore
from googledatastore.helper import *

def main():
  # Set project id from command line argument.
  if len(sys.argv) < 2:
    print 'Usage: adams.py <PROJECT_ID>'
    sys.exit(1)
  # Set the project from the command line parameters.
  datastore.set_options(project_id=sys.argv[1])
  try:
    # Create a RPC request to begin a new transaction.
    req = datastore.BeginTransactionRequest()
    # Execute the RPC synchronously.
    resp = datastore.begin_transaction(req)
    # Get the transaction handle from the response.
    tx = resp.transaction
    # Create a RPC request to get entities by key.
    req = datastore.LookupRequest()
    # Create a new entity key.
    key = datastore.Key()
    # Set the entity key with only one `path` element: no parent.
    elem = key.path.add()
    elem.kind = 'Trivia'
    elem.name = 'hgtg'
    # Add one key to the lookup request.
    req.keys.extend([key])
    # Set the transaction, so we get a consistent snapshot of the
    # entity at the time the transaction started.
    req.read_options.transaction = tx
    # Execute the RPC and get the response.
    resp = datastore.lookup(req)
    # Create a RPC request to commit the transaction.
    req = datastore.CommitRequest()
    # Set the transaction to commit.
    req.transaction = tx
    if resp.found:
      # Get the entity from the response if found.
      entity = resp.found[0].entity
    else:
      # If no entity was found, insert a new one in the commit request mutation.
      entity = req.mutations.add().insert
      # Copy the entity key.
      entity.key.CopyFrom(key)
      # Add two entity properties:
      # - a utf-8 string: `question`
      entity.properties['question'].string_value ='Meaning of life?'
      # - a 64bit integer: `answer`
      prop = entity.properties['answer'].integer_value = 42
    # Execute the Commit RPC synchronously and ignore the response:
    # Apply the insert mutation if the entity was not found and close
    # the transaction.
    datastore.commit(req)
    # Get question property value.
    question = entity.properties['question'].string_value
    # Get answer property value.
    answer = entity.properties['answer'].integer_value
    # Print the question and read one line from stdin.
    print question
    result = raw_input('> ')
    if result == str(answer):
      print ('fascinating, extraordinary and, '
             'when you think hard about it, completely obvious.')
    else:
      print "Don't Panic!"
  except datastore.RPCError as e:
    # RPCError is raised if any error happened during a RPC.
    # It includes the `method` called, the canonical error code `code`,
    # and the `message` of the failure.
    logging.error('Error while doing datastore operation')
    logging.error('RPCError: %(method)s %(reason)s',
                  {'method': e.method,
                   'code': e.code,
                   'reason': e.message})
    return

if __name__ == '__main__':
  main()
