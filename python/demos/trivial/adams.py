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
export DATASTORE_SERVICE_ACCOUNT=..  # not required on
export DATASTORE_PRIVATE_KEY_FILE=.. # Google Compute Engine
adams.py <DATASET_ID>
"""
import logging
import sys

import googledatastore as datastore

def main():
  # Set dataset id from command line argument.
  if len(sys.argv) < 2:
    print 'Usage: adams.py <DATASET_ID>'
    sys.exit(1)
  # Set the dataset from the command line parameters.
  datastore.set_options(dataset=sys.argv[1])
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
    # Set the entity key with only one `path_element`: no parent.
    path = key.path_element.add()
    path.kind = 'Trivia'
    path.name = 'hgtg'
    # Add one key to the lookup request.
    req.key.extend([key])
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
      entity = req.mutation.insert.add()
      # Copy the entity key.
      entity.key.CopyFrom(key)
      # Add two entity properties:
      # - a utf-8 string: `question`
      prop = entity.property.add()
      prop.name = 'question'
      prop.value.string_value = 'Meaning of life?'
      # - a 64bit integer: `answer`
      prop = entity.property.add()
      prop.name = 'answer'
      prop.value.integer_value = 42
    # Execute the Commit RPC synchronously and ignore the response:
    # Apply the insert mutation if the entity was not found and close
    # the transaction.
    datastore.commit(req)
    # Get question property value.
    question = entity.property[0].value.string_value
    # Get answer property value.
    answer = entity.property[1].value.integer_value
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
    # It includes the `method` called and the `reason` of the
    # failure as well as the original `HTTPResponse` object.
    logging.error('Error while doing datastore operation')
    logging.error('RPCError: %(method)s %(reason)s',
                  {'method': e.method,
                   'reason': e.reason})
    logging.error('HTTPError: %(status)s %(reason)s',
                  {'status': e.response.status,
                   'reason': e.response.reason})
    return

if __name__ == '__main__':
  main()
