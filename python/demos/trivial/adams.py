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
"""Adams datastore demo."""
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
    # Create a RPC request to write mutations outside of a transaction.
    req = datastore.BlindWriteRequest()
    # Add mutation that update or insert one entity.
    entity = req.mutation.upsert.add()
    # Set the entity key with only one `path_element`: no parent.
    path = entity.key.path_element.add()
    path.kind = 'Trivia'
    path.name = 'hgtg'
    # Add two entity properties:
    # - a utf-8 string: `question`
    property = entity.property.add()
    property.name = 'questions'
    value = property.value.add()
    value.string_value = 'Meaning of life?'
    # - a 64bit integer: `answer`
    property = entity.property.add()
    property.name = 'answer'
    value = property.value.add()
    value.integer_value = 42
    # Execute the RPC synchronously and ignore the response.
    datastore.blind_write(req)
    # Create a RPC request to get entities by key.
    req = datastore.LookupRequest()
    # Add one key to lookup w/ the same entity key.
    req.key.extend([entity.key])
    # Execute the RPC and get the response.
    resp = datastore.lookup(req)
    # Found one entity result.
    entity = resp.found[0].entity
    # Get question property value.
    question = entity.property[0].value[0].string_value
    # Get answer property value.
    answer = entity.property[1].value[0].integer_value
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
