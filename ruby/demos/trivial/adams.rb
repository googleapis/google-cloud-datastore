#! /usr/bin/env ruby
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

require 'rubygems'
require 'json'
require 'google/api_client'

if ARGV.empty?
  abort "usage: adams.rb <dataset-id>"
end

client = Google::APIClient.new(
  :application_name => 'adams-ruby',
  :application_version => '1.0.0')
# Build the datastore API client.
datastore = client.discovered_api('datastore', 'v1beta2')

# Get the dataset id from command line argument.
dataset_id = ARGV[0]
# Get the credentials from the environment.
service_account = ENV['DATASTORE_SERVICE_ACCOUNT']
private_key_file = ENV['DATASTORE_PRIVATE_KEY_FILE']

# Load the private key from the .p12 file.
private_key = Google::APIClient::KeyUtils.load_from_pkcs12(private_key_file,
                                                           'notasecret')
# Set authorization scopes and credentials.
client.authorization = Signet::OAuth2::Client.new(
  :token_credential_uri => 'https://accounts.google.com/o/oauth2/token',
  :audience => 'https://accounts.google.com/o/oauth2/token',
  :scope => ['https://www.googleapis.com/auth/datastore',
             'https://www.googleapis.com/auth/userinfo.email'],
  :issuer => service_account,
  :signing_key => private_key)
# Authorize the client.
client.authorization.fetch_access_token!

# Start a new transaction.
resp = client.execute(
  :api_method => datastore.datasets.begin_transaction,
  :parameters => {:datasetId => dataset_id},
  :body_object => {})

# Get the transaction handle
tx = Base64.encode64(resp.data.transaction)

# Get the entity by key.
resp = client.execute(
  :api_method => datastore.datasets.lookup,
  :parameters => {:datasetId => dataset_id},
  :body_object => {
    # Set the transaction, so we get a consistent snapshot of the
    # value at the time the transaction started.
    :readOptions => {:transaction => tx},
    # Add one entity key to the lookup request, with only one
    # :path element (i.e. no parent)
    :keys => [{:path => [{:kind => 'Trivia', :name => 'hgtg42'}]}]
  })

if not resp.data.found.empty?
  # Get the entity from the response if found.
  entity = resp.data.found[0].entity
  # Get `question` property value.
  question = entity.properties.question.stringValue
  # Get `answer` property value.
  answer = entity.properties.answer.integerValue
  # No mutation.
  mutation = nil
else
  question = 'Meaning of life?'
  answer = 42
  # If the entity is not found create it.
  entity = {
    # Set the entity key with only one `path` element: no parent.
    :key => {
      :path => [{:kind => 'Trivia', :name => 'hgtg42'}]
    },
    # Set the entity properties:
    # - a utf-8 string: `question`
    # - a 64bit integer: `answer`
    :properties => {
      :question => {:stringValue => question},
      :answer => {:integerValue => answer},
    }
  }
  # Build a mutation to insert the new entity.
  mutation = {:insert => [entity]}
end

# Commit the transaction and the insert mutation if the entity was not found.
client.execute(
  :api_method => datastore.datasets.commit,
  :parameters => {:datasetId => dataset_id},
  :body_object => {
    :transaction => tx,
    :mutation => mutation
  })

# Print the question and read one line from stdin.
puts question
result = STDIN.gets.chomp
# Validate the input against the entity answer property.
if result == answer.to_s
  puts ("Fascinating, extraordinary and, when you think hard about it, " +
        "completely obvious.")
else
  puts "Don't Panic!"
end
