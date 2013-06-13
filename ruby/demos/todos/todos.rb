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

require 'bundler'
Bundler.require

config = YAML.load File.open "datastore.yml"

# Uncomment the next line to see what requests are being sent to the GCD API in your console
# HttpLogger.logger = Logger.new STDOUT

# Uncomment the next line if you want to see what headers are being sent too.
# HttpLogger.log_headers = true

$client = ActiveDatastore::Client.new config["SERVICE_ACCOUNT"], File.open(config["PRIVATE_KEY_FILE"]).read
$dataset = ActiveDatastore::Dataset.new config["DATASET_ID"], $client


get '/' do
	batch = $dataset.run_query({
		query: {
			kinds: [{name: 'Todo'}],
			filter: {
				propertyFilter: {
					property: { name: '__key__'},
					operator: 'hasAncestor',
					value: {
						keyValue: {
							path: [{kind: 'TodoList', name: 'default'}]
						}
					}
				}
			}
		}
	}).data.batch

	todos = (batch["entityResults"] || []).map do |e|
		properties = e.entity.properties
		content = properties.content.values.first.stringValue
		created = properties.created.values.first.dateTimeValue
		"#{created}: <strong>#{content}</strong>"
	end

	form = <<-EOS
		<form method="POST">
			<input name="todo"/>
			<button type="submit">Create Todo</button>
		</form>
	EOS

	"<h1>Todos</h1><ul>#{todos.map{|t| "<li>#{t}</li>"}.join}</ul>" + form
end

post '/' do
	todo = params[:todo]
	new_todo_id = $dataset.blind_write({
		mutation: {
			insertAutoId: [
				{
					key: {path: [{kind: 'TodoList', name: 'default'}, {kind: 'Todo'}]},
					properties: {
						content: {values: [{stringValue: todo}]},
						created: {values: [{dateTimeValue: Time.now.utc.to_datetime.rfc3339}]}
					}
				}
			]
		}
	}).data.mutationResult.insertAutoIdKeys.first.path.inspect

	"<p>Your todo has been created with key: #{new_todo_id}. Click <strong><a href='/'>here</a></strong> to go back to your list.</p>"

end
