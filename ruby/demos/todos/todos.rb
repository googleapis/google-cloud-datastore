require 'sinatra'

require 'active_datastore'

config = YAML.load File.open "datastore.yml"

$client = ActiveDatastore::Client.new config["EMAIL"], File.open(config["KEY_PATH"]).read
$dataset = ActiveDatastore::Dataset.new config["DATASET_ID"], $client


get '/' do
	todos = $dataset.run_query({
		query: {
			kinds: [{name: 'Todo'}]
		}
	}).data.batch.entityResults.map do |e|
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
					key: {path: [{kind: 'Todo'}]},
					properties: {
						content: {values: [{stringValue: todo}]},
						created: {values: [{dateTimeValue: Time.now.utc.to_datetime.rfc3339}]}
					}
				}
			]
		}
	}).data.mutationResult.insertAutoIdKeys.first.path.first.id

	"<p>Your todo has been created with ID: #{new_todo_id}. Click <strong><a href='/'>here</a></strong> to go back to your list.</p>"

end