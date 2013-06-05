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