#Todo Demo

The demo requires [Sinatra](http://www.sinatrarb.com/), a simple web server and the [ActiveDatastore](https://github.com/sudhirj/active_datastore) gem. Install them from the Gemfile using

	bundle install

Get your credentials for the Google Cloud Datastore by following the instructions [here](https://developers.google.com/datastore/docs/activate#google_cloud_datastore_from_other_platforms). Then make a copy of the `datastore.yml.example` file, call it `datastore.yml` and put your credentials into it.

To start the server, run

	ruby todos.rb

Have fun!