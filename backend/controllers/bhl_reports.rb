class ArchivesSpaceService < Sinatra::Base

	Endpoint.get('/classifications/classification_names')
		.description("Get a list of classification names")
		.permissions([])
		.returns([200, "Classification names"]) \
	do

	json_response(Classification.map([:identifier, :identifier]))
	end
end
