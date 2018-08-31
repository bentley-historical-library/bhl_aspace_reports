class ArchivesSpaceService < Sinatra::Base

  Endpoint.get('/repositories/:repo_id/bhl_aspace_reports/field_archivists')
    .description("Get a list of field archivists")
    .params(["repo_id", :repo_id])
    .permissions([:view_repository])
    .returns([200, "Field Archivists"]) \
  do
    json_response(BHLAspaceReportsHelper.field_archivists())
  end

end
