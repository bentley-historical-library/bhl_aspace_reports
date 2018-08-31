class BhlRevisionsReport < AbstractReport
  
  register_report({
                    :params => [["from", Date, "The start of report range"],
                                ["to", Date, "The start of report range"]]
                  })


  def initialize(params, job, db)
    super
    @from, @to = BHLAspaceReportsHelper.parse_date_params(params)
  end

  def fix_row(row)
    row[:new_or_updated] = (row[:earliest_revision] >= @from) ? "new" : "updated"
  end

  def query_revision_statements
    date_condition = BHLAspaceReportsHelper.format_date_condition(db.literal(@from), db.literal(@to), 'revision_statement.date')
    query_string = "select
                      revision_statement.resource_id
                    from revision_statement
                    where #{date_condition}
                    group by revision_statement.resource_id"
    resource_ids = db.fetch(query_string).map(:resource_id)
    resource_ids.uniq
  end

  def query_string
    resource_ids = query_revision_statements()
    resource_ids_condition = "resource.id in (#{resource_ids.join(', ')})"

    "select
      resource.title as resource_title,
      MIN(revision_statement.date) as earliest_revision,
      MAX(revision_statement.date) as latest_revision
    from resource left outer join revision_statement on revision_statement.resource_id=resource.id
    where
      resource.repo_id=#{db.literal(@repo_id)}
      and resource.publish=1
      and #{resource_ids_condition}
    group by resource.id
    order by resource.title"
  end

  def after_tasks
    info.delete(:repository)
  end
  
end