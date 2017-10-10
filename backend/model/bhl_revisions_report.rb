class BhlRevisionsReport < AbstractReport
  
  register_report({
                    :uri_suffix => "bhl_revisions_report",
                    :description => "Bentley Historical Library Revision Statements Report",
                    :params => [["from", Date, "The start of report range"],
                                ["to", Date, "The start of report range"]]
                  })


  def initialize(params, job)
    super

    if ASUtils.present?(params["from"])
      from = params["from"]
    else
      from = Time.new(1800, 01, 01).to_s
    end

    if ASUtils.present?(params["to"])
      to = params["to"]
    else
      to = Time.now.to_s
    end

    @from = DateTime.parse(from).to_time.strftime("%Y-%m-%d %H:%M:%S")
    @to = DateTime.parse(to).to_time.strftime("%Y-%m-%d %H:%M:%S")
  end


  def title
    "Bentley Historical Library Revision Statements Report"
  end

  def headers
    ['resource_title', 'earliest_revision', 'latest_revision', 'new_or_updated']
  end

  def processor
    {'new_or_updated' => proc {|record| if record[:earliest_revision] >= @from
                                          "new"
                                        else
                                          "updated"
                                        end
                              }
    }
  end

  def scope_by_repo_id(dataset)
    # repo scope is applied in the query below
    dataset
  end

  def query(db)
    resource_ids = db[:revision_statement].where(:date=>(@from..@to)).
    left_outer_join(:resource, Sequel.qualify(:resource, :id) => Sequel.qualify(:revision_statement, :resource_id)).
    select(
      Sequel.qualify(:resource, :id)
      ).
    where(Sequel.qualify(:resource, :publish) => 1).
    group(Sequel.qualify(:resource, :id)).map(:id)

    dataset = db[:revision_statement].
    filter(:resource_id => resource_ids).
    left_outer_join(:resource, Sequel.qualify(:resource, :id) => Sequel.qualify(:revision_statement, :resource_id)).
    select(
      Sequel.qualify(:resource, :title).as(:resource_title),
      Sequel.lit('GROUP_CONCAT(revision_statement.date)').as(:revision_dates),
      Sequel.lit('MIN(revision_statement.date)').as(:earliest_revision),
      Sequel.lit('MAX(revision_statement.date)').as(:latest_revision),
      Sequel.lit('COUNT(revision_statement.id)').as(:revision_count)
      ).
    group(Sequel.qualify(:resource, :id)).
    order(Sequel.asc(Sequel.qualify(:resource, :title)))

    dataset
  end
end