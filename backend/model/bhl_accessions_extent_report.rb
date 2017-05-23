class BhlAccessionsExtentReport < AbstractReport
  
  register_report({
                    :uri_suffix => "bhl_accessions_extent_report",
                    :description => "Bentley Historical Library Accessions Extent Report",
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
    "Bentley Historical Library Accessions Extent Report"
  end

  def headers
    ['totalNumber', 'extentType']
  end

  def processor
    {
      'identifier' => proc {|record| ASUtils.json_parse(record[:identifier] || "[]").compact.join("-")}
    }
  end

  def scope_by_repo_id(dataset)
    # repo scope is applied in the query below
    dataset
  end

  def query(db)    
    dataset = db[:accession].where(:accession_date => (@from..@to)).
    left_outer_join(:extent, :accession_id => Sequel.qualify(:accession, :id)).
    select(
      Sequel.as(Sequel.lit('SUM(extent.number)'), :totalNumber),
      Sequel.as(Sequel.lit('GetEnumValue(extent.extent_type_id)'), :extentType)
        ).
    group(:extent_type_id).
    where(Sequel.qualify(:accession, :repo_id) => @repo_id)

    dataset
  end


end