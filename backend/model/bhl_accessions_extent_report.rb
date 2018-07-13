class BhlAccessionsExtentReport < AbstractReport
  
  register_report({
                    :params => [["from", Date, "The start of report range"],
                                ["to", Date, "The start of report range"],
                                ["Additional Parameters", "accessionsextentparams", "Additional Accessions Extent parameters"]]
                  })

  attr_reader :classification

  # Workaround to avoid new ArchivesSpace csv_response
  def to_csv
    CSV.generate do |csv|
      csv << headers
      each do |row|
        csv << headers.map{|header| row[header]}
      end
    end
  end

  def initialize(params, job, db)
    super

    if ASUtils.present?(params['classification'])
      @classification = params["classification"]
    end

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
    {}
  end

  def scope_by_repo_id(dataset)
    # repo scope is applied in the query below
    dataset
  end

  def query   
    dataset = db[:accession].where(:accession_date => (@from..@to)).
    left_outer_join(:user_defined, :accession_id => Sequel.qualify(:accession, :id)).
    left_outer_join(:extent, :accession_id => Sequel.qualify(:accession, :id)).
    select(
      Sequel.as(Sequel.lit('SUM(extent.number)'), :totalNumber),
      Sequel.as(Sequel.lit('GetEnumValue(extent.extent_type_id)'), :extentType)
        ).
    group(:extent_type_id).
    where(Sequel.qualify(:accession, :repo_id) => @repo_id)

    if classification
      dataset = dataset.where(Sequel.lit('GetEnumValue(user_defined.enum_1_id)') => @classification).or(Sequel.lit('GetEnumValue(user_defined.enum_2_id)') => @classification).or(Sequel.lit('GetEnumValue(user_defined.enum_3_id)') => @classification)
    end

    dataset
  end
end