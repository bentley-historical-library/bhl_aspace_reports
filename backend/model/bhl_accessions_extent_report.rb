class BhlAccessionsExtentReport < AbstractReport
  
  register_report({
                    :params => [["from", Date, "The start of report range"],
                                ["to", Date, "The end of report range"],
                                ["Additional Parameters", "accessionsextentparams", "Additional Accessions Extent parameters"]]
                  })

  attr_reader :classification

  def initialize(params, job, db)
    super

    if ASUtils.present?(params['classification'])
      @classification = params["classification"]
    end

    from, to = BHLAspaceReportsHelper.parse_date_params(params)
    @from = BHLAspaceReportsHelper.format_date(from)
    @to = BHLAspaceReportsHelper.format_date(to)
  end

  def query_string
    classification_condition = classification ? BHLAspaceReportsHelper.format_classification_condition(db.literal(@classification)) : '1=1'
    date_condition = BHLAspaceReportsHelper.format_date_condition(db.literal(@from), db.literal(@to), 'accession.accession_date')
    
    "select
      SUM(extent.number) as totalNumber,
      GetEnumValue(extent.extent_type_id) as extentType
    from accession 
      left outer join user_defined on user_defined.accession_id=accession.id
      left outer join extent on extent.accession_id=accession.id
    where 
      accession.repo_id = #{db.literal(@repo_id)} 
      and #{date_condition}
      and #{classification_condition}
    group by extent.extent_type_id"
  end

  def after_tasks
    info.delete(:repository)
  end

end