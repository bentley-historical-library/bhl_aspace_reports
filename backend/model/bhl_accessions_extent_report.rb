class BhlAccessionsExtentReport < AbstractReport
  
  register_report({
                    :params => [["from", Date, "The start of report range"],
                                ["to", Date, "The end of report range"],
                                ["Additional Parameters", "accessionsextentparams", "Additional Accessions Extent parameters"]]
                  })

  attr_reader :classification, :processing_status

  def initialize(params, job, db)
    super

    if ASUtils.present?(params['processing_status'])
      @processing_status = params["processing_status"]
    end

    if ASUtils.present?(params['classification'])
      @classification = params["classification"]
    end

    from, to = BHLAspaceReportsHelper.parse_date_params(params)
    @from = BHLAspaceReportsHelper.format_date(from)
    @to = BHLAspaceReportsHelper.format_date(to)
  end

  def query_string
    processing_status_condition = processing_status ? BHLAspaceReportsHelper.format_enum_condition('enumvals_processing_status', db.literal(processing_status)) : '1=1'
    classification_condition = classification ? BHLAspaceReportsHelper.format_classification_condition(db.literal(@classification)) : '1=1'
    date_condition = BHLAspaceReportsHelper.format_date_condition(db.literal(@from), db.literal(@to), 'accession.accession_date')

    enum_processing_status_id = db[:enumeration][:name => 'collection_management_processing_status'][:id]
    
    "select
      SUM(extent.number) as totalNumber,
      GetEnumValue(extent.extent_type_id) as extentType
    from accession 
      left outer join user_defined on user_defined.accession_id=accession.id
      left outer join collection_management on collection_management.accession_id=accession.id
      left outer join enumeration_value as enumvals_processing_status on (enumvals_processing_status.enumeration_id=#{enum_processing_status_id} and collection_management.processing_status_id=enumvals_processing_status.id)
      left outer join extent on extent.accession_id=accession.id
    where 
      accession.repo_id = #{db.literal(@repo_id)}
      and #{processing_status_condition} 
      and #{date_condition}
      and #{classification_condition}
    group by extent.extent_type_id"
  end

  def after_tasks
    info.delete(:repository)
  end

end
