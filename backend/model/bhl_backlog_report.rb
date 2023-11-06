class BhlBacklogReport < AbstractReport
  
  register_report({
                    :params => [["from", Date, "The start of report range"],
                                ["to", Date, "The end of report range"]]
                  })

  
  include JSONModel
  
  def initialize(params, job, db)
    super
    from, to = BHLAspaceReportsHelper.parse_date_params(params)
    @from = BHLAspaceReportsHelper.format_date(from)
    @to = BHLAspaceReportsHelper.format_date(to)

  end

  def fix_row(row)
    BHLAspaceReportsHelper.fix_identifier_format_bhl(row)
    row.delete(:accession_id)
  end

  def query_string
    source_enum_id = db[:enumeration].filter(:name=>'linked_agent_role')
                        .join(:enumeration_value, :enumeration_id => Sequel.qualify(:enumeration, :id))
                        .where(:value => 'source')
                        .select(
                          Sequel.qualify(:enumeration_value, :id)
                        ).first[:id]

    enum_processing_status_id = db[:enumeration][:name => 'collection_management_processing_status'][:id]

    date_condition = BHLAspaceReportsHelper.format_date_condition(db.literal(@from), db.literal(@to), 'accession.accession_date')    
    processing_status_condition = BHLAspaceReportsHelper.format_enum_condition('enumvals_processing_status', db.literal('backlog'))
    
    classification_condition = "(BHL_GetEnumValue(user_defined.enum_1_id) in ('MHC', 'FAC')
                                or BHL_GetEnumValue(user_defined.enum_2_id) in ('MHC', 'FAC')
                                or BHL_GetEnumValue(user_defined.enum_3_id) in ('MHC', 'FAC'))"

    
    "select 
      accession.id as accession_id,
      BHL_GetAccessionSourceName(accession.id) as donor_name,
      BHL_GetAccessionDonorNumbers(accession.id) as donor_number,
      accession.identifier,
      accession.accession_date,
      accession.content_description as 'Description',
      BHL_GetAccessionExtentNumberType(accession.id) as 'extent',
      BHL_GetAccessionClassificationsUserDefined(accession.id) as classification
    from accession
      left outer join linked_agents_rlshp as source_linked_agents_rlshp on (source_linked_agents_rlshp.accession_id=accession.id and source_linked_agents_rlshp.role_id=#{source_enum_id})
      left outer join collection_management on collection_management.accession_id=accession.id
      left outer join enumeration_value as enumvals_processing_status on (enumvals_processing_status.enumeration_id=#{enum_processing_status_id} and collection_management.processing_status_id=enumvals_processing_status.id)
      left outer join user_defined on user_defined.accession_id=accession.id
    where
      accession.repo_id = #{db.literal(@repo_id)} 
      and #{date_condition} 
      and #{processing_status_condition} 
      and #{classification_condition} 
    group by accession.id
    order by donor_name, accession_date"
  end

  def after_tasks
    info.delete(:repository)
  end

end
