class BhlUnreportedAccessionsReport < AbstractReport
  
  register_report({
                    :params => [["from", Date, "The start of report range"],
                                ["to", Date, "The end of report range"]]
                  })


  def initialize(params, job, db)
    super
    from, to = BHLAspaceReportsHelper.parse_date_params(params)
    @from = BHLAspaceReportsHelper.format_date(from)
    @to = BHLAspaceReportsHelper.format_date(to)
  end

  def fix_row(row)
    BHLAspaceReportsHelper.fix_identifier_format_bhl(row, :Accession_Identifier)
    row.delete(:accession_id)
  end

  def query_string
    source_enum_id = db[:enumeration].filter(:name=>'linked_agent_role')
                        .join(:enumeration_value, :enumeration_id => Sequel.qualify(:enumeration, :id))
                        .where(:value => 'source')
                        .select(
                          Sequel.qualify(:enumeration_value, :id)
                        ).first[:id]
    date_condition = BHLAspaceReportsHelper.format_date_condition(db.literal(@from), db.literal(@to), 'accession.accession_date')

    classification_conditions = []
    classification_fields = ["user_defined.enum_1_id", "user_defined.enum_2_id", "user_defined.enum_3_id"]
    classification_fields.each do |classification_field|
      classification_conditions << "(#{classification_field} IS NULL OR NOT BHL_GetEnumValue(#{classification_field}) IN ('MHC', 'FAC'))"
    end
    classification_condition = "(#{classification_conditions.join(' AND ')})"

    processing_status_condition = "BHL_GetEnumValue(collection_management.processing_status_id) = 'deaccessioned'"
    
    "select
      accession.id as accession_id,
      accession.identifier as Accession_Identifier,
      accession.accession_date,
      accession.created_by,
      BHL_GetAccessionClassificationsUserDefined(accession.id) as classifications,
      BHL_GetAccessionSourceName(accession.id) as donor_name,
      BHL_GetAgentDARTLID(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id) as DART_LID,
      BHL_GetAgentBEALContactID(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id) as 'Donor Contact ID'
    from accession
      left outer join user_defined on user_defined.accession_id=accession.id
      left outer join collection_management on collection_management.accession_id=accession.id
      left outer join linked_agents_rlshp on (linked_agents_rlshp.accession_id=accession.id and linked_agents_rlshp.role_id=#{source_enum_id})
    where
      accession.repo_id=#{db.literal(@repo_id)}
      and #{date_condition}
      and (#{classification_condition} or #{processing_status_condition})
    group by accession.id, linked_agents_rlshp.id
    order by accession.accession_date"
  end

  def after_tasks
    info.delete(:repository)
  end

end
