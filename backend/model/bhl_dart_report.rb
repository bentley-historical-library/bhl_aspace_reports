class BhlDartReport < AbstractReport
  
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
    BHLAspaceReportsHelper.fix_identifier_format_bhl(row, :bhl_accession_id)
    row["Bentley Accession ID"] = row[:bhl_accession_id]
    row.delete(:id)
    row.delete(:bhl_accession_id)
  end

  def query_string
    source_enum_id = db[:enumeration].filter(:name=>'linked_agent_role')
                        .join(:enumeration_value, :enumeration_id => Sequel.qualify(:enumeration, :id))
                        .where(:value => 'source')
                        .select(
                          Sequel.qualify(:enumeration_value, :id)
                        ).first[:id]
    
    date_condition = BHLAspaceReportsHelper.format_date_condition(db.literal(@from), db.literal(@to), 'accession.accession_date')
    classification_condition = "(BHL_GetEnumValue(user_defined.enum_1_id) in ('MHC', 'FAC')
                                or BHL_GetEnumValue(user_defined.enum_2_id) in ('MHC', 'FAC')
                                or BHL_GetEnumValue(user_defined.enum_3_id) in ('MHC', 'FAC'))"
    
    processing_status_condition = "IF(collection_management.processing_status_id, BHL_GetEnumValue(collection_management.processing_status_id) NOT IN ('deaccessioned', 'discarded'), 1)"

    "select CONCAT_WS(' ', BHL_GetEnumValue(user_defined.enum_1_id), BHL_GetEnumValue(user_defined.enum_2_id), BHL_GetEnumValue(user_defined.enum_3_id)) AS CLASSIFICATIONS,
      accession.id,
      BHL_GetAgentDARTLID(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id) as DART_LID,
      BHL_GetAgentLastName(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id) as Lastname,
      '' as Middle,
      BHL_GetAgentRestOfName(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id) as Firstname,
      BHL_GetAgentSuffix(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id) as Suffix,
      BHL_GetAgentTitle(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id) as Title,
      BHL_GetAgentOrganizationOrUnit(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id) as OrganizationOrUnit,
      BHL_GetAgentAddress1(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id) as Street1,
      BHL_GetAgentAddress2(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id) as Street2,
      BHL_GetAgentCity(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id) as City,
      BHL_GetAgentState(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id) as St,
      BHL_GetAgentZipCode(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id) as Zip,
      BHL_GetAgentEmail(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id) as EMAIL,
      BHL_GetAgentPhone(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id) as PHONE,
      '1' as 'donation amt',
      accession.accession_date as 'credit date',
      'historical records.' as 'item description',
      '897100' as designation,
      '' as comment,
      '' as concatenated,
      '' as note,
      '' as 'note type',
      'Miscellaneous' as 'GIK subtype',
      '1' as '# of units',
      '' as 'item name',
      '' as 'REV type',
      'Do not Receipt' as 'do not receipt',
      BHL_GetAgentBEALContactID(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id) as 'Constituent alt lookup ID',
      accession.identifier as bhl_accession_id
    from accession
      left outer join user_defined on user_defined.accession_id=accession.id
      left outer join collection_management on collection_management.accession_id=accession.id
      left outer join linked_agents_rlshp on (linked_agents_rlshp.accession_id=accession.id and linked_agents_rlshp.role_id=#{source_enum_id})
    where
      accession.repo_id=#{db.literal(@repo_id)}
      and #{date_condition}
      and #{classification_condition}
      and #{processing_status_condition}
      and linked_agents_rlshp.id is not null
    group by accession.id, linked_agents_rlshp.id, user_defined.enum_1_id, user_defined.enum_2_id, user_defined.enum_3_id
    order by accession.accession_date"
  end


  def query
    # job.write_output('query_string = ' )
    # job.write_output(query_string)
    db.fetch(query_string)
  end

  def after_tasks
    info.delete(:repository)
  end

end
