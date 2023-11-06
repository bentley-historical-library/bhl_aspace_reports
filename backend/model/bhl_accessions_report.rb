class BhlAccessionsReport < AbstractReport
  
  register_report({
                    :params => [["from", Date, "The start of report range"],
                                ["to", Date, "The end of report range"],
                                ["Additional Parameters", "accessionsparams", "Additional Accession parameters"]]
                  })

  
  include JSONModel

  attr_reader :processing_status, :processing_priority, :classification, :donor_uri, :donor_type, :donor_id, :field_archivist
  
  def initialize(params, job, db)
    super
    if ASUtils.present?(params['processing_status'])
      @processing_status = params["processing_status"]
    end

    if ASUtils.present?(params['processing_priority'])
      @processing_priority = params["processing_priority"]
    end

    if ASUtils.present?(params['classification'])
      @classification = params["classification"]
    end

    if ASUtils.present?(params['field_archivist'])
      @field_archivist = params["field_archivist"]
    end

    from, to = BHLAspaceReportsHelper.parse_date_params(params)
    @from = BHLAspaceReportsHelper.format_date(from)
    @to = BHLAspaceReportsHelper.format_date(to)

    if ASUtils.present?(params["donor"])
      @donor_uri = params["donor"]["ref"]
      if donor_uri.include?("people")
        @donor_type = "agent_person"
      elsif donor_uri.include?("families")
        @donor_type = "agent_family"
      elsif donor_uri.include?("corporate_entities")
        @donor_type = "agent_corporate_entity"
      end
      @donor_id = JSONModel(donor_type).id_for(donor_uri)
    end

    # TO DO
    # extent: calculate a total
  end

  def fix_field_archivists(row)
    field_archivists = row[:staff_received] ? row[:staff_received] : row[:field_archivists]
    field_archivists
  end

  def fix_row(row)
    BHLAspaceReportsHelper.fix_identifier_format_bhl(row)
    row[:location_barcoded] = query_location(row[:accession_id])
    row[:location] = row[:location_barcoded] ? row[:location_barcoded] : row[:location_user_defined]
    row[:field_archivists] = fix_field_archivists(row)
    row.delete(:staff_received)
    row.delete(:location_barcoded)
    row.delete(:location_user_defined)
  end

def query_location(accession_id)
    query_string = "select 
                      location.barcode as location_barcode,
                      location.building as location_building
                    from location
                      left outer join top_container_housed_at_rlshp on top_container_housed_at_rlshp.location_id=location.id
                      left outer join top_container_link_rlshp on top_container_link_rlshp.top_container_id=top_container_housed_at_rlshp.top_container_id
                      left outer join sub_container on sub_container.id=top_container_link_rlshp.sub_container_id
                      left outer join instance on instance.id=sub_container.instance_id
                    where instance.accession_id=#{accession_id}"
    locations = db.fetch(query_string)
    location_string = ''
    if locations.count > 10
      location_string = "Too many locations to include in report"
    else
      locations.each do |location_row|
        location = location_row.to_hash
        next unless location[:location_barcode]
        location_string += ', ' if location_string != ''
        location_string += "#{location[:location_building]} #{location[:location_barcode]}"
      end
    end
    location_string.empty? ? nil : location_string
  end

  def query_string
    source_enum_id = db[:enumeration].filter(:name=>'linked_agent_role')
                        .join(:enumeration_value, :enumeration_id => Sequel.qualify(:enumeration, :id))
                        .where(:value => 'source')
                        .select(
                          Sequel.qualify(:enumeration_value, :id)
                        ).first[:id]

    custody_transfer_id = db[:enumeration].filter(:name=>'event_event_type')
                          .join(:enumeration_value, :enumeration_id => Sequel.qualify(:enumeration, :id))
                          .where(:value => 'custody_transfer')
                          .select(
                            Sequel.qualify(:enumeration_value, :id)
                          ).first[:id]

    field_archivist_id = db[:enumeration].filter(:name => 'linked_agent_event_roles')
                          .join(:enumeration_value, :enumeration_id => Sequel.qualify(:enumeration, :id))
                          .where(:value => 'field_archivist')
                          .select(
                            Sequel.qualify(:enumeration_value, :id)
                          ).first[:id]

    enum_processing_status_id = db[:enumeration][:name => 'collection_management_processing_status'][:id]
    enum_processing_priority_id = db[:enumeration][:name => 'collection_management_processing_priority'][:id]


    date_condition = BHLAspaceReportsHelper.format_date_condition(db.literal(@from), db.literal(@to), 'accession.accession_date')    
    processing_status_condition = processing_status ? BHLAspaceReportsHelper.format_enum_condition('enumvals_processing_status', db.literal(processing_status)) : '1=1'
    processing_priority_condition = processing_priority ? BHLAspaceReportsHelper.format_enum_condition('enumvals_processing_priority', db.literal(processing_priority)) : '1=1'    
    classification_condition = classification ? BHLAspaceReportsHelper.format_classification_condition(db.literal(@classification)) : '1=1'                                
    donor_condition = donor_uri ? "source_linked_agents_rlshp.#{donor_type}_id=#{db.literal(@donor_id)}" : '1=1'    
    field_archivist_condition = field_archivist ? "name_person.sort_name=#{db.literal(@field_archivist)}" : '1=1'

    
    "select 
      accession.id as accession_id,
      accession.accession_date,
      accession.identifier,
      accession.content_description,
      GROUP_CONCAT(user_defined.string_1 SEPARATOR '; ') as staff_received,
      BHL_GetAccessionFieldArchivists(accession.id) as field_archivists,
      BHL_GetAccessionLocationUserDefined(accession.id) as location_user_defined,
      BHL_GetAccessionProcessingStatus(accession.id) as processing_status,
      BHL_GetAccessionProcessingPriority(accession.id) as processing_priority,
      BHL_GetAccessionClassificationsUserDefined(accession.id) as classification,
      BHL_GetAccessionExtentNumberType(accession.id) as extent_number_type,
      BHL_GetAccessionSourceName(accession.id) as donor_name,
      BHL_GetAccessionDonorNumbers(accession.id) as donor_number
    from accession
      left outer join linked_agents_rlshp as source_linked_agents_rlshp on (source_linked_agents_rlshp.accession_id=accession.id and source_linked_agents_rlshp.role_id=#{source_enum_id})
      left outer join collection_management on collection_management.accession_id=accession.id
      left outer join event_link_rlshp on event_link_rlshp.accession_id=accession.id
      left outer join event on (event.id=event_link_rlshp.event_id and event.event_type_id=#{custody_transfer_id})
      left outer join linked_agents_rlshp as field_archivist_linked_agents_rlshp on (field_archivist_linked_agents_rlshp.event_id=event.id and field_archivist_linked_agents_rlshp.role_id=#{field_archivist_id})
      left outer join name_person on name_person.agent_person_id=field_archivist_linked_agents_rlshp.agent_person_id
      left outer join enumeration_value as enumvals_processing_status on (enumvals_processing_status.enumeration_id=#{enum_processing_status_id} and collection_management.processing_status_id=enumvals_processing_status.id)
      left outer join enumeration_value as enumvals_processing_priority on (enumvals_processing_priority.enumeration_id=#{enum_processing_priority_id} and collection_management.processing_priority_id=enumvals_processing_priority.id)
      left outer join user_defined on user_defined.accession_id=accession.id
    where
      accession.repo_id = #{db.literal(@repo_id)} 
      and #{date_condition} 
      and #{processing_status_condition} 
      and #{processing_priority_condition} 
      and #{classification_condition} 
      and #{donor_condition} 
      and #{field_archivist_condition}
    group by accession.id
    order by accession_date"
  end

  def after_tasks
    info.delete(:repository)
  end

end
