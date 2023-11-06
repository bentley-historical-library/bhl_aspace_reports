class BhlRestrictionsReport < AbstractReport
  
  register_report
  
  def initialize(params, job, db)
    super
  end

  def query_child_ids(archival_object_id)
    child_ids_query_string = "select id from archival_object where parent_id=#{archival_object_id}"
    child_ids = db.fetch(child_ids_query_string).map(:id)
    lowest_level_ids = []
    while !child_ids.empty?
      child_ids.each do |child_id|
        grandchild_ids_query_string = "select id from archival_object where parent_id=#{child_id}"
        grandchild_ids = db.fetch(grandchild_ids_query_string).map(:id)
        if grandchild_ids.empty?
          lowest_level_ids << child_id
        else
          child_ids.concat grandchild_ids
        end
        child_ids.delete(child_id)
      end
    end
    lowest_level_ids.join(", ")
  end

  def query_archival_object_metadata(row)
    archival_object_id = row[:archival_object_id]
    query_string = "select
                      resource.title as collection_title,
                      resource.id as resource_id,
                      resource.identifier as call_number,
                      archival_object.title as archival_object_title,
                      display_string,
                      GROUP_CONCAT(CONCAT_WS('-', date.begin, date.end) SEPARATOR '; ') as dates_normalized,
                      GROUP_CONCAT(date.expression SEPARATOR '; ') as dates,
                      parent_id,
                      BHL_GetArchivalObjectBreadcrumb(archival_object.id) as breadcrumb,
                      BHL_GetArchivalObjectContainers(archival_object.id) as containers,
                      BHL_GetArchivalObjectDigitalObject(archival_object.id) as digital_objects
                    from
                      archival_object
                    left join 
                      date on date.archival_object_id=archival_object.id
                    left join
                      resource on resource.id=archival_object.root_record_id
                    where
                      archival_object.id=#{archival_object_id}"
    archival_object = db.fetch(query_string).first
    row["resource_id"] = archival_object[:resource_id]
    row["collection_title"] = archival_object[:collection_title]
    row["call_number"] = archival_object[:call_number]
    BHLAspaceReportsHelper.fix_identifier_format_bhl(row, "call_number")
    row["breadcrumb"] = archival_object[:breadcrumb]
    row["display_string"] = archival_object[:display_string]
    row["archival_object_title"] = archival_object[:archival_object_title]
    row["dates"] = archival_object[:dates]
    row["dates_normalized"] = archival_object[:dates_normalized]
    row["containers"] = archival_object[:containers]
    row["digital_objects"] = archival_object[:digital_objects]
    # row["lowest_level_ids"] = query_child_ids(archival_object_id)
  end

  def query_restriction_type(row)
    archival_object_id = row[:archival_object_id]
    query_string = "select
                      GROUP_CONCAT(BHL_GetEnumValue(rights_restriction_type.restriction_type_id) SEPARATOR '; ') as local_restriction_type,
                      GROUP_CONCAT(rights_restriction.end SEPARATOR '; ') as local_restriction_end_date
                    from
                      rights_restriction
                    left join
                      rights_restriction_type on rights_restriction_type.rights_restriction_id=rights_restriction.id
                    where
                      rights_restriction.archival_object_id=#{archival_object_id}"
    local_restriction_metadata = db.fetch(query_string).first
    row["local_restriction_type"] = local_restriction_metadata[:local_restriction_type]
    row["local_restriction_end_date"] = local_restriction_metadata[:local_restriction_end_date]    
  end

  def fix_row(row)
    query_archival_object_metadata(row)
    query_restriction_type(row)
    BHLAspaceReportsHelper.parse_notes(row, :accessrestrict)
  end

  def query_string
    "select
      archival_object_id,
      notes as accessrestrict,
      publish
    from
      note
    where
      archival_object_id is not null
      and notes like '%accessrestrict%'"
  end

  def after_tasks
    info.delete(:repository)
  end

end
