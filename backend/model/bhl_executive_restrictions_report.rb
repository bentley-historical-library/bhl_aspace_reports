class BhlExecutiveRestrictionsReport < AbstractReport
  
  register_report
  
  def initialize(params, job, db)
    super
  end

  def query_archival_object_metadata(row)
    archival_object_metadata = {}
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
                      GetArchivalObjectBreadcrumb(archival_object.id) as breadcrumb
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
    parent_id = archival_object[:parent_id]
    most_proximate_date = ""
    most_proximate_normalized = ""
    depth = 0
    if archival_object[:dates].nil? | (archival_object[:dates].to_s.downcase == "undated")
      while parent_id && most_proximate_date.empty?
        depth += 1
        parent_query = "select
                          parent_id,
                          GROUP_CONCAT(CONCAT_WS('-', date.begin, date.end) SEPARATOR '; ') as parent_dates_normalized,
                          GROUP_CONCAT(date.expression SEPARATOR '; ') as parent_dates
                        from
                          archival_object
                        left join
                          date on date.archival_object_id=archival_object.id
                        where
                          archival_object.id=#{parent_id}"
        parent_result = db.fetch(parent_query).first
        parent_dates = parent_result[:parent_dates]
        if !parent_dates.nil?
          most_proximate_date = parent_dates
          most_proximate_normalized = parent_result[:parent_dates_normalized]
        else
          parent_id = parent_result[:parent_id]
        end
      end
    end
    row["parent_dates"] = most_proximate_date
    row["parent_dates_normalized"] = most_proximate_normalized
    row["parent_depth"] = depth.to_s
    row["parent_id"] = parent_id.to_s
  end

  def fix_row(row)
    query_archival_object_metadata(row)
    BHLAspaceReportsHelper.parse_notes(row, :accessrestrict)
  end

  def query_string
    "select
      archival_object_id,
      notes as accessrestrict
    from
      note
    where
      archival_object_id is not null
      and publish=1
      and notes like '%accessrestrict%'
      and LOWER(CONVERT(notes using utf8)) like '%er restrict%'"
  end

  def after_tasks
    info.delete(:repository)
  end

end
