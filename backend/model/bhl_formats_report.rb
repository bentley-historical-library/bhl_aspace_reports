class BhlFormatsReport < AbstractReport
  
  register_report({
                    :params => [["Formats", "formatsparams", "The formats of interest"]]
                  })

  
  def permutate_format(format)
    permutations = []
    singular_format = format.gsub(/s$/, "")
    plural_format = format.gsub(/(?<!s)$/, "s")
    [singular_format, plural_format].each do |format_variation|
      permutations << format_variation
      # Replace spaces with underscores
      permutations << format_variation.gsub(" ", "_")
      # Remove spaces
      permutations << format_variation.gsub(" ", "")
      # Remove dashes
      permutations << format_variation.gsub("-", "")
      # Replace dashes with spaces
      permutations << format_variation.gsub("-", " ")
      # Replace disc with disk
      permutations << format_variation.gsub("disc", "disk")
      # Replace disk with disc
      permutations << format_variation.gsub("disk", "disc")
    end

    permutations.uniq
  end

  def initialize(params, job, db)
    super
    submitted_formats = params["submitted_formats"]
    info[:queried_formats] = submitted_formats
    submitted_formats_array = submitted_formats.split(",")
    @formats_array = []
    submitted_formats_array.each do |submitted_format|
      @formats_array << submitted_format.downcase.strip
      @formats_array.concat permutate_format(submitted_format.downcase.strip)
    end
    @formats_array.uniq!
    @formats_regex = /\b(#{Regexp.union(@formats_array).source})\b/i
    @note_columns = []
  end

  def fix_row(row)
    BHLAspaceReportsHelper.fix_identifier_format_bhl(row, :call_number)
    @note_columns.each do |note_column|
      BHLAspaceReportsHelper.parse_and_format_notes(row, note_column.to_sym)
    end
    row[:location] = query_location(row[:archival_object_id])
  end

  def query_location(archival_object_id)
    query_string = "select 
                      location.barcode as location_barcode,
                      location.building as location_building
                    from location
                      left outer join top_container_housed_at_rlshp on top_container_housed_at_rlshp.location_id=location.id
                      left outer join top_container_link_rlshp on top_container_link_rlshp.top_container_id=top_container_housed_at_rlshp.top_container_id
                      left outer join sub_container on sub_container.id=top_container_link_rlshp.sub_container_id
                      left outer join instance on instance.id=sub_container.instance_id
                    where instance.archival_object_id=#{archival_object_id}"
    locations = db.fetch(query_string)
    location_string = ''
    locations.each do |location_row|
      location = location_row.to_hash
      next unless location[:location_barcode]
      location_string = "#{location[:location_building]} #{location[:location_barcode]}"
    end
    location_string.empty? ? nil : location_string
  end


  def query_extents()
    extent_type_ids = query_extent_types()
    physical_details = query_extent_physical_details()

    extent_type_condition = extent_type_ids.empty? ? '1=2' : "extent.extent_type_id in (#{extent_type_ids.join(", ")})"
    physical_details_condition = physical_details.empty? ? '1=2' : "extent.physical_details in (#{physical_details.join(", ")})"
    extent_condition = "(#{extent_type_condition} or #{physical_details_condition})"

    extent_query_string = "select id, archival_object_id from extent where archival_object_id is not null and #{extent_condition}"
    matching_extents = db.fetch(extent_query_string).all
    archival_object_to_extent_ids = {}
    matching_extents.each do |matching_extent|
      archival_object_id = matching_extent[:archival_object_id]
      extent_id = matching_extent[:id]
      if !archival_object_to_extent_ids.include?(archival_object_id)
        archival_object_to_extent_ids[archival_object_id] = []
      end
      archival_object_to_extent_ids[archival_object_id] << extent_id
    end
    archival_object_to_extent_ids
  end

  def query_extent_types()
    extent_type_ids_query = "select distinct(extent_type_id) as id, GetEnumValue(extent_type_id) as value from extent"
    extent_types = db.fetch(extent_type_ids_query).all
    matching_extent_type_ids = extent_types.select{|e| e[:value] =~ @formats_regex}.map{|e| e[:id]}
    matching_extent_type_ids
  end

  def query_extent_physical_details()
    physical_details_query = "select distinct(physical_details) from extent where physical_details is not null"
    physical_details = db.fetch(physical_details_query).map(:physical_details)
    matching_physical_details = physical_details.select{|v| v =~ @formats_regex}.map{|v| db.literal(v)}
    matching_physical_details
  end

  def query_notes()
    note_types = ["physfacet", "odd", "dimensions", "phystech", "abstract", "materialspec", "physdesc"]
    note_types_conditions = []
    note_types.each do |note_type|
      conditional_type = '%"' + note_type + '"%'
      note_types_conditions << "note.notes like #{db.literal(conditional_type)}"
    end
    note_types_condition = "(#{note_types_conditions.join(" or ")})"
    note_content_conditions = []
    @formats_array.each do |format_|
      note_content_conditions << "LOWER(CONVERT(note.notes using utf8)) like #{db.literal("%" + format_ + "%")}"
    end
    note_content_condition = "(#{note_content_conditions.join(" or ")})"
    notes_condition = "(#{note_types_condition} and #{note_content_condition})"
    notes_query_string = "select id, archival_object_id, notes from note where archival_object_id is not null and #{notes_condition}"
    notes_query = db.fetch(notes_query_string).all
    matching_notes = notes_query.select{|r| r[:notes] =~ @formats_regex}
    archival_object_to_note_ids = {}
    matching_notes.each do |matching_note|
      archival_object_id = matching_note[:archival_object_id]
      note_id = matching_note[:id]
      if !archival_object_to_note_ids.include?(archival_object_id)
        archival_object_to_note_ids[archival_object_id] = []
      end
      archival_object_to_note_ids[archival_object_id] << note_id
    end
    archival_object_to_note_ids
  end

  def make_notes_select_list(archival_object_to_note_ids)
    max_notes = archival_object_to_note_ids.values.max.count
    selects = []
    (1..max_notes).each do |i|
      note_column = "matched_note_#{i}"
      @note_columns << note_column
      selects << "matched_notes_#{i}.notes as #{note_column}"
    end
    selects.join(", ")
  end

  def make_notes_joins(archival_object_to_note_ids)
    max_notes = archival_object_to_note_ids.values.max.count
    joins = []
    (1..max_notes).each do |i|
      note_ids = archival_object_to_note_ids.values.map{|v| v[i-1]}.compact
      if note_ids.count > 0
        note_ids_condition = "note.id in (#{note_ids.join(", ")})"
        joins << "left outer join (select id, notes, archival_object_id from note where #{note_ids_condition}) as matched_notes_#{i} on matched_notes_#{i}.archival_object_id=archival_object.id"
      end
    end
    joins.join(" ")
  end

  def make_notes_group(archival_object_to_note_ids)
    max_notes = archival_object_to_note_ids.values.max.count
    groups = []
    (1..max_notes).each do |i|
      groups << "matched_notes_#{i}.id"
    end
    groups.join(", ")
  end

  def make_extents_select_list(archival_object_to_extent_ids)
    max_extents = archival_object_to_extent_ids.values.max.count
    selects = []
    (1..max_extents).each do |i|
      extent_column = "extent_#{i}"
      format_column = "format_#{i}"
      selects << "CONCAT(extents_#{i}.number, ' ', GetEnumValue(extents_#{i}.extent_type_id)) as #{extent_column}, GetExtentPhysicalDetails(extents_#{i}.id) as #{format_column}"
    end
    selects.join(", ")
  end

  def make_extents_joins(archival_object_to_extent_ids)
    max_extents = archival_object_to_extent_ids.values.max.count
    joins = []
    (1..max_extents).each do |i|
      extent_ids = archival_object_to_extent_ids.values.map{|v| v[i-1]}.compact
      if extent_ids.count > 0
        extent_ids_condition = "extent.id in (#{extent_ids.join(", ")})"
        joins << "left outer join (select id, number, extent_type_id, archival_object_id from extent where #{extent_ids_condition}) as extents_#{i} on extents_#{i}.archival_object_id=archival_object.id"
      end
    end
    joins.join(" ")
  end

  def make_extents_group(archival_object_to_extent_ids)
    max_extents = archival_object_to_extent_ids.values.max.count
    groups = []
    (1..max_extents).each do |i|
      groups << "extents_#{i}.id"
    end
    groups.join(", ")
  end

  def query_string
    archival_object_ids = []
    archival_object_to_extent_ids = query_extents()
    extent_archival_object_ids = archival_object_to_extent_ids.keys
    archival_object_to_note_ids = query_notes()
    note_archival_object_ids = archival_object_to_note_ids.keys
    archival_object_ids.concat(extent_archival_object_ids)
    archival_object_ids.concat(note_archival_object_ids)
    archival_object_ids.uniq!

    if archival_object_to_note_ids.count > 0
      notes_select_list = ", " + make_notes_select_list(archival_object_to_note_ids)
      notes_joins = make_notes_joins(archival_object_to_note_ids)
      notes_group_list = ", " + make_notes_group(archival_object_to_note_ids)
    else
      notes_select_list = ""
      notes_joins = ""
      notes_group_list = ""
    end

    if archival_object_to_extent_ids.count > 0
      extents_select_list = make_extents_select_list(archival_object_to_extent_ids) + ", "
      extents_joins = make_extents_joins(archival_object_to_extent_ids)
      extents_group_list = ", " + make_extents_group(archival_object_to_extent_ids)
    else
      extents_select_list = ""
      extents_joins = ""
      extents_group_list = ""
    end

    archival_object_ids_condition = archival_object_ids.empty? ? '1=2' : "archival_object.id in (#{archival_object_ids.join(", ")})"

    "select
      resource.title as collection_title,
      resource.identifier as call_number,
      GetArchivalObjectContainers(archival_object.id) as containers,
      #{extents_select_list}
      archival_object.title as title,
      GROUP_CONCAT(date.expression SEPARATOR ', ') as dates,
      archival_object.id as archival_object_id,
      GetArchivalObjectBreadcrumb(archival_object.id) as breadcrumb,
      archival_object.component_id as component_unique_id,
      GetArchivalObjectDigitalObject(archival_object.id) as digital_object,
      archival_object.root_record_id as resource_id
      #{notes_select_list}
    from archival_object
      left outer join resource on resource.id=archival_object.root_record_id
      left outer join date on date.archival_object_id=archival_object.id
      #{notes_joins}
      #{extents_joins}
    where
      archival_object.repo_id=#{db.literal(@repo_id)}
      and #{archival_object_ids_condition}
    group by archival_object.id #{notes_group_list} #{extents_group_list}
    order by archival_object.root_record_id, archival_object.id"
  end

  def after_tasks
    info.delete(:repository)
  end

end
