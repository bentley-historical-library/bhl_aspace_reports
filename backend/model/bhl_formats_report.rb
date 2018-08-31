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
    submitted_formats = ASUtils.json_parse(@job.job_blob)["additional_params"]
    info[:queried_formats] = submitted_formats
    submitted_formats_array = submitted_formats.split(",")
    @formats_array = []
    submitted_formats_array.each do |submitted_format|
      @formats_array << submitted_format.downcase.strip
      @formats_array.concat permutate_format(submitted_format.downcase.strip)
    end
    @formats_array.uniq!
    regex_formats = []
    @formats_array.each do |permutated_format|
      regex_formats << "[[:<:]]#{permutated_format}[[:>:]]"
    end
    @regex_string = regex_formats.join("|")
    @literals_array = []
    @formats_array.each do |unliteralized_format|
      @literals_array << db.literal(unliteralized_format)
    end    
  end

  def fix_row(row)
    BHLAspaceReportsHelper.fix_identifier_format_bhl(row, :call_number)
    BHLAspaceReportsHelper.parse_note_content(row, :physical_details_note)
    BHLAspaceReportsHelper.parse_note_content(row, :general_note)
  end

  def query_extents()
    extent_type_condition = "GetEnumValue(extent.extent_type_id) in (#{@literals_array.join(', ')})"
    physical_details_condition = "extent.physical_details regexp #{db.literal(@regex_string)}"
    extent_match_query = "select
                            archival_object_id
                          from extent
                            where archival_object_id is not null
                            and (#{extent_type_condition} or #{physical_details_condition})"
    extent_matches = db.fetch(extent_match_query).map(:archival_object_id).uniq
    extent_matches
  end

  def query_notes(archival_object_ids, note_type)
    notes_condition = "note.notes like '%#{note_type}%' and note.notes regexp #{db.literal(@regex_string)}"
    archival_object_id_condition = archival_object_ids.empty? ? '1=1' : "archival_object_id not in (#{archival_object_ids.join(', ')})"
    note_match_query = "select
                          archival_object_id
                        from note
                          where archival_object_id is not null
                          and #{archival_object_id_condition}
                          and #{notes_condition}"
    note_matches = db.fetch(note_match_query).map(:archival_object_id).uniq
    note_matches
  end

  def query_string
    archival_object_ids = []
    archival_object_ids.concat(query_extents())
    archival_object_ids.concat(query_notes(archival_object_ids, 'physfacet'))
    archival_object_ids.concat(query_notes(archival_object_ids, 'odd'))
    archival_object_id_condition = archival_object_ids.empty? ? '1=2' : "archival_object.id in (#{archival_object_ids.join(', ')})"

    "select
      resource.title as collection_title,
      resource.identifier as call_number,
      GetArchivalObjectBreadcrumb(archival_object.id) as breadcrumb,
      archival_object.display_string as display_string,
      archival_object.component_id as component_unique_id,
      GetArchivalObjectExtent(archival_object.id) as extents,
      GetArchivalObjectNoteByType(archival_object.id, 'physfacet') as physical_details_note,
      GetArchivalObjectNoteByType(archival_object.id, 'odd') as general_note,
      GetArchivalObjectContainers(archival_object.id) as containers,
      GetArchivalObjectDigitalObject(archival_object.id) as digital_object,
      archival_object.root_record_id as resource_id,
      archival_object.id as archival_object_id
    from archival_object
      left outer join resource on resource.id=archival_object.root_record_id
    where
      archival_object.repo_id=#{db.literal(@repo_id)}
      and #{archival_object_id_condition}
    group by archival_object.id"
  end

  def after_tasks
    info.delete(:repository)
  end

end
