class BhlFormatsReport < AbstractReport
  
  register_report({
                    :params => [["Formats", "formatsparams", "The formats of interest"]]
                  })

  def initialize(params, job, db)
    super

    submitted_formats = JSON.parse(@job.job_blob)["additional_params"]
    submitted_formats = submitted_formats.gsub("\n", ", ")
    submitted_formats = submitted_formats.split(%r{,\s*})
    submitted_formats = submitted_formats.map{|format| format.downcase}

    submitted_formats.each do |format|
      if format.end_with?("s")
        singular_format = format.gsub(/s$/, "")
        if !submitted_formats.include?(singular_format)
          submitted_formats << singular_format
        end
      else
        plural_format = format + "s"
        if !submitted_formats.include?(plural_format)
          submitted_formats << plural_format
        end
      end

      if format.include?(" ")
        underscore_format = format.gsub(" ", "_")
        if !submitted_formats.include?(underscore_format)
          submitted_formats << underscore_format
        end

        no_space_format = format.gsub(" ", "")
        if !submitted_formats.include?(no_space_format)
          submitted_formats << no_space_format
        end
      end

      if format.include?("-")
        space_format = format.gsub("-", " ")
        if !submitted_formats.include?(space_format)
          submitted_formats << space_format
        end

        no_dash_format = format.gsub("-", "")
        if !submitted_formats.include?(no_dash_format)
          submitted_formats << no_dash_format
        end
      end

    end
    @formats_array = submitted_formats
    @formats_regex = Regexp.new submitted_formats.join("|")
  end


  def title
    "Bentley Historical Library Formats Report"
  end

  def headers
    ['display_string', 'extents', 'physical_details_note', 'containers', 'breadcrumb', 'call_number', 'resource_id', 'archival_object_id']
  end

  def processor
    {
      'call_number' => proc {|record| ASUtils.json_parse(record[:resource_identifier] || "[]").compact.join("-")},
      'physical_details_note' => proc {|record| if record[:notes] && JSON.parse(record[:notes])["type"] == "physfacet"
                                                  JSON.parse(record[:notes])["content"][0]
                                                else
                                                  ""
                                                end
                                      }

    }
  end

  def scope_by_repo_id(dataset)
    # repo scope is applied in the query below
    dataset
  end

  def query    
    dataset = db[:archival_object].
    left_outer_join(:extent, Sequel.qualify(:extent, :archival_object_id) => Sequel.qualify(:archival_object, :id)).
    left_outer_join(:note, Sequel.qualify(:note, :archival_object_id) => Sequel.qualify(:archival_object, :id)).
    select(
      Sequel.qualify(:archival_object, :id).as(:archival_object_id),
      Sequel.qualify(:archival_object, :root_record_id).as(:resource_id),
      Sequel.qualify(:archival_object, :display_string).as(:display_string),
      Sequel.as(Sequel.lit('GetResourceCallNumber(archival_object.root_record_id)'), :resource_identifier),
      Sequel.as(Sequel.lit('GetArchivalObjectExtent(archival_object.id)'), :extents),
      Sequel.as(Sequel.lit('GetArchivalObjectNoteByType(archival_object.id, "physfacet")'), :notes),
      Sequel.as(Sequel.lit('GetArchivalObjectContainers(archival_object.id)'), :containers),
      Sequel.as(Sequel.lit('GetArchivalObjectBreadcrumb(archival_object.id)'), :breadcrumb)
      ).
    where(Sequel.lit('GetEnumValue(extent.extent_type_id)') => @formats_array).
    or(Sequel.ilike(Sequel.qualify(:extent, :physical_details), @formats_regex)).
    or(Sequel.ilike(Sequel.lit('GetArchivalObjectNoteByType(archival_object.id, "physfacet")'), @formats_regex)).
    group(Sequel.qualify(:archival_object, :id))

    dataset.where(Sequel.qualify(:archival_object, :repo_id) => @repo_id)
  end
end