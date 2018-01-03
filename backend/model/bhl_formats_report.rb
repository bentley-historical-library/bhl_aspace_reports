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
    end

    permutations
  end

  def initialize(params, job, db)
    super

    submitted_formats = ASUtils.json_parse(@job.job_blob)["additional_params"].split(",")
    @formats_array = []
    submitted_formats.each do |format|
      @formats_array << format.downcase.strip
      @formats_array.concat permutate_format(format.downcase.strip)
    end

    @formats_array.uniq!
    @formats_regex = Regexp.union(@formats_array)
  end


  def title
    "Bentley Historical Library Formats Report"
  end

  def headers
    ['display_string', 'extents', 'containers', 'physical_details_note', 'breadcrumb', 'call_number', 'resource_id', 'archival_object_id']
  end

  def processor
    {
      'call_number' => proc {|record| ASUtils.json_parse(record[:resource_identifier] || "[]").compact.join("-")},
      'physical_details_note' => proc {|record| if record[:notes]
                                                  ASUtils.json_parse(record[:notes])["content"][0]
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
    where(Sequel.lit('GetEnumValue(extent.extent_type_id)') => @formats_array)#.
    or(Sequel.ilike(Sequel.qualify(:extent, :physical_details), @formats_regex)).
    or(Sequel.ilike(Sequel.lit('GetArchivalObjectNoteByType(archival_object.id, "physfacet")'), @formats_regex)).
    group(Sequel.qualify(:archival_object, :id))

    dataset.where(Sequel.qualify(:archival_object, :repo_id) => @repo_id)
  end
end