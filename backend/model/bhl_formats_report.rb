require 'csv'
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

    permutations.uniq
  end

  # Workaround to avoid new ArchivesSpace csv_response
  def to_csv
    CSV.generate do |csv|
      csv << headers
      each do |row|
        csv << headers.map{|header| row[header]}
      end
    end
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
    ['collection_title', 'display_string', 'component_unique_id', 'extents', 'containers', 'digital_object', 'physical_details_note', 'breadcrumb', 'call_number', 'resource_id', 'archival_object_id']
  end

  def processor
    {
      'call_number' => proc {|record| ASUtils.json_parse(record[:resource_identifier] || "[]").compact.join("-")},
      'physical_details_note' => proc {|record| if record[:physical_details_note]
                                        ASUtils.json_parse(record[:physical_details_note])["content"][0]
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
    archival_object_ids = []

    extent_matches = db[:extent].
    exclude(:archival_object_id => nil).
    where(Sequel.lit('GetEnumValue(extent.extent_type_id) IN :formats', formats: @formats_array) | Sequel.ilike(:physical_details, @formats_regex)).
    map(:archival_object_id).uniq
    archival_object_ids.concat(extent_matches)

    note_matches = db[:note].
    exclude(:archival_object_id => nil).
    exclude(:archival_object_id => archival_object_ids).
    where(Sequel.like(:notes, "%physfacet%")).
    where(Sequel.ilike(:notes, @formats_regex)).
    map(:archival_object_id).uniq
    archival_object_ids.concat(note_matches)

    dataset = db[:archival_object].
    filter(Sequel.qualify(:archival_object, :id) => archival_object_ids).
    filter(Sequel.qualify(:archival_object, :repo_id) => @repo_id).
    left_outer_join(:resource, Sequel.qualify(:resource, :id) => Sequel.qualify(:archival_object, :root_record_id)).
    select(
      Sequel.qualify(:archival_object, :id).as(:archival_object_id),
      Sequel.qualify(:archival_object, :component_id).as(:component_unique_id),
      Sequel.qualify(:archival_object, :root_record_id).as(:resource_id),
      Sequel.qualify(:archival_object, :display_string).as(:display_string),
      Sequel.qualify(:resource, :title).as(:collection_title),
      Sequel.qualify(:resource, :identifier).as(:resource_identifier),
      Sequel.as(Sequel.lit('GetArchivalObjectDigitalObject(archival_object.id)'), :digital_object),
      Sequel.as(Sequel.lit('GetArchivalObjectExtent(archival_object.id)'), :extents),
      Sequel.as(Sequel.lit('GetArchivalObjectNoteByType(archival_object.id, "physfacet")'), :physical_details_note),
      Sequel.as(Sequel.lit('GetArchivalObjectContainers(archival_object.id)'), :containers),
      Sequel.as(Sequel.lit('GetArchivalObjectBreadcrumb(archival_object.id)'), :breadcrumb)
      ).
    group(Sequel.qualify(:archival_object, :id))

    dataset
  end
end