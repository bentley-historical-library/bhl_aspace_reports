class BhlAccessionsReport < AbstractReport
  
  register_report({
                    :params => [["from", Date, "The start of report range"],
                                ["to", Date, "The end of report range"],
                                ["Additional Parameters", "accessionsparams", "Additional Accession parameters"]]
                  })

  
  include JSONModel

  attr_reader :processing_status, :processing_priority, :classification, :donor_uri, :donor_type, :donor_id, :field_archivist

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

    if ASUtils.present?(params["from"])
      from = params["from"]
    else
      from = Time.new(1800, 01, 01).to_s
    end

    if ASUtils.present?(params["to"])
      to = params["to"]
    else
      to = Time.now.to_s
    end

    @from = DateTime.parse(from).to_time.strftime("%Y-%m-%d %H:%M:%S")
    @to = DateTime.parse(to).to_time.strftime("%Y-%m-%d %H:%M:%S")

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

  def title
    "Bentley Historical Library Accessions Report"
  end

  def headers
    ['identifier', 'donor_name', 'donor_number', 'accession_date', 'content_description', 'processing_status', 'processing_priority', 'classifications', 'extent_number_type', 'location', 'field_archivists']
  end

  def processor
    {
      'identifier' => proc {|record| ASUtils.json_parse(record[:identifier] || "[]").compact.join("-")},
      'field_archivists' => proc {|record| record[:staff_received] ? record[:staff_received] : record[:field_archivists]}
    }
  end

  def scope_by_repo_id(dataset)
    # repo scope is applied in the query below
    dataset
  end

  def query
    source_enum_id = db[:enumeration].filter(:name=>'linked_agent_role').join(:enumeration_value, :enumeration_id => Sequel.qualify(:enumeration, :id)).where(:value => 'source').all[0][:id]
    custody_transfer_id = db[:enumeration].filter(:name=>'event_event_type').join(:enumeration_value, :enumeration_id => Sequel.qualify(:enumeration, :id)).where(:value => 'custody_transfer').all[0][:id]
    field_archivist_id = db[:enumeration].filter(:name => 'linked_agent_event_roles').join(:enumeration_value, :enumeration_id => Sequel.qualify(:enumeration, :id)).where(:value => 'field_archivist').all[0][:id]
    
    dataset = db[:accession].where(:accession_date => (@from..@to)).
    left_outer_join(:linked_agents_rlshp, [[:accession_id, Sequel.qualify(:accession, :id)], [:role_id, source_enum_id]], :table_alias => :source_linked_agents_rlshp).
    left_outer_join(:collection_management, :accession_id => Sequel.qualify(:accession, :id)).
    left_outer_join(:event_link_rlshp, :accession_id => Sequel.qualify(:accession, :id)).
    left_outer_join(:event, [[Sequel.qualify(:event, :id), Sequel.qualify(:event_link_rlshp, :event_id)], [:event_type_id, custody_transfer_id]]).
    left_outer_join(:linked_agents_rlshp, [[:event_id, Sequel.qualify(:event, :id)], [:role_id, field_archivist_id]], :table_alias => :field_archivist_linked_agents_rlshp).
    left_outer_join(:name_person, :agent_person_id => Sequel.qualify(:field_archivist_linked_agents_rlshp, :agent_person_id)).
    join(:enumeration,
        {
          :name => "collection_management_processing_status"
        },
        {
          :table_alias => :enum_processing_status
        }).
    join(:enumeration,
        {
         :name => 'collection_management_processing_priority'
        },
        {
         :table_alias => :enum_processing_priority
        }).
    left_outer_join(:enumeration_value,
        {
          Sequel.qualify(:enumvals_processing_status, :enumeration_id) => Sequel.qualify(:enum_processing_status, :id),
          Sequel.qualify(:collection_management, :processing_status_id) => Sequel.qualify(:enumvals_processing_status, :id),
        },
        {
          :table_alias => :enumvals_processing_status
        }).
    left_outer_join(:enumeration_value,
        {
          Sequel.qualify(:enumvals_processing_priority, :enumeration_id) =>  Sequel.qualify(:enum_processing_priority, :id),
          Sequel.qualify(:collection_management, :processing_priority_id) => Sequel.qualify(:enumvals_processing_priority, :id),
        },
        {
         :table_alias => :enumvals_processing_priority
        }).
    left_outer_join(:user_defined, :accession_id => Sequel.qualify(:accession, :id)).
    select(
      Sequel.qualify(:accession, :id).as(:accession_id),
      Sequel.qualify(:accession, :accession_date).as(:accession_date),
      Sequel.qualify(:accession, :identifier),
      Sequel.qualify(:accession, :content_description),
      Sequel.qualify(:user_defined, :string_1).as(:staff_received),
      Sequel.as(Sequel.lit('GetAccessionFieldArchivists(accession.id)'), :field_archivists),
      Sequel.as(Sequel.lit('GetAccessionLocationUserDefined(accession.id)'), :location),
      Sequel.as(Sequel.lit('GetAccessionProcessingStatus(accession.id)'), :processing_status),
      Sequel.as(Sequel.lit('GetAccessionProcessingPriority(accession.id)'), :processing_priority),
      Sequel.as(Sequel.lit('GetAccessionClassificationsUserDefined(accession.id)'), :classifications),
      Sequel.as(Sequel.lit('GetAccessionExtentNumberType(accession.id)'), :extent_number_type),
      Sequel.as(Sequel.lit('GetAccessionSourceName(accession.id)'), :donor_name),
      Sequel.as(Sequel.lit('GetAccessionDonorNumbers(accession.id)'), :donor_number),
      ).
    group(Sequel.qualify(:accession, :id)).
    order(Sequel.asc(:accession_date))

    dataset = dataset.where(Sequel.qualify(:accession, :repo_id) => @repo_id)

    if processing_status
      if processing_status == "No Defined Value"
        dataset = dataset.where(Sequel.qualify(:enumvals_processing_status, :value) => nil)
      elsif processing_status == "Any Defined Value"
        dataset = dataset.exclude(Sequel.qualify(:enumvals_processing_status, :value) => nil)
      else
        dataset = dataset.where(Sequel.qualify(:enumvals_processing_status, :value) => @processing_status)
      end
    end

    if processing_priority
      if processing_priority == "No Defined Value"
        dataset = dataset.where(Sequel.qualify(:enumvals_processing_priority, :value) => nil)
      elsif processing_priority == "Any Defined Value"
        dataset = dataset.exclude(Sequel.qualify(:enumvals_processing_priority, :value) => nil)
      else
        dataset = dataset.where(Sequel.qualify(:enumvals_processing_priority, :value) => @processing_priority)
      end
    end

    if classification
      #where{(price - 100 > 200) | (price / 100 >= 200)}
      dataset = dataset.where(Sequel.lit('GetEnumValue(user_defined.enum_1_id)') => @classification).or(Sequel.lit('GetEnumValue(user_defined.enum_2_id)') => @classification).or(Sequel.lit('GetEnumValue(user_defined.enum_3_id)') => @classification)
    end

    if donor_uri
      dataset = dataset.where(Sequel.qualify(:source_linked_agents_rlshp, :"#{@donor_type}_id") => @donor_id)
    end

    if field_archivist
      dataset = dataset.where(Sequel.qualify(:name_person, :sort_name) => @field_archivist)
    end

    dataset
  end

end
