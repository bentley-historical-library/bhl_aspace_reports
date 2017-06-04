class BhlDARTReport < AbstractReport
  
  register_report({
                    :uri_suffix => "bhl_dart_report",
                    :description => "Bentley Historical Library DART Report",
                    :params => [["from", Date, "The start of report range"],
                                ["to", Date, "The start of report range"]]
                  })


  def initialize(params, job)
    super
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
  end


  def title
    "Bentley Historical Library DART Report"
  end

  def headers
    ['DART_LID', 'Lastname', 'RestOfName', 'Suffix', 'Title', 'OrganizationOrUnit', 'Street1', 'Street2', 'City', 'St', 'Zip', 'donation amt', 'credit date', 'item description', 'designation', 'comment', 'concatenated', 'note', 'note type', 'GIK subtype', '# of units', 'item name', 'REV type', 'do not receipt', 'Constituent alt lookup ID', 'Bentley Accession ID']
  end

  def processor
    {
      'donation amt' => proc {|record| '1'},
      'credit date' => proc {|record| record[:accession_date]},
      'item description' => proc {|record| record[:thank_you_text]},
      'designation' => proc {|record| '897100'},
      'GIK subtype' => proc {|record| 'art & books'},
      '# of units' => proc {|record| '1'},
      'Bentley Accession ID' => proc {|record| ASUtils.json_parse(record[:identifier] || "[]").compact.join("-")},
      'Constituent alt lookup ID' => proc {|record| record[:donor_number]}
    }
  end

  def scope_by_repo_id(dataset)
    # repo scope is applied in the query below
    dataset
  end

  def query(db)
    source_enum_id = db[:enumeration].filter(:name=>'linked_agent_role').join(:enumeration_value, :enumeration_id => :id).where(:value => 'source').all[0][:id]
    
    dataset = db[:accession].where(:accession_date => (@from..@to)).
    left_outer_join(:user_defined, :accession_id => Sequel.qualify(:accession, :id)).
    left_outer_join(:linked_agents_rlshp, [[:accession_id, Sequel.qualify(:accession, :id)], [:role_id, source_enum_id]]).
    left_outer_join(:classification_rlshp, :accession_id => Sequel.qualify(:accession, :id)).
    left_outer_join(:classification, :id => Sequel.qualify(:classification_rlshp, :classification_id)).
    select(
      Sequel.qualify(:accession, :id).as(:accession_id),
      Sequel.qualify(:accession, :identifier),
      Sequel.qualify(:accession, :accession_date).as(:accession_date),
      Sequel.qualify(:user_defined, :text_1).as(:thank_you_text),
      Sequel.as(Sequel.lit('GetAgentLastName(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :Lastname),
      Sequel.as(Sequel.lit('GetAgentRestOfName(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :RestOfName),
      Sequel.as(Sequel.lit('GetAgentSuffix(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :Suffix),
      Sequel.as(Sequel.lit('GetAgentTitle(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :Title),
      Sequel.as(Sequel.lit('GetAgentOrganizationOrUnit(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :OrganizationOrUnit),
      Sequel.as(Sequel.lit('GetAgentContactName(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :donor_name),
      Sequel.as(Sequel.lit('GetAgentAddress(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :Street1),
      Sequel.as(Sequel.lit('GetAgentAddress2(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :Street2),
      Sequel.as(Sequel.lit('GetAgentCity(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :City),
      Sequel.as(Sequel.lit('GetAgentState(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :St),
      Sequel.as(Sequel.lit('GetAgentZipCode(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :Zip),
      Sequel.as(Sequel.lit('GetAccessionDonorNumber(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :donor_number),
      Sequel.as(Sequel.lit('GetAccessionDonorDARTLID(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :DART_LID)
      ).
    where(Sequel.qualify(:accession, :repo_id) => @repo_id).
    exclude(Sequel.qualify(:classification, :identifier) => ["UA", "RCS"])

    #agreement_signed_id = db[:enumeration].filter(:name => 'event_event_type').join(:enumeration_value, :enumeration_id => :id).where(:value=>'agreement_signed').all[0][:id]
    #pass_id = db[:enumeration].filter(:name => 'event_outcome').join(:enumeration_value, :enumeration_id => :id).where(:value => 'pass').all[0][:id]
    
    # dataset = db[:accession].
    # left_outer_join(:user_defined, :accession_id => Sequel.qualify(:accession, :id)).
    # left_outer_join(:linked_agents_rlshp, :accession_id => Sequel.qualify(:accession, :id)).
    # left_outer_join(:event_link_rlshp, :accession_id => Sequel.qualify(:accession, :id)).
    # left_outer_join(:event,:id => Sequel.qualify(:event_link_rlshp, :event_id)).
    # select(
    # 	Sequel.qualify(:accession, :id).as(:accession_id),
    # 	Sequel.qualify(:accession, :accession_date).as(:accession_date),
    # 	Sequel.qualify(:accession, :identifier),
    # 	Sequel.qualify(:user_defined, :text_1).as(:thank_you_text),
    # 	Sequel.as(Sequel.lit('GetAgentContactName(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :donor_name),
    #   Sequel.as(Sequel.lit('GetAgentAddress(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :address),
    #   Sequel.as(Sequel.lit('GetAgentCity(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :city),
    #   Sequel.as(Sequel.lit('GetAgentState(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :state),
    #   Sequel.as(Sequel.lit('GetAgentCountry(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :country),
    #   Sequel.as(Sequel.lit('GetAgentZipCode(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :zip_code),
    # 	Sequel.as(Sequel.lit('GetAccessionDonorNumber(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :donor_number),
    # 	Sequel.as(Sequel.lit('GetAccessionDonorDARTLID(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :dart_lid)
    # 	).
    # where(Sequel.qualify(:event, :timestamp) => (@from..@to)).
    # where(Sequel.qualify(:linked_agents_rlshp, :role_id) => source_enum_id).
    # where(Sequel.qualify(:event, :event_type_id) => agreement_signed_id).
    # where(Sequel.qualify(:event, :outcome_id) => pass_id).
    # where(Sequel.qualify(:accession, :repo_id) => @repo_id)

    dataset
  end


end