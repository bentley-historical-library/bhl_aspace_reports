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

    # add 1 day to the to parameter to make the Sequel query inclusive of the date specified in the to parameter
    to_parsed = DateTime.parse(to) + 1
    @to = to_parsed.to_time.strftime("%Y-%m-%d %H:%M:%S")
  end


  def title
    "Bentley Historical Library DART Report"
  end

  def headers
    ['DART_LID', 'Lastname', 'Firstname', 'Middle', 'Suffix', 'Title', 'OrganizationOrUnit', 'Street1', 'Street2', 'City', 'St', 'Zip', 'donation amt', 'credit date', 'item description', 'designation', 'comment', 'concatenated', 'note', 'note type', 'GIK subtype', '# of units', 'item name', 'REV type', 'do not receipt', 'Constituent alt lookup ID', 'Bentley Accession ID']
  end

  def processor
    {
      'donation amt' => proc {|record| '1'},
      'credit date' => proc {|record| record[:accession_date]},
      'designation' => proc {|record| '897100'},
      'GIK subtype' => proc {|record| 'art & books'},
      '# of units' => proc {|record| '1'},
      'Bentley Accession ID' => proc {|record| ASUtils.json_parse(record[:identifier] || "[]").compact.join("-")},
      'Constituent alt lookup ID' => proc {|record| record[:beal_contact_id]}
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
    select(
      Sequel.qualify(:accession, :id).as(:accession_id),
      Sequel.qualify(:accession, :identifier),
      Sequel.qualify(:accession, :accession_date).as(:accession_date),
      Sequel.as(Sequel.lit('GetAgentLastName(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :Lastname),
      Sequel.as(Sequel.lit('GetAgentRestOfName(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :Firstname),
      Sequel.as(Sequel.lit('GetAgentSuffix(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :Suffix),
      Sequel.as(Sequel.lit('GetAgentTitle(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :Title),
      Sequel.as(Sequel.lit('GetAgentOrganizationOrUnit(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :OrganizationOrUnit),
      Sequel.as(Sequel.lit('GetAgentAddress1(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :Street1),
      Sequel.as(Sequel.lit('GetAgentAddress2(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :Street2),
      Sequel.as(Sequel.lit('GetAgentCity(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :City),
      Sequel.as(Sequel.lit('GetAgentState(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :St),
      Sequel.as(Sequel.lit('GetAgentZipCode(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :Zip),
      Sequel.as(Sequel.lit('GetAgentBEALContactID(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :beal_contact_id),
      Sequel.as(Sequel.lit('GetAgentDARTLID(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :DART_LID)
      ).
    where(Sequel.qualify(:accession, :repo_id) => @repo_id).
    group(Sequel.qualify(:accession, :id))


    dataset = dataset.where(Sequel.lit('GetEnumValue(user_defined.enum_1_id)') => ["MHC", "FAC"]).or(Sequel.lit('GetEnumValue(user_defined.enum_2_id)') => ["MHC", "FAC"]).or(Sequel.lit('GetEnumValue(user_defined.enum_3_id)') => ["MHC", "FAC"])

    dataset
  end


end