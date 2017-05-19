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
    ['accession_id', 'identifier', 'accession_date', 'thank_you_text', 'content_description', 'donor_name', 'donor_number', 'dart_lid']
  end

  def processor
    {
      'identifier' => proc {|record| ASUtils.json_parse(record[:identifier] || "[]").compact.join("-")}
    }
  end

  def scope_by_repo_id(dataset)
    # repo scope is applied in the query below
    dataset
  end

  def query(db)
  	source_enum_id = db[:enumeration].filter(:name=>'linked_agent_role').join(:enumeration_value, :enumeration_id => :id).where(:value => 'source').all[0][:id]
    agreement_signed_id = db[:enumeration].filter(:name => 'event_event_type').join(:enumeration_value, :enumeration_id => :id).where(:value=>'agreement_signed').all[0][:id]
    pass_id = db[:enumeration].filter(:name => 'event_outcome').join(:enumeration_value, :enumeration_id => :id).where(:value => 'pass').all[0][:id]
    
    dataset = db[:accession].
    left_outer_join(:user_defined, :accession_id => Sequel.qualify(:accession, :id)).
    left_outer_join(:linked_agents_rlshp, :accession_id => Sequel.qualify(:accession, :id)).
    left_outer_join(:event_link_rlshp, :accession_id => Sequel.qualify(:accession, :id)).
    left_outer_join(:event,:id => Sequel.qualify(:event_link_rlshp, :event_id)).
    select(
    	Sequel.qualify(:accession, :id).as(:accession_id),
    	Sequel.qualify(:accession, :accession_date).as(:accession_date),
    	Sequel.qualify(:accession, :identifier),
    	Sequel.qualify(:accession, :content_description),
    	Sequel.qualify(:user_defined, :text_1).as(:thank_you_text),
    	Sequel.as(Sequel.lit('GetAccessionSourceName(accession.id)'), :donor_name),
    	Sequel.as(Sequel.lit('GetAccessionDonorNumber(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :donor_number),
    	Sequel.as(Sequel.lit('GetAccessionDonorDARTLID(linked_agents_rlshp.agent_person_id, linked_agents_rlshp.agent_family_id, linked_agents_rlshp.agent_corporate_entity_id)'), :dart_lid)
    	).
    where(Sequel.qualify(:event, :timestamp) => (@from..@to)).
    where(Sequel.qualify(:linked_agents_rlshp, :role_id) => source_enum_id).
    where(Sequel.qualify(:event, :event_type_id) => agreement_signed_id).
    where(Sequel.qualify(:event, :outcome_id) => pass_id).
    where(Sequel.qualify(:accession, :repo_id) => @repo_id)

    dataset
  end


end