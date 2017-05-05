=begin

Using these as a guide: 
https://github.com/archivesspace/archivesspace/blob/master/backend/app/model/reports/unprocessed_accessions_report.rb
This one has params: https://github.com/archivesspace/archivesspace/blob/master/backend/app/model/reports/created_accessions_report.rb
This one has params that are defined in some other, non-intuitive places: https://github.com/archivesspace/archivesspace/blob/master/backend/app/model/reports/location_holdings_report.rb
LocationList: https://github.com/archivesspace/archivesspace/blob/master/frontend/app/views/jobs/report_partials/_locationlist.html.erb
Relies on this JS: https://github.com/archivesspace/archivesspace/blob/f286c031af272722c3209008dd1fe818dd67f997/frontend/app/assets/javascripts/jobs.crud.js#L342

This is where some other param info is: https://github.com/archivesspace/archivesspace/blob/master/frontend/app/views/jobs/_form.html.erb#L93-L115
=end

class BhlAccessionsReport < AbstractReport
  
  register_report({
                    :uri_suffix => "bhl_accessions_report",
                    :description => "Bentley Historical Library Accessions Report",
                    :params => [["from", Date, "The start of report range"],
                                ["to", Date, "The start of report range"],
                                ["Additional Parameters", "accessionsparams", "Additional Accession parameters"]]
                  })

  
  attr_reader :processing_status, :processing_priority, :classification, :donor

  def initialize(params, job)
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
    
  end

  def title
    "Bentley Historical Library Accessions Report"
  end

  def headers
    ['accession_id', 'identifier', 'content_description', 'processing_status', 'processing_priority', 'classification_name', 'extent_number']
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
    dataset = db[:accession].
    left_outer_join(:extent, :accession_id => Sequel.qualify(:accession, :id)).
    left_outer_join(:collection_management, :accession_id => Sequel.qualify(:accession, :id)).
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
    left_outer_join(:classification_rlshp, :accession_id => Sequel.qualify(:accession, :id)).
    left_outer_join(:classification, :id => Sequel.qualify(:classification_rlshp, :classification_id)).
    select(
      Sequel.qualify(:accession, :id).as(:accession_id),
      Sequel.qualify(:accession, :identifier),
      Sequel.qualify(:accession, :content_description),
      Sequel.qualify(:enumvals_processing_status, :value).as(:processing_status),
      Sequel.qualify(:enumvals_processing_priority, :value).as(:processing_priority),
      Sequel.qualify(:classification, :identifier).as(:classification_name),
      Sequel.qualify(:extent, :number).as(:extent_number)
      )

    dataset = dataset.where(Sequel.qualify(:accession, :repo_id) => @repo_id)

    if processing_status
      dataset = dataset.where(Sequel.qualify(:enumvals_processing_status, :value) => @processing_status)
    end

    if processing_priority
      dataset = dataset.where(Sequel.qualify(:enumvals_processing_priority, :value) => @processing_priority)
    end

    if classification
      dataset = dataset.where(Sequel.qualify(:classification, :identifier) => @classification)
    end

    dataset
  end

end
