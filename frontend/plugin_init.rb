Rails.application.config.after_initialize do 

  JobsController.class_eval do 
    def new
      @job = JSONModel(:job).new._always_valid!
      @import_types = import_types
      @report_data = JSONModel::HTTP::get_json("/reports")
      #@report_data["reports"] = @report_data["reports"].select{|r| r.start_with?("bhl_")}
      # handle any options passed through via parameters
      @job_type = params['job_type']
    end
  end
    
end
