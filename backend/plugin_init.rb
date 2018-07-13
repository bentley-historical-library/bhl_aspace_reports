ReportResponse.class_eval do
  def generate
    custom_csv_handler_reports = ["Bentley Historical Library Revisions Report", "Bentley Historical Library Accessions Extent Report", "Bentley Historical Library Accessions Report", "Bentley Historical Library Formats Report", "Bentley Historical Library DART Report", "Bentley Historical Library Restrictions Report", "Bentley Historical Library Non-DART Accessions Report"]
    return @report.to_csv if (custom_csv_handler_reports.include?(@report.title) && @report.format == "csv")

    @params[:html_report] ||= proc {
      ReportErbRenderer.new(@report, @params).render("report.erb")
    }

    format = @report.format

    klass = Object.const_get("#{format.upcase}Response")
    klass.send(:new, @report, @params).generate
    end
end