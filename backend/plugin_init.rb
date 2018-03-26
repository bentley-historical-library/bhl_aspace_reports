ReportResponse.class_eval do
  def generate
    return @report.to_csv if (@report.title == "Bentley Historical Library Formats Report" && @report.format == "csv")

    @params[:html_report] ||= proc {
      ReportErbRenderer.new(@report, @params).render("report.erb")
    }

    format = @report.format

    klass = Object.const_get("#{format.upcase}Response")
    klass.send(:new, @report, @params).generate
    end
end