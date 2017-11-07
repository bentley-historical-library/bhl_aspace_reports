ReportErbRenderer.class_eval do
  def template_path(template_name)
  	if File.exist?(File.join(ASUtils.find_base_directory, 'plugins', 'bhl_aspace_reports', 'backend', 'views', 'reports', template_name))
      return File.join(ASUtils.find_base_directory, 'plugins', 'bhl_aspace_reports', 'backend', 'views', 'reports', template_name)
    end

    StaticAssetFinder.new('reports').find(template_name)
  end
end