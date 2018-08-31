class BhlRestrictionsReport < AbstractReport
  
  register_report
  
  def initialize(params, job, db)
    super
  end

  def calculate_percent_restricted(row)
    if row[:restricted_containers] > 0 && row[:total_containers] > 0
      ((row[:restricted_containers].to_f / row[:total_containers].to_f) * 100.0).to_i
    else
      ""
    end
  end

  def fix_row(row)
    row[:Other] = row[:total_restrictions] - row[:CR] - row[:ER] - row[:PR] - row[:SR] - row[:reading_room]
    row[:percent_restricted] = calculate_percent_restricted(row)
    row.delete(:id)
  end

  def query_string
    "select
      resource.id,
      resource.title as collection_title,
      CountAccessrestrictsForResource(resource.id) as total_restrictions,
      CountAccessrestrictByType(resource.id, 'cr') as CR,
      CountAccessrestrictByType(resource.id, 'er') as ER,
      CountAccessrestrictByType(resource.id, 'pr') as PR,
      CountAccessrestrictByType(resource.id, 'sr') as SR,
      CountReadingRoomRestrictions(resource.id) as reading_room,
      '' as Other,
      CountDigitalRestrictionsForResource(resource.id) as restricted_digital,
      CountRestrictedContainersForResource(resource.id) as restricted_containers,
      CountContainersForResource(resource.id) as total_containers
    from 
      resource
    where 
      resource.repo_id=#{db.literal(@repo_id)}
      and resource.publish=1
    group by resource.id
    having
      CountAccessrestrictsForResource(resource.id) > 0"
  end

  def after_tasks
    info.delete(:repository)
  end
  
end