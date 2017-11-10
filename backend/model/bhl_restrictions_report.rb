class BhlRestrictionsReport < AbstractReport
  
  register_report

  def initialize(params, job, db)
    super

  end

  def headers
    ['collection_title', 'total_restrictions', 'CR', 'ER', 'PR', 'SR', 'reading_room', 'Other', 'restricted_digital', 'restricted_containers', 'total_containers', 'percent_restricted']
  end

  def processor
    {
      'Other' => proc {|record| (record[:total_restrictions] - record[:CR] - record[:ER] - record[:PR] - record[:SR] - record[:reading_room])},
      'percent_restricted' => proc {|record|  if record[:restricted_containers] > 0 && record[:total_containers] > 0
                                                ((record[:restricted_containers].to_f / record[:total_containers].to_f) * 100.0).to_i
                                              else
                                                ""
                                              end
                                            }
    }
  end

  def scope_by_repo_id(dataset)
    # repo scope is applied in the query below
    dataset
  end

  def query
    resource_ids = db[:note].
    where(Sequel.like(:notes, "%accessrestrict%")).
    exclude(Sequel.qualify(:note, :archival_object_id) => nil).
    left_outer_join(:archival_object, Sequel.qualify(:archival_object, :id) => Sequel.qualify(:note, :archival_object_id)).
    left_outer_join(:resource, Sequel.qualify(:resource, :id) => Sequel.qualify(:archival_object, :root_record_id)).
    select(
      Sequel.qualify(:resource, :id)
      ).
    where(Sequel.qualify(:resource, :publish) => 1).
    group(Sequel.qualify(:resource, :id)).map(:id)

    dataset = db[:resource].
    filter(:id => resource_ids).
    select(
      Sequel.qualify(:resource, :title).as(:collection_title),
      Sequel.as(Sequel.lit('CountAccessrestrictsForResource(resource.id)'), :total_restrictions),
      Sequel.as(Sequel.lit('CountRestrictedContainersForResource(resource.id)'), :restricted_containers),
      Sequel.as(Sequel.lit('CountContainersForResource(resource.id)'), :total_containers),
      Sequel.as(Sequel.lit('CountAccessrestrictByType(resource.id, "pr")'), :PR),
      Sequel.as(Sequel.lit('CountAccessrestrictByType(resource.id, "sr")'), :SR),
      Sequel.as(Sequel.lit('CountAccessrestrictByType(resource.id, "er")'), :ER),
      Sequel.as(Sequel.lit('CountAccessrestrictByType(resource.id, "cr")'), :CR),
      Sequel.as(Sequel.lit('CountReadingRoomRestrictions(resource.id)'), :reading_room),
      Sequel.as(Sequel.lit('GetResourceExtentNumberType(resource.id)'), :collection_extents),
      Sequel.as(Sequel.lit('CountDigitalRestrictionsForResource(resource.id)'), :restricted_digital)
      ).group(Sequel.qualify(:resource, :id))

    dataset
  end
end