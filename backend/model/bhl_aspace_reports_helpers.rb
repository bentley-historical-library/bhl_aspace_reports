class BHLAspaceReportsHelper

    def self.field_archivists()
        field_archivist_role_id = Enumeration.filter(:name => 'linked_agent_event_roles').join(:enumeration_value, :enumeration_id => Sequel.qualify(:enumeration, :id)).where(:value => 'field_archivist').all[0][:id]

        field_archivists = NamePerson.left_outer_join(:linked_agents_rlshp, :agent_person_id => :agent_person_id).
                    where(:role_id => field_archivist_role_id).
                    select(
                        Sequel.qualify(:name_person, :sort_name)
                    ).distinct(:sort_name).map{|v| [v[:sort_name], v[:sort_name]]}

        field_archivists
    end
end