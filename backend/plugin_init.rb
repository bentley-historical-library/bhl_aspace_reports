# We depend on this enum value being here.  Make sure it is.
RequestContext.open(:create_enums => true) do
  BackendEnumSource.valid?('linked_agent_event_roles', 'field_archivist')
end
