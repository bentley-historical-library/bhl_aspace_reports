class BHLAspaceReportsHelper

  def self.field_archivists()
    field_archivist_role_id = Enumeration.filter(:name => 'linked_agent_event_roles').join(:enumeration_value, :enumeration_id => Sequel.qualify(:enumeration, :id)).where(:value => 'field_archivist').all[0][:id]

    field_archivists = NamePerson.left_outer_join(:linked_agents_rlshp, :agent_person_id => :agent_person_id).
                      where(:role_id => field_archivist_role_id).
                      select(
                        Sequel.qualify(:name_person, :sort_name)
                      ).
                      distinct(:sort_name).map{|v| [v[:sort_name], v[:sort_name]]}

    field_archivists
  end


  def self.fix_identifier_format_bhl(row, field_name = :identifier)
    if row[field_name]
      identifiers = row[field_name].split(',,,')
    else
      identifiers = []
    end
  
    result = []
  
    identifiers.each do |identifier|
      result.push(ASUtils.json_parse(identifier).compact.join('-'))
    end
  
    row[field_name] = result.join(', ')
  end

  def self.format_date_condition(from, to, date_column)
    "(#{date_column} >= #{from} and #{date_column} <= #{to})"
  end

  def self.format_date(date)
    date.split(' ')[0].gsub('-', '')
  end

  def self.parse_date_params(params)
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

    from = DateTime.parse(from).to_time.strftime("%Y-%m-%d %H:%M:%S")
    to = DateTime.parse(to).to_time.strftime("%Y-%m-%d %H:%M:%S")
    [from, to]
  end

  def self.format_classification_condition(classification)
      "(GetEnumValue(user_defined.enum_1_id)=#{classification} or GetEnumValue(user_defined.enum_2_id)=#{classification} or GetEnumValue(user_defined.enum_3_id)=#{classification})"
  end

  def self.format_enum_condition(enum, value)
    if value == "'No Defined Value'"
      "#{enum}.value is null"
    elsif value == "'Any Defined Value'"
      "#{enum}.value is not null"
    else
      "#{enum}.value=#{value}"
    end
  end

  def self.parse_and_format_notes(row, field_name)
    if row[field_name]
      parsed_note = JSON.parse(row[field_name])
      note_type = parsed_note["type"]
      note_label = I18n.t("enumerations._note_types.#{note_type}", :default => note_type)
      note_content = self.parse_note_content(parsed_note)
      row[field_name] = "#{note_label}: #{note_content}"
    else
      row[field_name] = ""
    end
  end

  def self.parse_notes(row, field_name)
    if row[field_name]
      parsed_note = JSON.parse(row[field_name])
      note_content = self.parse_note_content(parsed_note)
      row[field_name] = note_content
    else
      row[field_name] = ""
    end
  end

  def self.parse_note_content(note)
    if note["jsonmodel_type"] == "note_singlepart"
      note["content"][0]
    elsif note["jsonmodel_type"] == "note_multipart"
      note["subnotes"][0]["content"]
    end
  end
end
