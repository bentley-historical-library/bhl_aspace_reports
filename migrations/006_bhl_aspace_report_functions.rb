require 'db/migrations/utils'

Sequel.migration do
  up do
    if $db_type == :mysql

run "DROP FUNCTION IF EXISTS GetAccessionFieldArchivists;"
run <<EOF
CREATE FUNCTION GetAccessionFieldArchivists(f_accession_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	SELECT 
		GROUP_CONCAT(name_person.sort_name SEPARATOR "; ") into f_value
	FROM
		name_person
	JOIN
		linked_agents_rlshp ON name_person.`agent_person_id`=linked_agents_rlshp.`agent_person_id`
	JOIN
		event ON event.`id`=linked_agents_rlshp.`event_id`
	JOIN
		event_link_rlshp ON event_link_rlshp.`event_id`=event.`id`
	WHERE
		event_link_rlshp.`accession_id`=f_accession_id 
	AND
		BINARY GetEnumValue(linked_agents_rlshp.`role_id`) = BINARY 'field_archivist'
	AND
		BINARY GetEnumValue(event.`event_type_id`) = BINARY 'custody_transfer';

	RETURN f_value;
END
EOF

	end
  end
end