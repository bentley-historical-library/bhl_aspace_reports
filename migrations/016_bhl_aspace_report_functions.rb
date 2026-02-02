require 'db/migrations/utils'

#Converted all SQL functions to start with BHL_ to prevent conflicts with ArchivesSpace

Sequel.migration do
  up do
    if $db_type == :mysql


run "DROP FUNCTION IF EXISTS BHL_GetAgentPhone;"
run <<EOF
CREATE FUNCTION BHL_GetAgentPhone(f_person_id INT, f_family_id INT, f_corporate_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
        SELECT telephone.`number` INTO f_value FROM telephone WHERE telephone.agent_contact_id = (SELECT id FROM agent_contact WHERE agent_person_id = f_person_id LIMIT 1) LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
        SELECT telephone.`number` INTO f_value FROM telephone WHERE telephone.agent_contact_id = (SELECT id FROM agent_contact WHERE agent_family_id = f_family_id LIMIT 1) LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
        SELECT telephone.`number` INTO f_value FROM telephone WHERE telephone.agent_contact_id = (SELECT id FROM agent_contact WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1) LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

    end
  end
end
