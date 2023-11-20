require 'db/migrations/utils'

#Converted all SQL functions to start with BHL_ to prevent conflicts with ArchivesSpace

Sequel.migration do
  up do
    if $db_type == :mysql

run "DROP FUNCTION IF EXISTS BHL_GetAccessionSourceName;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionSourceName(f_accession_id INT)
	RETURNS VARCHAR(1024)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(1024);

		SELECT 
			GROUP_CONCAT(BHL_GetAgentSortName(T1.`agent_person_id`, T1.`agent_family_id`, T1.`agent_corporate_entity_id`) SEPARATOR '; ') INTO f_value
		FROM 
			`linked_agents_rlshp` T1
		WHERE
			T1.`accession_id` = f_accession_id
		AND
			BINARY BHL_GetEnumValue(T1.`role_id`) = BINARY 'source';
	RETURN f_value;
END
EOF

# Function to return the sortname given a Person, Family, or Corporate
# when those ids found in the linked_agents_rlshp are passed in as parameters
run "DROP  FUNCTION IF EXISTS BHL_GetAgentSortName;"
run "DROP  FUNCTION IF EXISTS GetAgentSortName;"
run <<EOF
CREATE FUNCTION BHL_GetAgentSortName(f_person_id INT, f_family_id INT, f_corporate_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);   
    
    IF f_person_id IS NOT NULL THEN
            SELECT sort_name INTO f_value FROM name_person WHERE agent_person_id = f_person_id LIMIT 1;
        ELSEIF f_family_id IS NOT NULL THEN
            SELECT sort_name INTO f_value FROM name_family WHERE agent_family_id = f_family_id LIMIT 1;
        ELSEIF f_corporate_id IS NOT NULL THEN
            SELECT sort_name INTO f_value FROM name_corporate_entity WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
        ELSE 
            SET f_value = 'Unknown';
        END IF;

    RETURN f_value;
END 
EOF

# Function to any agents sort_name linked to the resource has
# Creators
run "DROP  FUNCTION IF EXISTS BHL_GetResourceCreator;"
run <<EOF
CREATE FUNCTION BHL_GetResourceCreator(f_record_id INT) 
    RETURNS VARCHAR(1024)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(1024);  
        
        SELECT
            GROUP_CONCAT(BHL_GetAgentSortName(T1.`agent_person_id`, T1.`agent_family_id`, T1.`agent_corporate_entity_id`) SEPARATOR '; ') INTO f_value
        FROM
            `linked_agents_rlshp` T1
        WHERE
            BHL_GetResourceId(T1.`resource_id`, T1.`archival_object_id`) = f_record_id
        AND
            BINARY BHL_GetEnumValue(T1.`role_id`) = BINARY 'creator';

    RETURN f_value;
END 
EOF

    end
  end
end