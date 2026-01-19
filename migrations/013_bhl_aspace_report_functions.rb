require 'db/migrations/utils'


Sequel.migration do
  up do
    if $db_type == :mysql


run "DROP FUNCTION IF EXISTS BHL_GetArchivalObjectBreadcrumb;"
run <<EOF
CREATE FUNCTION BHL_GetArchivalObjectBreadcrumb(f_archival_object_id INT) 
    RETURNS VARCHAR(8000) CHARSET utf8mb3
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(8000) CHARSET utf8mb3;

    DECLARE f_parent_id INT;
    SELECT parent_id INTO f_parent_id FROM archival_object WHERE id = f_archival_object_id;

    WHILE f_parent_id IS NOT NULL DO
        SELECT if(f_value IS NOT NULL, CONCAT(display_string, " > ", f_value), display_string) INTO f_value FROM archival_object WHERE id = f_parent_id;
        SELECT parent_id INTO f_parent_id FROM archival_object WHERE id = f_parent_id;
    END WHILE;


    RETURN f_value;
END 
EOF
















# Function to return the sortname given a Person, Family, or Corporate
# when those ids found in the linked_agents_rlshp are passed in as parameters
run "DROP  FUNCTION IF EXISTS BHL_GetAgentSortName;"
run "DROP  FUNCTION IF EXISTS GetAgentSortName;"
run <<EOF
CREATE FUNCTION BHL_GetAgentSortName(f_person_id INT, f_family_id INT, f_corporate_id INT) 
    RETURNS VARCHAR(255) CHARSET utf8mb3;
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255) CHARSET utf8mb3   
    
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


run "DROP FUNCTION IF EXISTS BHL_GetAccessionSourceName;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionSourceName(f_accession_id INT)
	RETURNS VARCHAR(1024) CHARSET utf8mb3;
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(1024) CHARSET utf8mb3;

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


# Function to any agents sort_name linked to the resource has
# Creators
run "DROP  FUNCTION IF EXISTS BHL_GetResourceCreator;"
run <<EOF
CREATE FUNCTION BHL_GetResourceCreator(f_record_id INT) 
    RETURNS VARCHAR(1024) CHARSET utf8mb3;
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(1024) CHARSET utf8mb3;
        
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

run "DROP FUNCTION IF EXISTS BHL_GetArchivalObjectContainers;"
run <<EOF
CREATE FUNCTION BHL_GetArchivalObjectContainers(f_archival_object_id INT) 
    RETURNS VARCHAR(1024) CHARSET utf8mb3
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(1024) CHARSET utf8mb3;

    SELECT
        GROUP_CONCAT(CONCAT(BHL_GetEnumValue(top_container.`type_id`), " ", top_container.`indicator`)) INTO f_value
    FROM
        top_container
    JOIN
        top_container_link_rlshp ON top_container_link_rlshp.top_container_id = top_container.id
    JOIN
        sub_container ON sub_container.id = top_container_link_rlshp.sub_container_id
    JOIN
        instance ON instance.id = sub_container.instance_id
    WHERE
        instance.archival_object_id = f_archival_object_id;

    RETURN f_value;
END 
EOF

#========= leave below
    end
  end
end