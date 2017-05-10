require 'db/migrations/utils'

Sequel.migration do
  up do
    if $db_type == :mysql

# Function to return the sortname given a Person, Family, or Corporate
# when those ids found in the linked_agents_rlshp are passed in as parameters
run "DROP  FUNCTION IF EXISTS GetAgentSortName;"
run <<EOF
CREATE FUNCTION GetAgentSortName(f_person_id INT, f_family_id INT, f_corporate_id INT) 
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

run "DROP FUNCTION IF EXISTS GetAccessionSourceName;"
run <<EOF
CREATE FUNCTION GetAccessionSourceName(f_accession_id INT)
	RETURNS VARCHAR(1024)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(1024);

		SELECT 
			GROUP_CONCAT(GetAgentSortname(T1.`agent_person_id`, T1.`agent_family_id`, T1.`agent_corporate_entity_id`) SEPARATOR '; ') INTO f_value
		FROM 
			`linked_agents_rlshp` T1
		WHERE
			T1.`accession_id` = f_accession_id
		AND
			BINARY GetEnumValue(T1.`role_id`) = BINARY 'source';
	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS GetAccessionDonorNumber;"
run <<EOF
CREATE FUNCTION GetAccessionDonorNumber(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT donor_number INTO f_value FROM donor_detail WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT donor_number INTO f_value FROM donor_detail WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT donor_number INTO f_value FROM donor_detail WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS GetAccessionDonorDARTLID;"
run <<EOF
CREATE FUNCTION GetAccessionDonorDARTLID(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT dart_id INTO f_value FROM donor_detail WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT dart_id INTO f_value FROM donor_detail WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT dart_id INTO f_value FROM donor_detail WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

# Function to return enum value given an id
run "DROP  FUNCTION IF EXISTS GetEnumValue;"
run <<EOF
CREATE FUNCTION GetEnumValue(f_enum_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);   
    SELECT enumeration_value.`value`INTO f_value
    FROM enumeration_value
    WHERE enumeration_value.`id` = f_enum_id;
    RETURN f_value;
END 
EOF

	end
  end
end