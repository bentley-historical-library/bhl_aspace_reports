require 'db/migrations/utils'

Sequel.migration do
  up do
    if $db_type == :mysql

run "DROP FUNCTION IF EXISTS GetAccessionClassificationsUserDefined;"
run <<EOF
CREATE FUNCTION GetAccessionClassificationsUserDefined(f_accession_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	SELECT 
		CONCAT_WS("; ", GetEnumValue(enum_1_id), GetEnumValue(enum_2_id), GetEnumValue(enum_3_id)) INTO f_value
	FROM
		user_defined
	WHERE
		accession_id = f_accession_id;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS GetAccessionDonorNumbers;"
run <<EOF
CREATE FUNCTION GetAccessionDonorNumbers(f_accession_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	SELECT
		GROUP_CONCAT(donor_number SEPARATOR "; ") into f_value
	FROM
		donor_detail
	JOIN
		linked_agents_rlshp ON linked_agents_rlshp.`accession_id` = f_accession_id
	WHERE
		(donor_detail.`agent_person_id` = linked_agents_rlshp.`agent_person_id` OR donor_detail.`agent_corporate_entity_id` = linked_agents_rlshp.`agent_corporate_entity_id` OR donor_detail.`agent_family_id` = linked_agents_rlshp.`agent_family_id`)
	AND 
		BINARY GetEnumValue(linked_agents_rlshp.`role_id`) = BINARY 'source';	

	RETURN f_value;
END
EOF

run "DROP  FUNCTION IF EXISTS GetAccessionExtentNumberType;"
run <<EOF
CREATE FUNCTION GetAccessionExtentNumberType(f_accession_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);

    SELECT 
    	GROUP_CONCAT(CONCAT(number, " ", GetEnumValue(extent_type_id)) SEPARATOR "; ") into f_value
    FROM
    	extent
	WHERE
		extent.`accession_id` = f_accession_id;
	RETURN f_value;   
END 
EOF

run "DROP FUNCTION IF EXISTS GetAccessionLocationUserDefined;"
run <<EOF
CREATE FUNCTION GetAccessionLocationUserDefined(f_accession_id INT)
	RETURNS VARCHAR(1024)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(1024);
    
    SELECT
        text_1 into f_value
    FROM
        user_defined
    WHERE
        user_defined.`accession_id` = f_accession_id;
    
    RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS GetAccessionProcessingPriority;"
run <<EOF
CREATE FUNCTION GetAccessionProcessingPriority(f_accession_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	SELECT
		value into f_value
	FROM
		enumeration_value
	JOIN
		collection_management ON collection_management.`accession_id` = f_accession_id
	WHERE
		enumeration_value.`id` = collection_management.`processing_priority_id`;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS GetAccessionProcessingStatus;"
run <<EOF
CREATE FUNCTION GetAccessionProcessingStatus(f_accession_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	SELECT
		value into f_value
	FROM
		enumeration_value
	JOIN
		collection_management ON collection_management.`accession_id` = f_accession_id
	WHERE
		enumeration_value.`id` = collection_management.`processing_status_id`;

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

run "DROP FUNCTION IF EXISTS GetAgentAddress1;"
run <<EOF
CREATE FUNCTION GetAgentAddress1(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT address_1 INTO f_value FROM agent_contact WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT address_1 INTO f_value FROM agent_contact WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT address_1 INTO f_value FROM agent_contact WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS GetAgentAddress2;"
run <<EOF
CREATE FUNCTION GetAgentAddress2(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT address_2 INTO f_value FROM agent_contact WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT address_2 INTO f_value FROM agent_contact WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT address_2 INTO f_value FROM agent_contact WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS GetAgentCity;"
run <<EOF
CREATE FUNCTION GetAgentCity(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT city INTO f_value FROM agent_contact WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT city INTO f_value FROM agent_contact WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT city INTO f_value FROM agent_contact WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS GetAgentCountry;"
run <<EOF
CREATE FUNCTION GetAgentCountry(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT country INTO f_value FROM agent_contact WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT country INTO f_value FROM agent_contact WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT country INTO f_value FROM agent_contact WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS GetAgentDARTLID;"
run <<EOF
CREATE FUNCTION GetAgentDARTLID(f_person_id INT, f_family_id INT, f_corporate_id INT)
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

run "DROP FUNCTION IF EXISTS GetAgentDonorNumber;"
run <<EOF
CREATE FUNCTION GetAgentDonorNumber(f_person_id INT, f_family_id INT, f_corporate_id INT)
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

run "DROP FUNCTION IF EXISTS GetAgentLastName;"
run <<EOF
CREATE FUNCTION GetAgentLastName(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT primary_name INTO f_value FROM name_person WHERE agent_person_id = f_person_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS GetAgentOrganizationOrUnit;"
run <<EOF
CREATE FUNCTION GetAgentOrganizationOrUnit(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_corporate_id IS NOT NULL THEN
		SELECT sort_name INTO f_value FROM name_corporate_entity WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT sort_name INTO f_value FROM name_family WHERE agent_family_id = f_family_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS GetAgentRestOfName;"
run <<EOF
CREATE FUNCTION GetAgentRestOfName(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT rest_of_name INTO f_value FROM name_person WHERE agent_person_id = f_person_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS GetAgentState;"
run <<EOF
CREATE FUNCTION GetAgentState(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT region INTO f_value FROM agent_contact WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT region INTO f_value FROM agent_contact WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT region INTO f_value FROM agent_contact WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS GetAgentSuffix;"
run <<EOF
CREATE FUNCTION GetAgentSuffix(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT suffix INTO f_value FROM name_person WHERE agent_person_id = f_person_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS GetAgentTitle;"
run <<EOF
CREATE FUNCTION GetAgentTitle(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT title INTO f_value FROM name_person WHERE agent_person_id = f_person_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS GetAgentZipCode;"
run <<EOF
CREATE FUNCTION GetAgentZipCode(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT post_code INTO f_value FROM agent_contact WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT post_code INTO f_value FROM agent_contact WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT post_code INTO f_value FROM agent_contact WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

	end
  end
end