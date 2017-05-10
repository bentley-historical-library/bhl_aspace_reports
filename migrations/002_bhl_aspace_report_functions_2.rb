require 'db/migrations/utils'

Sequel.migration do
  up do
    if $db_type == :mysql

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

run "DROP FUNCTION IF EXISTS GetAccessionDARTLIDs;"
run <<EOF
CREATE FUNCTION GetAccessionDARTLIDs(f_accession_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	SELECT
		GROUP_CONCAT(dart_id SEPARATOR "; ") into f_value
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

run "DROP FUNCTION IF EXISTS GetAccessionClassifications;"
run <<EOF
CREATE FUNCTION GetAccessionClassifications(f_accession_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	SELECT
		GROUP_CONCAT(identifier SEPARATOR "; ") into f_value
	FROM
		classification
	JOIN
		classification_rlshp ON classification_rlshp.`accession_id` = f_accession_id
	WHERE
		classification.`id` = classification_rlshp.`classification_id`;

	RETURN f_value;
END
EOF


	end
  end
end