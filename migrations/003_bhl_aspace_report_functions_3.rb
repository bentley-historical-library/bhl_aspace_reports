require 'db/migrations/utils'

Sequel.migration do
  up do
    if $db_type == :mysql

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

	end
  end
end