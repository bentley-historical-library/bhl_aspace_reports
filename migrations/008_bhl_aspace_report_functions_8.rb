require 'db/migrations/utils'

Sequel.migration do
  up do
    if $db_type == :mysql

# last name, first name, middle name, suffix, title, organization or unit
run "DROP FUNCTION IF EXISTS GetAccessionClassificationsByID;"
run <<EOF
CREATE FUNCTION GetAccessionClassificationsByID(f_accession_id INT)
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

	end
  end
end