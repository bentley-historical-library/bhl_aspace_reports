require 'db/migrations/utils'

Sequel.migration do
  up do
    if $db_type == :mysql

# last name, first name, middle name, suffix, title, organization or unit
run "DROP FUNCTION IF EXISTS GetAccessionClassificationUserDefined;"
run <<EOF
CREATE FUNCTION GetAccessionClassificationUserDefined(enum_1_id INT, enum_2_id INT, enum_3_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF enum_3_id IS NOT NULL THEN
		SELECT CONCAT(GetEnumValue(enum_1_id), "; ", GetEnumValue(enum_2_id), "; ", GetEnumValue(enum_3_id)) INTO f_value;
	ELSEIF enum_2_id IS NOT NULL THEN
		SELECT CONCAT(GetEnumValue(enum_1_id), "; ", GetEnumValue(enum_2_id)) INTO f_value;
	ELSEIF enum_1_id IS NOT NULL THEN
		SELECT GetEnumValue(enum_1_id) INTO f_value;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

	end
  end
end