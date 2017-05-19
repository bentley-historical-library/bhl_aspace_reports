require 'db/migrations/utils'

Sequel.migration do
  up do
    if $db_type == :mysql

run "DROP FUNCTION IF EXISTS GetAccessionLocationUserDefined;"
run <<EOF
CREATE FUNCTION GetAccessionLocationUserDefined(f_accession_id INT)
	RETURNS VARCHAR(1024)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(1024);
    
    SELECT
        text_2 into f_value
    FROM
        user_defined
    WHERE
        user_defined.`accession_id` = f_accession_id;
    
    RETURN f_value;
END
EOF


    end
  end
end