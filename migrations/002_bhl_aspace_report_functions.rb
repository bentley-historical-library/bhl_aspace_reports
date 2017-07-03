require 'db/migrations/utils'

Sequel.migration do
  up do
    if $db_type == :mysql

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
