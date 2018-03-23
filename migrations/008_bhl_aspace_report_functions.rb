require 'db/migrations/utils'

Sequel.migration do
  up do
    if $db_type == :mysql

run "DROP FUNCTION IF EXISTS GetArchivalObjectDigitalObject;"
run <<EOF
CREATE FUNCTION GetArchivalObjectDigitalObject(f_archival_object_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_value INT;

    SELECT
        digital_object.`id` INTO f_value
    FROM
        digital_object
    JOIN
        instance_do_link_rlshp ON instance_do_link_rlshp.digital_object_id = digital_object.id
    JOIN
        instance ON instance.id = instance_do_link_rlshp.instance_id
    WHERE
        instance.archival_object_id = f_archival_object_id;

    RETURN f_value;
END 
EOF

    end
  end
end