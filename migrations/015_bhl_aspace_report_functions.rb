require 'db/migrations/utils'

#Converted all SQL functions to start with BHL_ to prevent conflicts with ArchivesSpace

Sequel.migration do
  up do
    if $db_type == :mysql


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

    end
  end
end