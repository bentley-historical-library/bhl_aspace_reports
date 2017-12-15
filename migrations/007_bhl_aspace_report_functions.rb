require 'db/migrations/utils'

Sequel.migration do
  up do
    if $db_type == :mysql

run "DROP FUNCTION IF EXISTS GetArchivalObjectNoteByType;"
run <<EOF
CREATE FUNCTION GetArchivalObjectNoteByType(f_archival_object_id INT, f_note_type VARCHAR(20)) 
    RETURNS VARCHAR(8000)
    READS SQL DATA
BEGIN
    DECLARE f_value MEDIUMBLOB;

    SELECT
        notes into f_value
    FROM
    	note
	WHERE
		note.`archival_object_id` = f_archival_object_id AND note.`notes` LIKE CONCAT('%', f_note_type, '%') LIMIT 1;

	RETURN f_value;
END 
EOF

run "DROP  FUNCTION IF EXISTS GetArchivalObjectExtent;"
run <<EOF
CREATE FUNCTION GetArchivalObjectExtent(f_archival_object_id INT) 
    RETURNS VARCHAR(1024)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(1024);

    SELECT 
        GROUP_CONCAT(CONCAT(number, " ", GetEnumValue(extent_type_id), if(GetExtentPhysicalDetails(id) IS NOT NULL, CONCAT(" ", GetExtentPhysicalDetails(id)), '')) SEPARATOR "; ") into f_value
    FROM
        extent
    WHERE
        extent.`archival_object_id` = f_archival_object_id;
    RETURN f_value; 
END 
EOF

run "DROP  FUNCTION IF EXISTS GetExtentPhysicalDetails;"
run <<EOF
CREATE FUNCTION GetExtentPhysicalDetails(f_extent_id INT) 
    RETURNS VARCHAR(1024)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(1024);

    SELECT
        CONCAT("(", COALESCE(physical_details, ''), if(physical_details IS NULL OR dimensions IS NULL, '', ', '), COALESCE(dimensions, ''), ")") into f_value 
    FROM
        extent
    WHERE
        extent.`id` = f_extent_id AND (extent.`physical_details` IS NOT NULL OR extent.`dimensions` IS NOT NULL);

    RETURN f_value;
END 
EOF

run "DROP FUNCTION IF EXISTS GetArchivalObjectContainers;"
run <<EOF
CREATE FUNCTION GetArchivalObjectContainers(f_archival_object_id INT) 
    RETURNS VARCHAR(1024)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(1024);

    SELECT
        GROUP_CONCAT(CONCAT(GetEnumValue(top_container.`type_id`), " ", top_container.`indicator`)) INTO f_value
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

run "DROP FUNCTION IF EXISTS GetArchivalObjectBreadcrumb;"
run <<EOF
CREATE FUNCTION GetArchivalObjectBreadcrumb(f_archival_object_id INT) 
    RETURNS VARCHAR(8000)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(8000);

    DECLARE f_parent_id INT;
    SELECT parent_id INTO f_parent_id FROM archival_object WHERE id = f_archival_object_id;

    WHILE f_parent_id IS NOT NULL DO
        SELECT if(f_value IS NOT NULL, CONCAT(display_string, " > ", f_value), display_string) INTO f_value FROM archival_object WHERE id = f_parent_id;
        SELECT parent_id INTO f_parent_id FROM archival_object WHERE id = f_parent_id;
    END WHILE;


    RETURN f_value;
END 
EOF

run "DROP FUNCTION IF EXISTS GetResourceCallNumber;"
run <<EOF
CREATE FUNCTION GetResourceCallNumber(f_resource_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);


    SELECT
        identifier INTO f_value
    FROM
        resource
    WHERE
        resource.id = f_resource_id;

    RETURN f_value;
END 
EOF


    end
  end
end