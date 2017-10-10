require 'db/migrations/utils'

Sequel.migration do
  up do
    if $db_type == :mysql



run "DROP FUNCTION IF EXISTS GetResourceExtentNumberType;"
run <<EOF
CREATE FUNCTION GetResourceExtentNumberType(f_resource_id INT) 
    RETURNS VARCHAR(1020)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(1020);

    SELECT 
    	GROUP_CONCAT(CONCAT(number, " ", GetEnumValue(extent_type_id)) SEPARATOR "; ") into f_value
    FROM
    	extent
	WHERE
		extent.`resource_id` = f_resource_id;
	RETURN f_value;   
END 
EOF

run "DROP FUNCTION IF EXISTS GetAccessrestrictContent;"
run <<EOF
CREATE FUNCTION GetAccessrestrictContent(f_note_id INT) 
    RETURNS VARCHAR(8000)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(8000);

    SELECT
    	JSON_EXTRACT(CONVERT(notes using utf8), '$.subnotes[0].content') into f_value
    FROM
    	note
	WHERE
		note.`id` = f_note_id;

	RETURN f_value;   
END 
EOF

run "DROP FUNCTION IF EXISTS CountAccessrestrictsForResource;"
run <<EOF
CREATE FUNCTION CountAccessrestrictsForResource(f_resource_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;

    SELECT
    	COUNT(*) into f_total
    FROM
    	note
	JOIN
		archival_object ON archival_object.id = note.archival_object_id
	JOIN
		resource ON resource.id = archival_object.root_record_id
	WHERE
		note.`archival_object_id` IS NOT NULL AND resource.`id` = f_resource_id AND note.`notes` LIKE "%accessrestrict%";

	RETURN f_total;
END 
EOF

run "DROP FUNCTION IF EXISTS CountAccessrestrictByType;"
run <<EOF
CREATE FUNCTION CountAccessrestrictByType(f_resource_id INT, f_accessrestrict_type CHAR(2)) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;

    SELECT
    	COUNT(*) into f_total
    FROM
    	note
	JOIN
		archival_object ON archival_object.id = note.archival_object_id
	JOIN
		resource ON resource.id = archival_object.root_record_id
	WHERE
		note.`archival_object_id` IS NOT NULL AND resource.`id` = f_resource_id AND note.`notes` LIKE "%accessrestrict%" AND LOWER(JSON_EXTRACT(CONVERT(notes using utf8), '$.subnotes[0].content')) LIKE CONCAT('"', f_accessrestrict_type, ' %');

	RETURN f_total;
END 
EOF

run "DROP FUNCTION IF EXISTS CountRestrictedContainersForResource;"
run <<EOF
CREATE FUNCTION CountRestrictedContainersForResource(f_resource_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;

    SELECT
        COUNT(DISTINCT top_container.`id`) into f_total
    FROM
        note
    JOIN
        archival_object ON archival_object.id = note.archival_object_id
    JOIN
        resource ON resource.id = archival_object.root_record_id
    JOIN
        instance ON instance.archival_object_id = archival_object.id
    JOIN
        sub_container ON sub_container.instance_id = instance.id
    JOIN
        top_container_link_rlshp ON top_container_link_rlshp.sub_container_id = sub_container.id
    JOIN
        top_container ON top_container.id = top_container_link_rlshp.top_container_id
    WHERE
        resource.`id` = f_resource_id AND note.`archival_object_id` IS NOT NULL AND note.`notes` LIKE "%accessrestrict%";

    RETURN f_total;
END 
EOF

run "DROP FUNCTION IF EXISTS CountContainersForResource;"
run <<EOF
CREATE FUNCTION CountContainersForResource(f_resource_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;

    SELECT
        COUNT(DISTINCT top_container.`id`) into f_total
    FROM
        archival_object
    JOIN
        resource ON resource.id = archival_object.root_record_id
    JOIN
        instance ON instance.archival_object_id = archival_object.id
    JOIN
        sub_container ON sub_container.instance_id = instance.id
    JOIN
        top_container_link_rlshp ON top_container_link_rlshp.sub_container_id = sub_container.id
    JOIN
        top_container ON top_container.id = top_container_link_rlshp.top_container_id
    WHERE
        resource.`id` = f_resource_id;

    RETURN f_total;
END 
EOF

run "DROP FUNCTION IF EXISTS CountDigitalRestrictionsForResource;"
run <<EOF
CREATE FUNCTION CountDigitalRestrictionsForResource(f_resource_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;

    SELECT
        COUNT(DISTINCT instance.`id`) into f_total
    FROM
        note
    JOIN
        archival_object ON archival_object.id = note.archival_object_id
    JOIN
        resource ON resource.id = archival_object.root_record_id
    JOIN
        instance ON instance.archival_object_id = archival_object.id
    WHERE
        resource.`id` = f_resource_id AND note.`archival_object_id` IS NOT NULL 
    AND 
        note.`notes` LIKE "%accessrestrict%" AND BINARY GetEnumValue(instance.`instance_type_id`) = BINARY "digital_object";

    RETURN f_total;
END 
EOF

run "DROP FUNCTION IF EXISTS CountReadingRoomRestrictions;"
run <<EOF
CREATE FUNCTION CountReadingRoomRestrictions(f_resource_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;

    SELECT
        COUNT(*) into f_total
    FROM
        note
    JOIN
        archival_object ON archival_object.id = note.archival_object_id
    JOIN
        resource ON resource.id = archival_object.root_record_id
    WHERE
        note.`archival_object_id` IS NOT NULL AND resource.`id` = f_resource_id 
    AND 
        note.`notes` LIKE "%accessrestrict%" 
    AND 
        LOWER(JSON_EXTRACT(CONVERT(notes using utf8), '$.subnotes[0].content')) LIKE "%reading room%";

    RETURN f_total;
END 
EOF
	end
  end
end